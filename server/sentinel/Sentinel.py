import logging
import os
import socket
import time
import threading
import docker
import asyncio
from common.Serializer import Serializer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)

# Get environment variables
WORKER_HOSTS_ENV = os.getenv("WORKER_HOSTS", "localhost")
WORKER_PORTS_ENV = os.getenv("WORKER_PORTS", "9001")
WORKER_HOSTS = WORKER_HOSTS_ENV.split(",") if "," in WORKER_HOSTS_ENV else [WORKER_HOSTS_ENV]
WORKER_PORTS = [int(port) for port in WORKER_PORTS_ENV.split(",")] if "," in WORKER_PORTS_ENV else [int(WORKER_PORTS_ENV)]
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", 5))
HOSTNAME = os.getenv("HOSTNAME", "unknown_host")
SERVICE_NAME = os.getenv("SERVICE_NAME", "sentinel")
PEER_PORT = int(os.getenv("PEER_PORT", 9010))
RESTART_ATTEMPTS = int(os.getenv("RESTART_ATTEMPTS", 3))
RESTART_COOLDOWN = int(os.getenv("RESTART_COOLDOWN", 30))
COMPOSE_PROJECT_NAME = os.getenv("COMPOSE_PROJECT_NAME")

# Election and Leader Constants
ELECTION_TIMEOUT_DURATION = 10
LEADER_HEARTBEAT_INTERVAL = CHECK_INTERVAL * 2
LEADER_DEAD_DURATION = 10
SLAVE_HEARTBEAT_TIMEOUT = 5  # Timeout in seconds for slave heartbeats

class Sentinel:
    def __init__(self, worker_hosts=WORKER_HOSTS, worker_ports=WORKER_PORTS, check_interval=CHECK_INTERVAL):
        self.worker_hosts = worker_hosts
        self.worker_ports = worker_ports
        
        # Ensure worker_ports is the same length as worker_hosts
        if len(self.worker_ports) < len(self.worker_hosts):
            self.worker_ports.extend([self.worker_ports[0]] * (len(self.worker_hosts) - len(self.worker_ports)))
        elif len(self.worker_ports) > len(self.worker_hosts):
            self.worker_ports = self.worker_ports[:len(self.worker_hosts)]
            
        self.check_interval = check_interval
        self.running = True
        self.sockets = {}  # Dictionary to store sockets for each worker
        self.hostname = HOSTNAME
        self.service_name = SERVICE_NAME
        self.peer_port = PEER_PORT
        self.id = self._calculate_hostname_sum() 

        self.peer_listener_thread = None
        self.discovered_peers_addresses = set() 

        self.is_leader = False
        self.current_leader_id = None
        self.election_in_progress = False
        self.i_am_election_coordinator = False 
        self.election_votes = {} 
        self.election_start_time = 0
        self.last_leader_heartbeat_time = 0
        self.election_message_received_this_cycle = False
        
        self.previous_leader_hostname = None
        self.previous_leader_id = None
        
        self.active_slaves = {}
        self.event_loop = None

        # Worker revival related variables
        self.compose_project_name = COMPOSE_PROJECT_NAME
        self.worker_unhealthy_counts = {worker: 0 for worker in self.worker_hosts}  # Track each worker separately
        self.last_restart_times = {worker: 0 for worker in self.worker_hosts}  # Track last restart time for each worker
        self.restart_attempts = {worker: 0 for worker in self.worker_hosts}  # Track restart attempts for each worker
        self.docker_client = None
        try:
            self.docker_client = docker.from_env()
        except Exception as e:
            logging.error(f"Failed to initialize Docker client: {e}")

    def _calculate_hostname_sum(self):
        return sum(ord(char) for char in self.hostname)

    def _send_message_to_peer(self, peer_host, peer_port, message_type, payload):
        """Sends a structured message to a specific peer."""
        full_message = {"type": message_type, "sender_id": self.id, **payload}
        serialized_message = Serializer.serialize(full_message)
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(2)
                s.connect((peer_host, peer_port))
                s.sendall(serialized_message)
                return True
        except socket.timeout:
            logging.warning(f"Timeout sending {message_type} to peer {peer_host}:{peer_port}")
        except ConnectionRefusedError:
            logging.warning(f"Connection refused sending {message_type} to peer {peer_host}:{peer_port} (possibly not ready)")
        except Exception as e:
            logging.error(f"Error sending {message_type} to peer {peer_host}:{peer_port}: {e}")
        return False

    def _discover_peers(self):
        """Discovers peer addresses using DNS. Does not send messages."""
        if not self.service_name:
            logging.warning("SERVICE_NAME not set, cannot discover peers.")
            return set()
        try:
            addr_info = socket.getaddrinfo(self.service_name, self.peer_port, socket.AF_INET, socket.SOCK_STREAM)
            peers = set()
            for res in addr_info:
                peer_ip = res[4][0]
                # Add all resolved IPs, including potentially self.
                # Self-messaging is handled by not sending to self.id or by listener ignoring self-originated messages.
                peers.add((peer_ip, self.peer_port))
            
            if not peers:
                logging.warning(f"No peers discovered for service {self.service_name}. This instance might be the only one.")
            self.discovered_peers_addresses = peers
            return peers
        except socket.gaierror:
            logging.warning(f"Could not resolve service name {self.service_name}. Peer discovery failed.")
        except Exception as e:
            logging.error(f"Error in peer discovery: {e}")
        self.discovered_peers_addresses = set()
        return set()

    def _broadcast_message(self, message_type, payload):
        """Broadcasts a message to all discovered peers, excluding self if message is from self."""
        peers_to_send = self._discover_peers()
        if not peers_to_send:
            logging.warning(f"No peers to broadcast {message_type} to.")
            return

        sent_count = 0
        for peer_host, peer_port_num in peers_to_send:
            # Basic self-connection avoidance: check if peer_host is one of local host's IPs
            # This is not perfectly reliable in all Docker network modes.
            # A better check is if the listener receives a message from its own ID.
            # For now, we attempt to send to all distinct discovered IPs.
            # The listener should handle messages from self if they get through.
            
            if self._send_message_to_peer(peer_host, peer_port_num, message_type, payload):
                sent_count +=1

    def _start_peer_listener(self):
        self.peer_listener_thread = threading.Thread(target=self._peer_listener_loop, daemon=True)
        self.peer_listener_thread.start()

    def _peer_listener_loop(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                s.bind(('0.0.0.0', self.peer_port))
                s.settimeout(1.0) # Timeout for accept to allow checking self.running
                s.listen()
                while self.running:
                    try:
                        conn, addr = s.accept()
                        with conn:
                            data = conn.recv(1024)
                            if data:
                                try:
                                    message = Serializer.deserialize(data)
                                    msg_type = message.get("type")
                                    sender_id = message.get("sender_id")

                                    if sender_id == self.id: 
                                        continue

                                    self.election_message_received_this_cycle = True 

                                    if msg_type == "ELECTION_START":
                                        if sender_id < self.id: 
                                            self._send_message_to_peer(addr[0], self.peer_port, "ELECTION_RESPONSE", {"id": self.id, "coordinator_id": sender_id, "hostname": self.hostname})
                                            
                                            # Initiate my own election.
                                            # This node is higher, so it should take over or ensure a proper election happens.
                                            self._initiate_election()
                                        
                                        elif sender_id > self.id:
                                            if self.i_am_election_coordinator:
                                                self.i_am_election_coordinator = False
                                                self.election_in_progress = False 
                                                self.election_votes = {} 
                                            
                                            self.election_in_progress = True
                                            self.i_am_election_coordinator = False # 
                                            self.current_leader_id = None
                                            self.is_leader = False
                                            self._send_message_to_peer(addr[0], self.peer_port, "ELECTION_RESPONSE", {"id": self.id, "coordinator_id": sender_id, "hostname": self.hostname})

                                    elif msg_type == "ELECTION_RESPONSE":
                                        voter_id = message.get("id") 
                                        response_for_coordinator_id = message.get("coordinator_id")
                                        voter_hostname = message.get("hostname", "unknown_hostname")

                                        if self.i_am_election_coordinator and self.election_in_progress and response_for_coordinator_id == self.id:
                                            if voter_id > self.id:
                                                self.i_am_election_coordinator = False
                                                self.election_in_progress = False 
                                                self.election_votes = {}
                                            else:
                                                self.election_votes[voter_id] = voter_id
                                                # Store slave information for future heartbeats
                                                current_time = time.time()
                                                self.active_slaves[voter_hostname] = {
                                                    "id": voter_id,
                                                    "ip": addr[0],
                                                    "last_heartbeat": current_time,
                                                    "last_sent_heartbeat": current_time,
                                                    "response_time": 0,
                                                    "consecutive_success": 0
                                                }

                                    elif msg_type == "LEADER_ANNOUNCEMENT":
                                        new_leader_id = message.get("leader_id")
                                        sender_of_announcement = sender_id
                                        leader_hostname = message.get("leader_hostname", "unknown")  # Get leader hostname
                                        logging.info(f"\033[38;5;208mReceived LEADER_ANNOUNCEMENT from {sender_of_announcement}: New leader is {new_leader_id} (hostname: {leader_hostname}). My ID is {self.id}.\033[0m")

                                        if new_leader_id < self.id and (self.current_leader_id is None or new_leader_id != self.current_leader_id):
                                            logging.warning(f"Contesting LEADER_ANNOUNCEMENT for {new_leader_id} (lower than my ID {self.id}). Initiating new election.")
                                            self.current_leader_id = None
                                            self.is_leader = False
                                            self.election_in_progress = False
                                            self.i_am_election_coordinator = False
                                            self._initiate_election()
                                        else:
                                            # If there was a previous leader, store it before switching to new leader
                                            if self.current_leader_id is not None and self.current_leader_id != new_leader_id:
                                                self.previous_leader_id = self.current_leader_id
                                                self.previous_leader_hostname = self.current_leader_hostname if hasattr(self, 'current_leader_hostname') else None
                                                logging.info(f"Stored previous leader: ID {self.previous_leader_id}, hostname {self.previous_leader_hostname}")
                                            
                                            self.current_leader_id = new_leader_id
                                            self.current_leader_hostname = leader_hostname  # Store the leader's hostname
                                            self.is_leader = (self.id == new_leader_id)
                                            self.election_in_progress = False
                                            self.i_am_election_coordinator = False
                                            self.election_votes = {} 
                                            self.last_leader_heartbeat_time = time.time() 
                                            if self.is_leader:
                                                logging.info(f"\033[38;5;208mI AM THE NEW LEADER (ID: {self.id}) based on announcement from {sender_of_announcement}.\033[0m")
                                                # Check if there's a previous leader to revive
                                                self._check_and_revive_previous_leader()
                                            else:
                                                # When becoming a slave, reset any leader-related state
                                                self.active_slaves = {} 
                                                logging.info(f"\033[36mI am a SLAVE. Leader is {self.current_leader_id}.\033[0m")

                                    elif msg_type == "LEADER_HEARTBEAT":
                                        leader_id_from_heartbeat = message.get("leader_id")
                                        timestamp = message.get("timestamp", 0)
                                        
                                        # Send an acknowledgement back to the leader
                                        self._send_message_to_peer(addr[0], self.peer_port, "HEARTBEAT_ACK", {
                                            "original_timestamp": timestamp,
                                            "hostname": self.hostname
                                        })
                                        
                                        if self.current_leader_id is None or leader_id_from_heartbeat == self.current_leader_id:
                                            if self.current_leader_id is None:
                                                logging.info(f"Accepting leader {leader_id_from_heartbeat} from first heartbeat.")
                                                self.current_leader_id = leader_id_from_heartbeat
                                                self.is_leader = (self.id == self.current_leader_id)
                                            self.last_leader_heartbeat_time = time.time()
                                        elif leader_id_from_heartbeat != self.current_leader_id:
                                            logging.warning(f"Conflicting LEADER_HEARTBEAT. Current leader {self.current_leader_id}, heartbeat from {sender_id} for {leader_id_from_heartbeat}. Election might be needed.")
                                            # Potentially trigger an election if conflict persists or if this sender has higher ID
                                            # For now, just log. A new election will eventually sort it out if the true leader stops heartbeating.
                                    
                                    elif msg_type == "HEARTBEAT_ACK":
                                        # Handle heartbeat acknowledgement from a slave if we're the leader
                                        if self.is_leader:
                                            original_timestamp = message.get("original_timestamp", 0)
                                            slave_hostname = message.get("hostname", "unknown")
                                            current_time = time.time()
                                            response_time = current_time - original_timestamp
                                            
                                            if slave_hostname in self.active_slaves:
                                                # Update the last successful heartbeat time for this slave
                                                self.active_slaves[slave_hostname]["last_heartbeat"] = current_time
                                                # Update IP if it changed (rare but possible with dynamic IPs)
                                                self.active_slaves[slave_hostname]["ip"] = addr[0]
                                                # Calculate and store response time
                                                self.active_slaves[slave_hostname]["response_time"] = response_time
                                                # Track consecutive successful heartbeats
                                                self.active_slaves[slave_hostname]["consecutive_success"] = self.active_slaves[slave_hostname].get("consecutive_success", 0) + 1
                                                
                                            else:
                                                # We don't know this slave yet - might be a new one that joined after election
                                                logging.info(f"Received heartbeat ACK from unknown slave {slave_hostname} (ID: {sender_id}). Adding to active slaves.")
                                                self.active_slaves[slave_hostname] = {
                                                    "id": sender_id,
                                                    "ip": addr[0],
                                                    "last_heartbeat": current_time,
                                                    "last_sent_heartbeat": current_time,
                                                    "response_time": response_time,
                                                    "consecutive_success": 1
                                                }

                                except Exception as e:
                                    logging.error(f"Failed to deserialize or process message from {addr}: {e}. Raw data: {data}")
                    except socket.timeout:
                        continue 
                    except Exception as e:
                        if self.running:
                            logging.error(f"Error in peer listener accept/receive: {e}")
                        time.sleep(0.1) 
        except Exception as e:
            logging.error(f"Peer listener loop failed critically: {e}")
        logging.info("Peer listener shutdown.")

    def _initiate_election(self):
        # This method asserts this node's intention to become coordinator.
        # Any previous election state (e.g., being a slave) is overridden.

        logging.info(f"\033[38;5;208mINITIATING ELECTION (My ID: {self.id}). Broadcasting ELECTION_START.\033[0m")
        self.election_in_progress = True
        self.i_am_election_coordinator = True 
        self.current_leader_id = None 
        self.is_leader = False
        self.election_start_time = time.time()
        self.election_votes = {self.id: self.id} 

        self._broadcast_message("ELECTION_START", {"coordinator_id": self.id})
        # After broadcasting, we wait for ELECTION_TIMEOUT_DURATION in the main loop to call _process_election_results.

    def _process_election_results(self):
        if not self.i_am_election_coordinator or not self.election_in_progress:
            return

        logging.info(f"\033[38;5;208mProcessing election results. My ID: {self.id}. Votes received: {self.election_votes}\033[0m")
        
        if not self.election_votes:
            logging.warning("No votes received in election. Re-evaluating or re-electing might be needed.")
            # This could happen if this is the only node or peers didn't respond.
            # For simplicity, declare self leader if no other votes.
            new_leader_id = self.id
        else:
            new_leader_id = max(self.election_votes.keys()) # Highest ID wins

        logging.info(f"\033[38;5;208mElection concluded. New leader determined to be ID: {new_leader_id}. Announcing...\033[0m")
        
        # Include hostname in leader announcement
        self._broadcast_message("LEADER_ANNOUNCEMENT", {"leader_id": new_leader_id, "leader_hostname": self.hostname})

        # If there was a previous leader and we're becoming the new leader, store it
        if self.current_leader_id is not None and self.current_leader_id != new_leader_id and new_leader_id == self.id:
            self.previous_leader_id = self.current_leader_id
            self.previous_leader_hostname = self.current_leader_hostname if hasattr(self, 'current_leader_hostname') else None
            logging.info(f"Stored previous leader: ID {self.previous_leader_id}, hostname {self.previous_leader_hostname}")
        
        self.current_leader_id = new_leader_id
        self.current_leader_hostname = self.hostname if new_leader_id == self.id else None
        self.is_leader = (self.id == new_leader_id)
        self.election_in_progress = False
        self.i_am_election_coordinator = False
        self.election_votes = {} # Clear votes after processing
        self.last_leader_heartbeat_time = time.time() # New leader is now active

        if self.is_leader:
            logging.info(f"\033[32mI AM THE NEW LEADER (ID: {self.id}) after coordinating election.\033[0m")
            # Check if there's a previous leader to revive
            self._check_and_revive_previous_leader()
        else:
            logging.info(f"\033[36mI am a SLAVE after coordinating. New leader is {self.current_leader_id}.\033[0m")

    def _check_and_revive_previous_leader(self):
        """Check if there's a previous leader to revive and attempt to restart it"""
        if not self.is_leader:
            logging.warning("Only the leader should attempt to revive previous leaders")
            return False
            
        if not self.previous_leader_hostname:
            return False
            
        logging.info(f"\033[33mNew leader detected previous leader: {self.previous_leader_hostname} (ID: {self.previous_leader_id}). Attempting to revive...\033[0m")
        
        restart_success = self.restart_sentinel(self.previous_leader_hostname)
        
        # Clear previous leader info regardless of restart success
        # We only want to attempt restart once
        self.previous_leader_hostname = None
        self.previous_leader_id = None
        
        return restart_success

    def restart_sentinel(self, sentinel_hostname):
        """Attempt to restart a Sentinel container by hostname"""
        if not self.docker_client:
            logging.error("Cannot restart sentinel: Docker client not initialized")
            return False
        
        if not self.compose_project_name:
            logging.error(f"COMPOSE_PROJECT_NAME not set. Cannot determine container to restart.")
            return False
        
        try:
            logging.info(f"\033[34mAttempting to find and restart sentinel container with hostname: {sentinel_hostname}\033[0m")
            
            containers = self.docker_client.containers.list(
                all=True, 
                filters={
                    "label": [
                        f"com.docker.compose.service={self.service_name}",
                        f"com.docker.compose.project={self.compose_project_name}"
                    ]
                }
            )
            
            matching_container = None
            for container in containers:
                container_info = container.attrs
                if container_info.get('Config', {}).get('Hostname') == sentinel_hostname:
                    matching_container = container
                    logging.info(f"Found sentinel container by hostname: {container.name}")
                    break
            
            # If no match by hostname, try the container ID (some hostnames might be container IDs)
            if not matching_container:
                logging.info(f"No container found with hostname {sentinel_hostname}, trying container ID")
                try:
                    # Try to get container directly by ID
                    matching_container = self.docker_client.containers.get(sentinel_hostname)
                    logging.info(f"Found container by ID: {matching_container.name}")
                except docker.errors.NotFound:
                    # If that fails, try to find any sentinel container that's stopped
                    logging.warning(f"No container found with ID {sentinel_hostname}, looking for stopped sentinel containers")
                    
                    # Get all sentinel containers that are not running (might be stopped or exited)
                    stopped_containers = self.docker_client.containers.list(
                        all=True, 
                        filters={
                            "status": ["exited", "dead"],
                            "label": [
                                f"com.docker.compose.service={self.service_name}",
                                f"com.docker.compose.project={self.compose_project_name}"
                            ]
                        }
                    )
                    
                    if stopped_containers:
                        # Use the first stopped container we find
                        matching_container = stopped_containers[0]
                        logging.info(f"Found stopped sentinel container: {matching_container.name}")
                    else:
                        logging.error(f"No stopped sentinel containers found")
                
            if not matching_container:
                logging.error(f"Could not find sentinel container with hostname '{sentinel_hostname}' in project '{self.compose_project_name}'")
                return False
            
            # Log detailed restart information
            container_status = matching_container.status
            logging.info(f"\033[31mRestarting sentinel container {matching_container.name} (current status: {container_status})\033[0m")
            
            # For stopped containers, we need to start not restart
            if container_status.lower() in ('exited', 'dead'):
                logging.info(f"Container is {container_status}, using start() instead of restart()")
                matching_container.start()
            else:
                # Restart with a timeout for graceful shutdown
                matching_container.restart(timeout=30)  # 30-second timeout for graceful shutdown
            
            logging.info(f"\033[32mSuccessfully initiated {container_status.lower() in ('exited', 'dead') and 'start' or 'restart'} of sentinel container: {matching_container.name}\033[0m")
            return True
                
        except docker.errors.NotFound:
            logging.error(f"Sentinel container with hostname '{sentinel_hostname}' not found")
            return False
        except docker.errors.APIError as e:
            logging.error(f"Docker API error when restarting container: {e}")
            return False
        except Exception as e:
            logging.error(f"Unexpected error restarting sentinel container: {e}", exc_info=True)
            return False

    def shutdown(self):
        logging.info(f"Shutdown called for Sentinel ID: {self.id}")
        self.running = False
        
        # Close all worker sockets
        for worker_host, socket_conn in self.sockets.items():
            if socket_conn:
                try:
                    socket_conn.close()
                    logging.info(f"Closed worker health check socket for {worker_host}.")
                except Exception as e:
                    logging.warning(f"Error closing worker health check socket for {worker_host}: {e}")
        
        self.sockets = {}
        
        # Any running asyncio tasks will be cancelled when their event loop is closed
        logging.info("Asyncio heartbeat tasks will terminate as their event loop is closed")

    def restart_worker(self, worker_host):
        """Attempt to restart the specific worker container with enhanced reliability"""
        if not self.docker_client:
            logging.error("Cannot restart worker: Docker client not initialized")
            return False
        
        current_time = time.time()
        if current_time - self.last_restart_times.get(worker_host, 0) < RESTART_COOLDOWN:
            logging.warning(f"Skipping restart attempt for {worker_host}: Cooldown period not elapsed ({RESTART_COOLDOWN} seconds)")
            return False
        
        if not self.compose_project_name:
            logging.error(f"COMPOSE_PROJECT_NAME not set. Cannot determine container to restart.")
            return False
        
        try:
            logging.info(f"\033[34mAttempting to find and restart worker container for service: {worker_host}\033[0m")
            
            # Find container using label-based filtering (more precise)
            containers = self.docker_client.containers.list(
                all=True, 
                filters={
                    "label": [
                        f"com.docker.compose.service={worker_host}",
                        f"com.docker.compose.project={self.compose_project_name}"
                    ]
                }
            )
            
            if not containers:
                logging.error(f"Could not find container for service '{worker_host}' in project '{self.compose_project_name}'")
                return False
            
            # Use the first matching container (should be only one based on precise labels)
            worker_container = containers[0]
            
            # Log detailed restart information
            self.restart_attempts[worker_host] += 1
            logging.info(f"\033[31mRestarting worker container {worker_container.name} (attempt #{self.restart_attempts[worker_host]})\033[0m")
            
            # Restart with a timeout for graceful shutdown
            worker_container.restart(timeout=30)  # 30-second timeout for graceful shutdown
            
            # Record restart time
            self.last_restart_times[worker_host] = current_time
            
            logging.info(f"\033[32mSuccessfully restarted worker container: {worker_container.name}\033[0m")
            return True
                
        except docker.errors.NotFound:
            logging.error(f"Worker container for service '{worker_host}' not found")
            return False
        except docker.errors.APIError as e:
            logging.error(f"Docker API error when restarting container: {e}")
            return False
        except Exception as e:
            logging.error(f"Unexpected error restarting worker container: {e}", exc_info=True)
            return False

    def run(self):
        worker_list = ", ".join([f"{host}:{port}" for host, port in zip(self.worker_hosts, self.worker_ports)])
        logging.info(f"Sentinel starting (ID: {self.id}) for workers: {worker_list}")
        self._start_peer_listener()

        # Initial peer discovery and ID announcement
        # Stagger initial broadcast to avoid network collision and allow listeners to start
        time.sleep(2 + (self.id % 3)) # Random stagger based on ID
        self._discover_peers() 
        self._broadcast_message("ID_ANNOUNCE", {"id": self.id})
        
        last_election_initiation_attempt = 0

        while self.running:
            try:
                current_time = time.time()
                self.election_message_received_this_cycle = False # Reset for this iteration

                if self.is_leader:
                    # Check health for all workers
                    for i, worker_host in enumerate(self.worker_hosts):
                        worker_port = self.worker_ports[i]
                        
                        if self._check_worker_health(worker_host, worker_port):
                            # logging.info(f"\033[32mWorker {worker_host}:{worker_port} is healthy\033[0m")
                            self.worker_unhealthy_counts[worker_host] = 0  # Reset counter if worker is healthy
                        else:
                            self.worker_unhealthy_counts[worker_host] += 1
                            logging.error(f"\033[31mWorker {worker_host}:{worker_port} is unhealthy \033[33m(count: {self.worker_unhealthy_counts[worker_host]}/{RESTART_ATTEMPTS})\033[0m")
                            
                            # If worker has been unhealthy for multiple consecutive checks, attempt to restart it
                            if self.worker_unhealthy_counts[worker_host] >= RESTART_ATTEMPTS:
                                logging.warning(f"\033[31mWorker {worker_host} has been unhealthy for {self.worker_unhealthy_counts[worker_host]} consecutive checks. Attempting restart...\033[0m")
                                restart_success = self.restart_worker(worker_host)
                                if restart_success:
                                    logging.info(f"Restart initiated successfully for \033[32m{worker_host}\033[0m. Resetting unhealthy counter and waiting for recovery.")
                                    self.worker_unhealthy_counts[worker_host] = 0  # Reset counter after successful restart
                                    self.restart_attempts[worker_host] = 0
                                else:
                                    logging.error(f"\033[31mFailed to restart worker {worker_host}. Will retry after cooldown period.\033[0m")
                    
                    if current_time - getattr(self, '_last_heartbeat_management_time', 0) > LEADER_HEARTBEAT_INTERVAL:
                        self._manage_slave_heartbeats()
                        self._last_heartbeat_management_time = current_time
                        
                        self._broadcast_message("LEADER_HEARTBEAT", {"leader_id": self.id})
                
                else:
                    if self.current_leader_id is not None: # We know a leader
                        if (current_time - self.last_leader_heartbeat_time) > LEADER_DEAD_DURATION:
                            logging.warning(f"\033[31mLeader {self.current_leader_id} not responding. Storing as previous leader and initiating new election.\033[0m")
                            
                            # Store the failing leader before considering it lost
                            self.previous_leader_id = self.current_leader_id
                            self.previous_leader_hostname = self.current_leader_hostname if hasattr(self, 'current_leader_hostname') else None
                            logging.info(f"Stored previous leader: ID {self.previous_leader_id}, hostname {self.previous_leader_hostname}")
                            
                            self.current_leader_id = None # Consider leader lost
                            self.is_leader = False # Ensure not leader
                            self._initiate_election()
                            last_election_initiation_attempt = current_time
                        # else: Slave: Monitoring leader
                    
                    else: # No leader known
                        if not self.election_in_progress and not self.election_message_received_this_cycle:
                            # Avoid starting election too frequently if one just failed or if messages are flowing
                            if current_time - last_election_initiation_attempt > ELECTION_TIMEOUT_DURATION * 1.5 : # Add some buffer
                                self._initiate_election()
                                last_election_initiation_attempt = current_time

                # Election coordinator tasks (if this instance initiated an election)
                if self.i_am_election_coordinator and self.election_in_progress:
                    if (current_time - self.election_start_time) > ELECTION_TIMEOUT_DURATION:
                        logging.info(f"\033[38;5;208mElection timeout reached for coordinator {self.id}. Processing results.\033[0m")
                        self._process_election_results()
                        # Resetting i_am_election_coordinator and election_in_progress is done in _process_election_results

                time.sleep(self.check_interval)

            except Exception as e:
                logging.error(f"Error in sentinel main loop: {e}", exc_info=True)
                time.sleep(self.check_interval) # Wait before retrying

        logging.info("Sentinel main loop ended. Initiating shutdown sequence...")
        # Listener thread is daemon, should exit when self.running is false and main thread exits.
        # However, explicit join is good practice.
        if self.peer_listener_thread and self.peer_listener_thread.is_alive():
            logging.info("Waiting for peer listener thread to shut down...")
            self.peer_listener_thread.join(timeout=3.0) # Increased timeout slightly
            if self.peer_listener_thread.is_alive():
                logging.warning("Peer listener thread did not shut down cleanly.")
        logging.info("Sentinel shutdown complete")

    # In the Sentinel class, modify the _check_worker_health method:
    def _check_worker_health(self, worker_host, worker_port):
        """Check if the specific worker is healthy by connecting to its echo server"""
        try:
            # Close previous connection if open for this worker
            if worker_host in self.sockets and self.sockets[worker_host]:
                try:
                    self.sockets[worker_host].close()
                except:
                    pass
                self.sockets[worker_host] = None
            
            # Try to resolve the IP address if it's a hostname
            try:
                # Get all addresses for the hostname
                addr_info = socket.getaddrinfo(worker_host, worker_port, socket.AF_INET, socket.SOCK_STREAM)
                if addr_info:
                    # Use the first resolved address
                    socket_family, socket_type, proto, _, addr = addr_info[0]
                    host_ip = addr[0]
                else:
                    host_ip = worker_host  # Fallback to the original hostname
            except socket.gaierror:
                logging.warning(f"Could not resolve hostname {worker_host}, trying direct connection")
                host_ip = worker_host  # Use the original hostname as fallback
                
            socket_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            socket_conn.settimeout(15) 
            socket_conn.connect((host_ip, worker_port))
            
            message = Serializer.serialize({"timestamp": int(time.time())})

            socket_conn.sendall(message)
            
            data = socket_conn.recv(1024)
            
            # Store the socket for later cleanup
            self.sockets[worker_host] = socket_conn
            
            # Simple validation - just check we got some data back
            if data:
                return True
            else:
                logging.warning(f"Empty health check response from {worker_host}:{worker_port}")
                return False
                
        except socket.timeout:
            logging.error(f"Health check for {worker_host}:{worker_port} timed out")
            return False
        except ConnectionRefusedError:
            logging.error(f"Connection refused for {worker_host}:{worker_port} - worker may be down")
            return False
        except Exception as e:
            logging.error(f"Error checking worker health for {worker_host}:{worker_port}: {e}")
            return False

    async def _async_send_message_to_peer(self, peer_host, peer_port, message_type, payload):
        """Asynchronous version of _send_message_to_peer"""
        full_message = {"type": message_type, "sender_id": self.id, **payload}
        serialized_message = Serializer.serialize(full_message)
        try:
            reader, writer = await asyncio.open_connection(peer_host, peer_port)
            writer.write(serialized_message)
            await writer.drain()
            writer.close()
            await writer.wait_closed()
            return True
        except ConnectionRefusedError:
            logging.warning(f"Connection refused sending {message_type} to peer {peer_host}:{peer_port} (possibly not ready)")
        except asyncio.TimeoutError:
            logging.warning(f"Timeout sending {message_type} to peer {peer_host}:{peer_port}")
        except Exception as e:
            logging.error(f"Error sending {message_type} to peer {peer_host}:{peer_port}: {e}")
        return False

    async def _send_heartbeat_to_slave(self, slave_hostname, slave_info):
        """
        Sends a heartbeat to a specific slave and monitors the response
        This function is intended to be run as an asyncio task
        """
        slave_ip = slave_info["ip"]
        slave_id = slave_info["id"]
        current_time = time.time()
        
        # First check if the slave has been unresponsive for too long
        if slave_hostname in self.active_slaves:
            last_heartbeat = self.active_slaves[slave_hostname].get("last_heartbeat", 0)
            time_since_last_heartbeat = current_time - last_heartbeat
            
            # If we haven't heard from this slave in a while, consider it potentially dead
            if time_since_last_heartbeat > SLAVE_HEARTBEAT_TIMEOUT * 3:
                logging.warning(f"\033[33mSlave {slave_hostname} (ID: {slave_id}) has been unresponsive for {time_since_last_heartbeat:.1f} seconds\033[0m")
                
                if time_since_last_heartbeat > SLAVE_HEARTBEAT_TIMEOUT * 5:
                    logging.error(f"\033[31mSlave {slave_hostname} (ID: {slave_id}) considered dead. Attempting restart...\033[0m")
                    
                    restart_success = self.restart_sentinel(slave_hostname)
                    
                    if restart_success:
                        if slave_hostname in self.active_slaves:
                            self.active_slaves.pop(slave_hostname, None)
                    else:
                        logging.error(f"\033[31mFailed to restart slave {slave_hostname}. Will try again later.\033[0m")
                    
                    return slave_hostname
        
        # Try to send a heartbeat
        heartbeat_sent_time = current_time
        sent = await self._async_send_message_to_peer(
            slave_ip, 
            self.peer_port, 
            "LEADER_HEARTBEAT", 
            {"leader_id": self.id, "timestamp": heartbeat_sent_time}
        )
        
        if sent:
            if slave_hostname in self.active_slaves:
                self.active_slaves[slave_hostname]["last_sent_heartbeat"] = heartbeat_sent_time
                logging.debug(f"Sent heartbeat to slave {slave_hostname}")
        else:
            logging.warning(f"\033[33mCouldn't send heartbeat to slave {slave_hostname} (ID: {slave_id})\033[0m")
            
            if slave_hostname in self.active_slaves:
                last_heartbeat = self.active_slaves[slave_hostname].get("last_heartbeat", 0)
                time_since_last_heartbeat = current_time - last_heartbeat
                
                if time_since_last_heartbeat > SLAVE_HEARTBEAT_TIMEOUT * 3:
                    logging.error(f"\033[31mSlave {slave_hostname} (ID: {slave_id}) connection failed and has been unresponsive for {time_since_last_heartbeat:.1f} seconds. Attempting restart...\033[0m")
                    
                    restart_success = self.restart_sentinel(slave_hostname)
                    
                    if restart_success:
                        logging.info(f"\033[32mSuccessfully initiated restart of slave {slave_hostname}\033[0m")
                        if slave_hostname in self.active_slaves:
                            self.active_slaves.pop(slave_hostname, None)
                    else:
                        logging.error(f"\033[31mFailed to restart slave {slave_hostname}. Will try again later.\033[0m")
        
        return slave_hostname

    async def _async_manage_slave_heartbeats(self):
        """Manages heartbeat tasks for all known slaves using asyncio"""
        if not self.is_leader:
            logging.warning("Only the leader should manage slave heartbeats")
            return
        
        # Create tasks for all slaves and wait for them to complete with timeout
        heartbeat_tasks = []
        completed_tasks = set()
        slaves_to_check = []
        
        for hostname, slave_info in list(self.active_slaves.items()):
            slaves_to_check.append((hostname, slave_info))
            
        if not slaves_to_check:
            logging.debug("No slaves to check")
            return
            
        for hostname, slave_info in slaves_to_check:
            task = asyncio.create_task(self._send_heartbeat_to_slave(hostname, slave_info))
            task.slave_hostname = hostname
            heartbeat_tasks.append(task)
        
        if heartbeat_tasks:
            # Wait for all tasks to complete with a timeout
            try:
                # Wait for all tasks with a timeout
                done, pending = await asyncio.wait(
                    heartbeat_tasks,
                    timeout=SLAVE_HEARTBEAT_TIMEOUT
                )
                
                for task in done:
                    try:
                        hostname = await task
                        completed_tasks.add(hostname)
                        logging.debug(f"Task for slave {hostname} completed successfully")
                    except Exception as e:
                        hostname = getattr(task, 'slave_hostname', 'unknown')
                        logging.error(f"Error in heartbeat task for slave {hostname}: {e}")
                
                # Cancel any pending tasks that didn't complete within the timeout
                for task in pending:
                    hostname = getattr(task, 'slave_hostname', 'unknown')
                    task.cancel()
                    logging.warning(f"Heartbeat task for slave {hostname} timed out and was cancelled")
                    
                # Clean up pending tasks
                if pending:
                    try:
                        await asyncio.gather(*pending, return_exceptions=True)
                    except asyncio.CancelledError:
                        pass
                    
            except Exception as e:
                logging.error(f"Error managing heartbeat tasks: {e}")
        
        # if slaves_to_check:
            # logging.info(f"Heartbeat cycle complete: {len(completed_tasks)}/{len(slaves_to_check)} slaves processed")
            
    def _manage_slave_heartbeats(self):
        """Synchronous wrapper for async heartbeat management"""
        if not self.is_leader:
            logging.warning("Only the leader should manage slave heartbeats")
            return
            
        # Create and run an event loop to execute the async functions
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self._async_manage_slave_heartbeats())
            loop.close()
        except Exception as e:
            logging.error(f"Error in asyncio event loop for heartbeat management: {e}")
