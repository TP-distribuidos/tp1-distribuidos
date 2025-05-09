import logging
import os
import socket
import time
import sys
import threading
from common.Serializer import Serializer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)

# Get environment variables
WORKER_HOST = os.getenv("WORKER_HOST", "localhost")
WORKER_PORT = int(os.getenv("WORKER_PORT", 9001))
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", 5))
HOSTNAME = os.getenv("HOSTNAME", "unknown_host")
SERVICE_NAME = os.getenv("SERVICE_NAME", "sentinel")
PEER_PORT = int(os.getenv("PEER_PORT", 9010))

# Election and Leader Constants
ELECTION_TIMEOUT_DURATION = 10
LEADER_HEARTBEAT_INTERVAL = CHECK_INTERVAL * 2
LEADER_STALE_DURATION = LEADER_HEARTBEAT_INTERVAL * 2.5

class Sentinel:
    def __init__(self, worker_host=WORKER_HOST, worker_port=WORKER_PORT, check_interval=CHECK_INTERVAL):
        self.worker_host = worker_host
        self.worker_port = worker_port
        self.check_interval = check_interval
        self.running = True
        self.socket = None
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

        logging.info(f"Sentinel initialized for worker at {worker_host}:{worker_port}")
        logging.info(f"Health check interval: {check_interval} seconds")
        logging.info(f"My ID: {self.id}")
        logging.info(f"Service Name for peer discovery: {self.service_name}")
        logging.info(f"Peer communication port: {self.peer_port}")

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
        logging.info(f"Broadcast {message_type} to {sent_count}/{len(peers_to_send)} discovered peers. Payload: {payload}")


    def _start_peer_listener(self):
        self.peer_listener_thread = threading.Thread(target=self._peer_listener_loop, daemon=True)
        self.peer_listener_thread.start()
        logging.info(f"Peer listener started on port {self.peer_port}")

    def _peer_listener_loop(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                s.bind(('0.0.0.0', self.peer_port))
                s.settimeout(1.0) # Timeout for accept to allow checking self.running
                s.listen()
                logging.info(f"Listening for peer connections on 0.0.0.0:{self.peer_port}")
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

                                    if msg_type == "ID_ANNOUNCE":
                                        logging.info(f"Received ID_ANNOUNCE from {sender_id} (ID: {message.get('id')})")

                                    elif msg_type == "ELECTION_START":
                                        coordinator_id_from_msg = message.get('coordinator_id')
                                        logging.info(f"Received ELECTION_START from {sender_id} (Coordinator ID: {coordinator_id_from_msg}). My ID is {self.id}.")

                                        if sender_id < self.id: 
                                            logging.info(f"ELECTION_START from lower ID coordinator {sender_id}. Responding to assert higher ID and initiating my own election.")
                                            self._send_message_to_peer(addr[0], self.peer_port, "ELECTION_RESPONSE", {"id": self.id, "coordinator_id": sender_id})
                                            
                                            # Initiate my own election.
                                            # This node is higher, so it should take over or ensure a proper election happens.
                                            self._initiate_election()
                                        
                                        elif sender_id > self.id:
                                            logging.info(f"ELECTION_START from higher ID coordinator {sender_id}. Participating.")
                                            if self.i_am_election_coordinator:
                                                logging.info(f"Was coordinating an election, but ELECTION_START from higher ID {sender_id} received. Abdicating my coordination.")
                                                self.i_am_election_coordinator = False
                                                self.election_in_progress = False 
                                                self.election_votes = {} 
                                            
                                            self.election_in_progress = True
                                            self.i_am_election_coordinator = False # 
                                            self.current_leader_id = None
                                            self.is_leader = False
                                            self._send_message_to_peer(addr[0], self.peer_port, "ELECTION_RESPONSE", {"id": self.id, "coordinator_id": sender_id})

                                    elif msg_type == "ELECTION_RESPONSE":
                                        voter_id = message.get("id") 
                                        response_for_coordinator_id = message.get("coordinator_id")

                                        if self.i_am_election_coordinator and self.election_in_progress and response_for_coordinator_id == self.id:
                                            if voter_id > self.id:
                                                logging.info(f"Received ELECTION_RESPONSE from a higher ID node ({voter_id}). Abdicating my coordination. They should have started a new election.")
                                                self.i_am_election_coordinator = False
                                                self.election_in_progress = False 
                                                self.election_votes = {}
                                            else:
                                                logging.info(f"Received ELECTION_RESPONSE (vote) from {voter_id} for my election.")
                                                self.election_votes[voter_id] = voter_id

                                    elif msg_type == "LEADER_ANNOUNCEMENT":
                                        new_leader_id = message.get("leader_id")
                                        sender_of_announcement = sender_id
                                        logging.info(f"Received LEADER_ANNOUNCEMENT from {sender_of_announcement}: New leader is {new_leader_id}. My ID is {self.id}.")

                                        if new_leader_id < self.id and (self.current_leader_id is None or new_leader_id != self.current_leader_id):
                                            logging.warning(f"Contesting LEADER_ANNOUNCEMENT for {new_leader_id} (lower than my ID {self.id}). Initiating new election.")
                                            self.current_leader_id = None
                                            self.is_leader = False
                                            self.election_in_progress = False
                                            self.i_am_election_coordinator = False
                                            self._initiate_election()
                                        else:
                                            self.current_leader_id = new_leader_id
                                            self.is_leader = (self.id == new_leader_id)
                                            self.election_in_progress = False
                                            self.i_am_election_coordinator = False
                                            self.election_votes = {} 
                                            self.last_leader_heartbeat_time = time.time() 
                                            if self.is_leader:
                                                logging.info(f"\033[92mI AM THE NEW LEADER (ID: {self.id}) based on announcement from {sender_of_announcement}.\033[0m")
                                            else:
                                                logging.info(f"I am a SLAVE. Leader is {self.current_leader_id} (announced by {sender_of_announcement}).")

                                    elif msg_type == "LEADER_HEARTBEAT":
                                        leader_id_from_heartbeat = message.get("leader_id")
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

        logging.info(f"INITIATING ELECTION (My ID: {self.id}). Broadcasting ELECTION_START.")
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

        logging.info(f"Processing election results. My ID: {self.id}. Votes received: {self.election_votes}")
        
        if not self.election_votes:
            logging.warning("No votes received in election. Re-evaluating or re-electing might be needed.")
            # This could happen if this is the only node or peers didn't respond.
            # For simplicity, declare self leader if no other votes.
            new_leader_id = self.id
        else:
            new_leader_id = max(self.election_votes.keys()) # Highest ID wins

        logging.info(f"Election concluded. New leader determined to be ID: {new_leader_id}. Announcing...")
        self._broadcast_message("LEADER_ANNOUNCEMENT", {"leader_id": new_leader_id})

        self.current_leader_id = new_leader_id
        self.is_leader = (self.id == new_leader_id)
        self.election_in_progress = False
        self.i_am_election_coordinator = False
        self.election_votes = {} # Clear votes after processing
        self.last_leader_heartbeat_time = time.time() # New leader is now active

        if self.is_leader:
            logging.info(f"\033[92mI AM THE NEW LEADER (ID: {self.id}) after coordinating election.\033[0m")
        else:
            logging.info(f"I am a SLAVE after coordinating. New leader is {self.current_leader_id}.")

    def shutdown(self):
        logging.info(f"Shutdown called for Sentinel ID: {self.id}")
        self.running = False
        if self.socket:
            try:
                self.socket.close()
                logging.info("Closed worker health check socket.")
            except Exception as e:
                logging.warning(f"Error closing worker health check socket: {e}")
            self.socket = None

    def run(self):
        logging.info(f"Sentinel starting (ID: {self.id}) for worker {self.worker_host}:{self.worker_port}")
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

                # Leader tasks
                if self.is_leader:
                    if self._check_worker_health():
                        logging.info(f"\033[32mLeader {self.id}: Worker {self.worker_host}:{self.worker_port} is healthy\033[0m")
                    else:
                        logging.error(f"\033[31mLeader {self.id}: Worker {self.worker_host}:{self.worker_port} is unhealthy\033[0m")
                    
                    # Broadcast leader heartbeat
                    if current_time - getattr(self, '_last_heartbeat_sent_time', 0) > LEADER_HEARTBEAT_INTERVAL:
                        # logging.debug(f"Leader {self.id} sending LEADER_HEARTBEAT.")
                        self._broadcast_message("LEADER_HEARTBEAT", {"leader_id": self.id})
                        self._last_heartbeat_sent_time = current_time
                
                else:
                    if self.current_leader_id is not None: # We know a leader
                        if (current_time - self.last_leader_heartbeat_time) > LEADER_STALE_DURATION:
                            logging.warning(f"Leader {self.current_leader_id} is STALE. Last heartbeat at {self.last_leader_heartbeat_time:.2f}. Initiating new election.")
                            self.current_leader_id = None # Consider leader lost
                            self.is_leader = False # Ensure not leader
                            self._initiate_election()
                            last_election_initiation_attempt = current_time
                        # else: Slave: Monitoring leader
                    
                    else: # No leader known
                        if not self.election_in_progress and not self.election_message_received_this_cycle:
                            # Avoid starting election too frequently if one just failed or if messages are flowing
                            if current_time - last_election_initiation_attempt > ELECTION_TIMEOUT_DURATION * 1.5 : # Add some buffer
                                logging.info("No leader known and no election in progress. Attempting to initiate election.")
                                self._initiate_election()
                                last_election_initiation_attempt = current_time

                # Election coordinator tasks (if this instance initiated an election)
                if self.i_am_election_coordinator and self.election_in_progress:
                    if (current_time - self.election_start_time) > ELECTION_TIMEOUT_DURATION:
                        logging.info(f"Election timeout reached for coordinator {self.id}. Processing results.")
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

    def _check_worker_health(self):
        """Check if the worker is healthy by connecting to its echo server"""
        try:
            # Close previous connection if open
            if self.socket:
                try:
                    self.socket.close()
                except:
                    pass
                self.socket = None
            
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.settimeout(5) 
            self.socket.connect((self.worker_host, self.worker_port))
            
            message = {"timestamp": int(time.time())} 
            serialized_message = Serializer.serialize(message)
            self.socket.sendall(serialized_message)
            
            data = self.socket.recv(1024)
            
            try:
                response = Serializer.deserialize(data)
                if "timestamp" in response:
                    return True
                else:
                    logging.warning(f"Invalid health check response: {response}")
                    return False
            except Exception as e:
                logging.warning(f"Failed to deserialize health check response: {e}")
                return False
                
        except socket.timeout:
            logging.error(f"Health check for {self.worker_host}:{self.worker_port} timed out")
            return False
        except ConnectionRefusedError:
            logging.error(f"Connection refused for {self.worker_host}:{self.worker_port} - worker may be down")
            return False
        except Exception as e:
            logging.error(f"Error checking worker health for {self.worker_host}:{self.worker_port}: {e}")
            return False

def main():
    try:
        sentinel = Sentinel()
        sentinel.run()
    except Exception as e:
        logging.error(f"Fatal error in sentinel: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()