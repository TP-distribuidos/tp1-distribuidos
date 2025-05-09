import logging
import os
import socket
import time
import sys
import threading # New import
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
        self.hostname_sum = self._calculate_hostname_sum()
        self.peer_listener_thread = None
        self.peers = set() 

        logging.info(f"Sentinel initialized for worker at {worker_host}:{worker_port}")
        logging.info(f"Health check interval: {check_interval} seconds")
        logging.info(f"My ID: {self.hostname_sum}")
        logging.info(f"Service Name for peer discovery: {self.service_name}")
        logging.info(f"Peer communication port: {self.peer_port}")

    def _calculate_hostname_sum(self):
        return sum(ord(char) for char in self.hostname)

    def _start_peer_listener(self):
        """Starts a thread to listen for messages from peer sentinels."""
        self.peer_listener_thread = threading.Thread(target=self._peer_listener_loop, daemon=True)
        self.peer_listener_thread.start()
        logging.info(f"Peer listener started on port {self.peer_port}")

    def _peer_listener_loop(self):
        """Listens for incoming connections from peers and receives their hostname sums."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                s.bind(('0.0.0.0', self.peer_port))
                s.listen()
                logging.info(f"Listening for peer connections on 0.0.0.0:{self.peer_port}")
                while self.running:
                    try:
                        conn, addr = s.accept()
                        with conn:
                            logging.info(f"Peer connection from {addr}")
                            data = conn.recv(1024)
                            if data:
                                try:
                                   
                                    peer_sum_data = Serializer.deserialize(data)
                                    received_sum = peer_sum_data.get("sum", -1)
                                    logging.info(f"Received ID {received_sum} from {addr}")
                                except Exception as e:
                                    logging.error(f"Failed to deserialize or process message from peer {addr}: {e}. Raw data: {data}")
                    except Exception as e:
                        if self.running: 
                            logging.error(f"Error in peer listener accept/receive: {e}")
                        time.sleep(1) 
        except Exception as e:
            logging.error(f"Peer listener loop failed critically: {e}")
        logging.info("Peer listener shutdown.")

    def _discover_and_send_to_peers(self):
        """Discovers peers using DNS and sends them the current sentinel's hostname sum."""
        if not self.service_name:
            logging.warning("SERVICE_NAME not set, cannot discover peers.")
            return

        try:
            # Docker's DNS will resolve the service name to all its tasks (replicas)
            # The service_name (e.g., "sentinel1") will resolve to IPs of its replicas.
            
            addr_info = socket.getaddrinfo(self.service_name, self.peer_port, socket.AF_INET, socket.SOCK_STREAM)
            
            current_peers = set()
            for res in addr_info:
                peer_ip = res[4][0]
                # Avoid sending to self by checking resolved IP against local IPs (can be complex with Docker NAT)
                # A simpler approach for now is to just try connecting. If it's self, it might fail or connect to own listener.
                current_peers.add((peer_ip, self.peer_port))
            
            # Update self.peers and identify new peers to message
            new_peers = current_peers - self.peers 
            # self.peers.update(current_peers) # Keep track of all known peers over time

            logging.info(f"Discovered peers for {self.service_name}: {current_peers}")

            message_payload = {"hostname": self.hostname, "sum": self.hostname_sum}
            serialized_payload = Serializer.serialize(message_payload)

            for peer_host, peer_port_num in current_peers:
                # Simple check to avoid connecting to self if peer_host is one of our own IPs
                # This is not foolproof in all Docker network configs but a basic attempt.
                # A more robust way is if the listener identifies self-connection.
                # For now, we'll try to connect to all resolved IPs.
                # If the resolved IP and port match our listening IP/port, we might connect to ourselves.
                # The listener should ideally ignore messages from self if identifiable.
                try:
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                        s.settimeout(2) # Short timeout for peer connection
                        s.connect((peer_host, peer_port_num))
                        s.sendall(serialized_payload)
                        logging.info(f"Sent ID {self.hostname_sum} to peer {peer_host}:{peer_port_num}")
                except socket.timeout:
                    logging.warning(f"Timeout connecting to peer {peer_host}:{peer_port_num}")
                except ConnectionRefusedError:
                    logging.warning(f"Connection refused by peer {peer_host}:{peer_port_num} (possibly not ready or self)")
                except Exception as e:
                    logging.error(f"Error sending sum to peer {peer_host}:{peer_port_num}: {e}")
            
            # Update the set of peers we've attempted to contact
            self.peers.update(current_peers)

        except socket.gaierror:
            logging.warning(f"Could not resolve service name {self.service_name}. Peer discovery failed. This might be temporary if service is starting up.")
        except Exception as e:
            logging.error(f"Error in peer discovery or sending sum: {e}")

    def run(self):
        """Run the sentinel main loop"""
        logging.info(f"Sentinel starting for {self.worker_host}:{self.worker_port} (Hostname: {self.hostname}, Sum: {self.hostname_sum})")
        
        self._start_peer_listener() # Start listening for peer messages

        # Initial peer discovery and send
        # Give some time for other replicas to start their listeners
        time.sleep(5 + (self.hostname_sum % 5)) # Stagger initial send based on sum to reduce thundering herd
        self._discover_and_send_to_peers()

        while self.running:
            try:
                # Check worker health
                if self._check_worker_health():
                    logging.info(f"\033[32mWorker {self.worker_host}:{self.worker_port} is healthy\033[0m")
                else:
                    logging.error(f"\033[31mWorker {self.worker_host}:{self.worker_port} is unhealthy\033[0m")
                
                if int(time.time()) % (self.check_interval * 4) < self.check_interval :
                     logging.info("Periodic peer discovery and sum broadcast.")
                     self._discover_and_send_to_peers()

                time.sleep(self.check_interval)
                
            except KeyboardInterrupt:
                logging.info("Sentinel stopped by user")
                self.running = False
            except Exception as e:
                logging.error(f"Error in sentinel loop: {e}")
                time.sleep(self.check_interval)
                
        logging.info("Sentinel shutdown initiated...")
        if self.peer_listener_thread and self.peer_listener_thread.is_alive():
            logging.info("Waiting for peer listener thread to shut down...")
            # The listener loop checks self.running, so it should exit.
            # Socket accept might block, so closing the socket from outside or using timeout is good.
            # Since it's a daemon thread, it will also exit if main exits.
            # For graceful shutdown, ensure listener socket is closed.
            # The 'with socket.socket' in _peer_listener_loop should handle closing on exit.
            self.peer_listener_thread.join(timeout=5.0) 
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
            
            # Create a new socket and connect to the worker
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.settimeout(5)  # Set a timeout for connect and recv operations
            self.socket.connect((self.worker_host, self.worker_port))
            
            # Send a health check message
            message = {"timestamp": int(time.time())}
            serialized_message = Serializer.serialize(message)
            self.socket.sendall(serialized_message)
            
            # Wait for response
            data = self.socket.recv(1024)
            
            # Deserialize and check if response is valid
            try:
                response = Serializer.deserialize(data)
                if "timestamp" in response:
                    return True
                else:
                    logging.warning(f"Invalid health check response: {response}")
                    return False
            except Exception as e:
                logging.warning(f"Failed to deserialize response: {e}")
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
        # Create and run the sentinel
        sentinel = Sentinel()
        sentinel.run()
    except Exception as e:
        logging.error(f"Fatal error in sentinel: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()