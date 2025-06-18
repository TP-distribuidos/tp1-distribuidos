import time
import logging
import threading
import socket
from Config import Config

class ReconnectionManager:
    def __init__(self, client, max_retries=10, initial_backoff=4, max_backoff=60):
        """
        Initialize the reconnection manager
        """
        self.client = client
        self.max_retries = max_retries
        self.initial_backoff = initial_backoff
        self.max_backoff = max_backoff
        self.reconnection_lock = threading.Lock()
        self.reconnecting = threading.Event()
        self.should_stop = threading.Event()
        self.config = Config()
        # Cache the IP address for fallback during DNS outages
        self.last_known_ip = None
        
        # Log the configuration for easier debugging
        logging.info(f"ReconnectionManager initialized with host: {self.config.get_host()}, port: {self.config.get_port()}")
    
    def reconnect(self):
        """
        Attempt to reconnect with exponential backoff
        Returns True if reconnection successful, False otherwise
        """
        # Only one thread should handle reconnection
        if not self.reconnection_lock.acquire(False):
            # Another thread is already handling reconnection
            self.reconnecting.wait()  # Wait for reconnection to complete
            return not self.should_stop.is_set()
        
        try:
            self.reconnecting.set()  # Signal that reconnection is in progress
            
            retry_count = 0
            current_backoff = self.initial_backoff
            
            while retry_count < self.max_retries and not self.should_stop.is_set():
                try:
                    logging.info(f"Attempting to reconnect (attempt {retry_count + 1}/{self.max_retries})...")
                    
                    # Close the old socket if it exists
                    if self.client.skt:
                        try:
                            self.client.skt.close()
                        except:
                            pass
                        self.client.skt = None
                    
                    # Try to connect using hostname first
                    host = self.config.get_host()
                    port = self.config.get_port()
                    logging.info(f"Trying to connect to boundary at {host}:{port}")
                    
                    try:
                        # Try to resolve the hostname first to check DNS
                        if self.last_known_ip is None:
                            # First, try to resolve the hostname to get IP
                            try:
                                logging.info(f"Resolving hostname: {host}")
                                addr_info = socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM)
                                self.last_known_ip = addr_info[0][4][0]  # Extract IP from getaddrinfo result
                                logging.info(f"✅ Successfully resolved {host} to IP: {self.last_known_ip}")
                            except socket.gaierror as e:
                                logging.warning(f"⚠️ Could not resolve hostname {host}: {e}")
                                # Continue anyway, we'll try direct connection
                        else:
                            logging.info(f"Using cached IP for {host}: {self.last_known_ip}")
                        
                        # Attempt to reconnect with hostname
                        logging.info(f"Attempting connection using hostname: {host}:{port}")
                        self.client.connect(host, port)
                        
                        # After successful connection, get the actual connection details
                        if self.client.skt:
                            local_addr = self.client.skt.getsockname()
                            remote_addr = self.client.skt.getpeername()
                            logging.info(f"✅ Connection successful! Local: {local_addr}, Remote: {remote_addr}")
                            
                            # Update last known IP with the actual connected IP
                            self.last_known_ip = remote_addr[0]
                            
                        logging.info("\033[92mReconnection successful using hostname!\033[0m")
                        return True
                    except socket.gaierror as e:
                        # If hostname resolution fails, try the last known IP if we have it
                        logging.warning(f"⚠️ Hostname connection failed: {e}")
                        if self.last_known_ip:
                            logging.info(f"Hostname resolution failed, trying last known IP: {self.last_known_ip}:{port}")
                            try:
                                # Create new socket and connect directly with IP
                                self.client.skt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                                self.client.skt.connect((self.last_known_ip, port))
                                
                                # Log connection details
                                local_addr = self.client.skt.getsockname()
                                remote_addr = self.client.skt.getpeername()
                                logging.info(f"✅ IP connection successful! Local: {local_addr}, Remote: {remote_addr}")
                                
                                logging.info("\033[92mReconnection successful using IP address!\033[0m")
                                return True
                            except (socket.error, ConnectionRefusedError, TimeoutError) as e:
                                logging.warning(f"⚠️ IP reconnection failed: {e}")
                                # Fall through to retry logic below
                        else:
                            logging.warning("⚠️ Hostname resolution failed and no known IP available")
                        # Fall through to retry logic below
                    
                except (ConnectionRefusedError, TimeoutError, socket.gaierror, OSError) as e:
                    retry_count += 1
                    if retry_count >= self.max_retries:
                        logging.error(f"❌ Failed to reconnect after {self.max_retries} attempts")
                        break
                    
                    logging.info(f"⚠️ Reconnection failed: {e}. Retrying in {current_backoff} seconds...")
                    time.sleep(current_backoff)
                    
                    # Increase backoff time for next attempt (exponential backoff)
                    current_backoff = min(current_backoff * 2, self.max_backoff)
            
            return False
        finally:
            self.reconnecting.clear()  # Signal that reconnection attempt is complete
            self.reconnection_lock.release()
    
    def stop(self):
        """Signal that reconnection should stop (e.g., during shutdown)"""
        self.should_stop.set()
        self.reconnecting.set()  # Wake up any waiting threads