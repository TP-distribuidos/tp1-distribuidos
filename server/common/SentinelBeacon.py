import logging
import socket
import threading
import json

class SentinelBeacon:
    def __init__(self, port, name="Worker"):
        self._running = True
        self._port = port
        self._name = name
        self._server_socket = None
        self._sentinel_thread = None
        
        # Start monitoring thread
        self._start_monitoring()
    
    def _start_monitoring(self):
        """Start the sentinel server in a separate thread"""
        self._sentinel_thread = threading.Thread(target=self._run_sentinel_server)
        self._sentinel_thread.daemon = True  # Thread will exit when main thread exits
        self._sentinel_thread.start()
        logging.debug(f"Sentinel beacon started for {self._name} on port {self._port}")
    
    def _run_sentinel_server(self):
        """Run a simple echo server for the sentinel to connect to"""
        try:
            self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self._server_socket.bind(('0.0.0.0', self._port))
            self._server_socket.settimeout(1.0)  # Set timeout for accept
            self._server_socket.listen(5)
            
            logging.debug(f"Worker server running on port {self._port}")
            
            while self._running:
                try:
                    client_socket, addr = self._server_socket.accept()
                    client_socket.settimeout(5.0)  # Set timeout for receive/send
                    
                    # Handle client directly in this thread
                    self._handle_sentinel_client(client_socket, addr)
                    
                except socket.timeout:
                    # This is normal, just retry accept
                    continue
                except Exception as e:
                    if self._running:  # Only log if not shutting down
                        logging.error(f"Error accepting connection: {e}")
            
            if self._server_socket:
                self._server_socket.close()
                self._server_socket = None
            
        except Exception as e:
            logging.error(f"Sentinel server error: {e}")
    
    def _handle_sentinel_client(self, client_socket, addr):
        """Handle a sentinel client connection without using Serializer"""
        try:
            data = client_socket.recv(1024)
            if data:
                # Just echo back whatever we received - no deserialization
                client_socket.sendall(data)
            
            client_socket.close()
            
        except Exception as e:
            logging.error(f"Error handling sentinel client: {e}")
            try:
                client_socket.close()
            except:
                pass
    
    def shutdown(self):
        """Shut down the sentinel beacon"""
        self._running = False
        
        # Close the server socket to unblock accept
        if self._server_socket:
            try:
                self._server_socket.close()
            except Exception as e:
                logging.error(f"Error closing server socket: {e}")
        
        # Wait for the sentinel thread to finish
        if self._sentinel_thread and self._sentinel_thread.is_alive():
            self._sentinel_thread.join(timeout=2.0)
            
        logging.info(f"Sentinel beacon shut down for {self._name}")
