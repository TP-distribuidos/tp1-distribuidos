import logging
import os
import socket
import time
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)

# Get environment variables
WORKER_HOST = os.getenv("WORKER_HOST", "localhost")
WORKER_PORT = int(os.getenv("WORKER_PORT", 9001))
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", 5))

class Sentinel:
    def __init__(self, worker_host=WORKER_HOST, worker_port=WORKER_PORT, check_interval=CHECK_INTERVAL):
        self.worker_host = worker_host
        self.worker_port = worker_port
        self.check_interval = check_interval
        self.running = True
        self.socket = None
        
        logging.info(f"Sentinel initialized for worker at {worker_host}:{worker_port}")
        logging.info(f"Health check interval: {check_interval} seconds")
        
    def run(self):
        """Run the sentinel main loop"""
        logging.info(f"Sentinel starting for {self.worker_host}:{self.worker_port}")
        
        while self.running:
            try:
                # Check worker health
                if self._check_worker_health():
                    logging.info(f"\033[32mWorker {self.worker_host}:{self.worker_port} is healthy\033[0m")
                else:
                    logging.error(f"\033[31mWorker {self.worker_host}:{self.worker_port} is unhealthy\033[0m")
                    # Restart the worker and notify copies
                
                # Sleep until next check
                time.sleep(self.check_interval)
                
            except KeyboardInterrupt:
                logging.info("Sentinel stopped by user")
                self.running = False
            except Exception as e:
                logging.error(f"Error in sentinel loop: {e}")
                time.sleep(self.check_interval)  # Wait before retrying
                
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
            message = f"HEALTH_CHECK:{int(time.time())}"
            self.socket.sendall(message.encode('utf-8'))
            
            # Wait for response
            response = self.socket.recv(1024).decode('utf-8')
            
            # Check if response is valid
            if response == message:
                return True
            else:
                logging.warning(f"Invalid health check response: {response}")
                return False
                
        except socket.timeout:
            logging.error("Health check timed out")
            return False
        except ConnectionRefusedError:
            logging.error("Connection refused - worker may be down")
            return False
        except Exception as e:
            logging.error(f"Error checking worker health: {e}")
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