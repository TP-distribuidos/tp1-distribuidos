import logging
import os
import signal
import sys
import time

# Add parent directory to Python path to allow imports from sibling directories
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from rabbitmq.Rabbitmq_client import RabbitMQClient
from common.Serializer import Serializer
from common.SentinelBeacon import SentinelBeacon
# Import WAL and required interfaces
from common.data_persistance.WriteAheadLog import WriteAheadLog
from common.data_persistance.FileSystemStorage import FileSystemStorage
from common.data_persistance.SimpleStateInterpreter import SimpleStateInterpreter
import random

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)

# Get environment variables
PRODUCER_QUEUE = os.getenv("PRODUCER_QUEUE", "test_queue")
SENTINEL_PORT = int(os.getenv("SENTINEL_PORT", 9001))
NUM_BATCHES = int(os.getenv("NUM_BATCHES", 7))
BATCH_INTERVAL = float(os.getenv("BATCH_INTERVAL", 2))  # seconds
NODE_ID = os.getenv("NODE_ID", "default_node")
# File to store sent messages for verification
PRODUCER_LOG_FILE = "/app/output/producer_log.txt"
# WAL persistence directory
WAL_DIR = "/app/persistence/producer"

class ProducerWorker:
    def __init__(self, producer_queue=PRODUCER_QUEUE):
        self._running = True
        self.producer_queue = producer_queue
        self.rabbitmq = RabbitMQClient()
        
        # Create output directory for log file
        os.makedirs(os.path.dirname(PRODUCER_LOG_FILE), exist_ok=True)
        
        # Clear previous log file at startup
        open(PRODUCER_LOG_FILE, 'w').close()
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
        # Initialize Write-Ahead Log for counter persistence
        self._initialize_wal()
        
        # Initialize sentinel beacon
        self.sentinel_beacon = SentinelBeacon(SENTINEL_PORT, "Producer Worker")
        
        logging.info(f"Producer Worker initialized to send to queue '{producer_queue}'")
    
    def _initialize_wal(self):
        """Initialize the Write-Ahead Log for counter persistence"""
        try:
            # Create directory for WAL if it doesn't exist
            os.makedirs(WAL_DIR, exist_ok=True)
            
            # Create simple implementations of required interfaces
            state_interpreter = SimpleStateInterpreter()
            storage = FileSystemStorage()
            
            # Initialize WAL
            self.wal = WriteAheadLog(
                state_interpreter=state_interpreter,
                storage=storage,
                service_name="producer_counter",
                base_dir=WAL_DIR
            )
            
            # Log the recovered counter value
            counter_value = self.wal.get_counter_value()
            logging.info(f"Recovered batch counter: {counter_value}")
            
        except Exception as e:
            logging.error(f"Error initializing WAL: {e}")
            self.wal = None
    
    def run(self):
        """Run the worker, connecting to RabbitMQ and sending messages"""
        try:
            # Connect to RabbitMQ
            if not self._setup_rabbitmq():
                logging.error("Failed to set up RabbitMQ connection. Exiting.")
                return False
            
            logging.info(f"Producer Worker running and sending to queue '{self.producer_queue}'")
            
            # Send batches of messages
            while self._running:
                # Check if we have a WAL
                if self.wal is None:
                    logging.error("WAL not initialized, cannot continue sending messages")
                    break
                
                # Check if we've reached the batch limit
                current_batch = self.wal.get_counter_value()
                if current_batch >= NUM_BATCHES:
                    logging.info(f"Reached batch limit: {current_batch}/{NUM_BATCHES}")
                    break
                
                # Get the next batch number (current value + 1)
                next_batch = current_batch + 1
                
                # Send the batch with that number
                sent_successfully = self._send_batch(next_batch)
                
                # If send was successful, increment the counter
                if sent_successfully:
                    self.wal.increment_counter()
                    logging.info(f"Batch counter incremented to {self.wal.get_counter_value()}")
                else:
                    logging.error(f"Failed to send batch {next_batch}, not incrementing counter")
                
                time.sleep(BATCH_INTERVAL)
            
            logging.info(f"Finished sending {NUM_BATCHES} batches")
            
            # Keep the worker running until shutdown is triggered
            while self._running:
                time.sleep(1)
                
            return True
        finally:
            # Always clean up resources
            self.cleanup()
    
    def cleanup(self):
        """Clean up resources properly"""
        logging.info("Cleaning up resources...")
        if hasattr(self, 'rabbitmq'):
            try:
                self.rabbitmq.close()
                logging.info("RabbitMQ connection closed")
            except Exception as e:
                logging.error(f"Error closing RabbitMQ connection: {e}")
    
    def _setup_rabbitmq(self, retry_count=1):
        """Set up RabbitMQ connection and producer queue"""
        # Connect to RabbitMQ
        connected = self.rabbitmq.connect()
        if not connected:
            logging.error(f"Failed to connect to RabbitMQ, retrying in {retry_count} seconds...")
            wait_time = min(30, 2 ** retry_count)
            time.sleep(wait_time)
            return self._setup_rabbitmq(retry_count + 1)
        
        # Declare the producer queue
        queue_declared = self.rabbitmq.declare_queue(self.producer_queue, durable=True)
        if not queue_declared:
            logging.error(f"Failed to declare queue '{self.producer_queue}'")
            return False
        
        return True
    
    def _send_batch(self, batch_number):
        """Send a batch of messages"""
        try:
            # Send the number 1 to be added up
            message = {
                "batch": batch_number,
                "timestamp": time.time(),
                "value": 1,  # Always send 1 to add up
                "node_id": NODE_ID
            }
            
            # Log the message to file
            with open(PRODUCER_LOG_FILE, 'a') as f:
                f.write(f"--- BATCH {batch_number} ---\n")
                f.write(f"Value: {message['value']}\n\n")

            logging.info(f"Sleeping 3 seconds before sending batch {batch_number}...")
            time.sleep(3)  # Ensure file write is complete
            
            # Send the message
            success = self.rabbitmq.publish_to_queue(
                queue_name=self.producer_queue,
                message=Serializer.serialize(message),
                persistent=True
            )
            
            # Send duplicate message to simulate fault tolerance
            success = self.rabbitmq.publish_to_queue(
                queue_name=self.producer_queue,
                message=Serializer.serialize(message),
                persistent=True
            )

            # Randomly send a third duplicate message with 25% probability
            if random.random() < 0.25:
                logging.info("Randomly sending a third duplicate message (25% chance)")
                # Send duplicate message to simulate fault tolerance
                success = self.rabbitmq.publish_to_queue(
                    queue_name=self.producer_queue,
                    message=Serializer.serialize(message),
                    persistent=True
                )
            
            if not success:
                logging.error(f"Failed to send message: {message}")
                return False
            else:
                logging.info(f"Sent batch {batch_number} with value 1")
            
            # If this is the final batch, also write a summary
            if batch_number == NUM_BATCHES:
                self._write_summary_log()
            
            return True
            
        except Exception as e:
            logging.error(f"Error sending messages: {e}")
            return False
    
    def _write_summary_log(self):
        """Write summary log of all values sent"""
        try:
            total_sent = NUM_BATCHES  # Since we send 1 in each batch
            
            # Append summary to the log file
            with open(PRODUCER_LOG_FILE, 'a') as f:
                f.write(f"\n\n--- SUMMARY ---\n")
                f.write(f"Total batches sent: {NUM_BATCHES}\n")
                f.write(f"Value per batch: 1\n")
                f.write(f"Total value sent: {total_sent}\n")
                
            logging.info(f"Producer sent total value of {total_sent} across {NUM_BATCHES} batches")
        except Exception as e:
            logging.error(f"Error writing summary log: {e}")
    
    def _handle_shutdown(self, *_):
        """Handle shutdown signals"""
        logging.info("Shutting down producer worker...")
        self._running = False
        
        # Shut down the sentinel beacon
        if hasattr(self, 'sentinel_beacon'):
            self.sentinel_beacon.shutdown()

if __name__ == "__main__":
    worker = ProducerWorker()
    worker.run()
