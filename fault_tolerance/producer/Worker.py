import asyncio
import logging
import os
import signal
import sys
import time
import lorem

# Add parent directory to Python path to allow imports from sibling directories
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from rabbitmq.Rabbitmq_client import RabbitMQClient
from common.Serializer import Serializer
from common.SentinelBeacon import SentinelBeacon

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)

# Get environment variables
PRODUCER_QUEUE = os.getenv("PRODUCER_QUEUE", "test_queue")
SENTINEL_PORT = int(os.getenv("SENTINEL_PORT", 9001))
NUM_BATCHES = 7
BATCH_INTERVAL = 2  # seconds
NODE_ID = os.getenv("NODE_ID")
# File to store sent messages for verification
PRODUCER_LOG_FILE = "/app/output/producer_log.txt"

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
        
        # Initialize sentinel beacon
        self.sentinel_beacon = SentinelBeacon(SENTINEL_PORT, "Producer Worker")
        
        logging.info(f"Producer Worker initialized to send to queue '{producer_queue}'")
    
    async def run(self):
        """Run the worker, connecting to RabbitMQ and sending messages"""
        try:
            # Connect to RabbitMQ
            if not await self._setup_rabbitmq():
                logging.error("Failed to set up RabbitMQ connection. Exiting.")
                return False
            
            logging.info(f"Producer Worker running and sending to queue '{self.producer_queue}'")
            
            # Send batches of messages
            batch_count = 0
            while self._running and batch_count < NUM_BATCHES:
                await self._send_batch(batch_count + 1)
                batch_count += 1
                await asyncio.sleep(BATCH_INTERVAL)
            
            logging.info(f"Finished sending {NUM_BATCHES} batches")
            
            # Keep the worker running until shutdown is triggered
            while self._running:
                await asyncio.sleep(1)
                
            return True
        finally:
            # Always clean up resources
            await self.cleanup()
    
    async def cleanup(self):
        """Clean up resources properly"""
        logging.info("Cleaning up resources...")
        if hasattr(self, 'rabbitmq'):
            try:
                await self.rabbitmq.close()
                logging.info("RabbitMQ connection closed")
            except Exception as e:
                logging.error(f"Error closing RabbitMQ connection: {e}")
    
    async def _setup_rabbitmq(self, retry_count=1):
        """Set up RabbitMQ connection and producer queue"""
        # Connect to RabbitMQ
        connected = await self.rabbitmq.connect()
        if not connected:
            logging.error(f"Failed to connect to RabbitMQ, retrying in {retry_count} seconds...")
            wait_time = min(30, 2 ** retry_count)
            await asyncio.sleep(wait_time)
            return await self._setup_rabbitmq(retry_count + 1)
        
        # Declare the producer queue
        queue = await self.rabbitmq.declare_queue(self.producer_queue, durable=True)
        if not queue:
            logging.error(f"Failed to declare queue '{self.producer_queue}'")
            return False
        
        return True
    
    async def _send_batch(self, batch_number):
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
            
            # Send the message
            success = await self.rabbitmq.publish_to_queue(
                queue_name=self.producer_queue,
                message=Serializer.serialize(message),
                persistent=True
            )
            if not success:
                logging.error(f"Failed to send message: {message}")
            else:
                logging.info(f"Sent batch {batch_number} with value 1")
            
            # If this is the final batch, also write a summary
            if batch_number == NUM_BATCHES:
                self._write_summary_log()
            
        except Exception as e:
            logging.error(f"Error sending messages: {e}")
    
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