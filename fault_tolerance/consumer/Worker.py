import asyncio
import logging
import os
import signal
import sys

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
CONSUMER_QUEUE = os.getenv("CONSUMER_QUEUE", "test_queue")
SENTINEL_PORT = int(os.getenv("SENTINEL_PORT", 9002))
OUTPUT_FILE = os.getenv("OUTPUT_FILE", "/app/output/received_messages.txt")

class ConsumerWorker:
    def __init__(self, consumer_queue=CONSUMER_QUEUE):
        self._running = True
        self.consumer_queue = consumer_queue
        self.rabbitmq = RabbitMQClient()
        
        # Create output directory if it doesn't exist
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
        # Initialize sentinel beacon
        self.sentinel_beacon = SentinelBeacon(SENTINEL_PORT, "Consumer Worker")
        
        logging.info(f"Consumer Worker initialized to consume from queue '{consumer_queue}'")
    
    async def run(self):
        """Run the worker, connecting to RabbitMQ and consuming messages"""
        # Connect to RabbitMQ
        if not await self._setup_rabbitmq():
            logging.error("Failed to set up RabbitMQ connection. Exiting.")
            return False
        
        logging.info(f"Consumer Worker running and consuming from queue '{self.consumer_queue}'")
        
        # Keep the worker running until shutdown is triggered
        while self._running:
            await asyncio.sleep(1)
            
        return True
    
    async def _setup_rabbitmq(self, retry_count=1):
        """Set up RabbitMQ connection and consumer"""
        # Connect to RabbitMQ
        connected = await self.rabbitmq.connect()
        if not connected:
            logging.error(f"Failed to connect to RabbitMQ, retrying in {retry_count} seconds...")
            wait_time = min(30, 2 ** retry_count)
            await asyncio.sleep(wait_time)
            return await self._setup_rabbitmq(retry_count + 1)
        
        # Declare the consumer queue
        queue = await self.rabbitmq.declare_queue(self.consumer_queue, durable=True)
        if not queue:
            logging.error(f"Failed to declare queue '{self.consumer_queue}'")
            return False
        
        # Set up consumer
        success = await self.rabbitmq.consume(
            queue_name=self.consumer_queue,
            callback=self._process_message,
            no_ack=False
        )
        if not success:
            logging.error(f"Failed to set up consumer for queue '{self.consumer_queue}'")
            return False
            
        return True
    
    async def _process_message(self, message):
        """Process a message from the queue"""
        try:
            # Deserialize the message
            deserialized_message = Serializer.deserialize(message.body)
            
            # Log the received message
            batch = deserialized_message.get('batch')
            logging.info(f"Received message - Batch: {batch}")
            
            # Write message to file
            await self._write_to_file(deserialized_message)
            
            # Acknowledge message
            await message.ack()
            
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            # Reject the message and requeue it
            await message.reject(requeue=True)
    
    async def _write_to_file(self, message):
        """Write message to output file"""
        try:
            # Format message for output - simplified format with no labels
            formatted_message = f"--- Message {message.get('batch')} ---\n"
            formatted_message += f"{message.get('content')}\n\n"
            
            # Write to file (async)
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._write_to_file_sync, formatted_message)
            
        except Exception as e:
            logging.error(f"Error writing to file: {e}")
    
    def _write_to_file_sync(self, data):
        """Synchronous file write operation"""
        with open(OUTPUT_FILE, 'a') as f:
            f.write(data)
    
    def _handle_shutdown(self, *_):
        """Handle shutdown signals"""
        logging.info("Shutting down consumer worker...")
        self._running = False
        
        # Shut down the sentinel beacon
        if hasattr(self, 'sentinel_beacon'):
            self.sentinel_beacon.shutdown()
        
        # Close RabbitMQ connection - note we need to create a task
        # since this is called from a signal handler
        if hasattr(self, 'rabbitmq'):
            asyncio.create_task(self.rabbitmq.close())