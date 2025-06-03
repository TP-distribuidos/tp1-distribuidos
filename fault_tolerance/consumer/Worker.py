import asyncio
import logging
import os
import signal
import sys

import os
import sys
import time
import signal
import logging
import asyncio

from rabbitmq.Rabbitmq_client import RabbitMQClient
from common.Serializer import Serializer
from common.SentinelBeacon import SentinelBeacon
from ConsumerStateInterpreter import ConsumerStateInterpreter
from common.data_persistance.WriteAheadLog import WriteAheadLog
from common.data_persistance.FileSystemStorage import FileSystemStorage


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)

# Get environment variables
CONSUMER_QUEUE = os.getenv("CONSUMER_QUEUE", "test_queue")
RESPONSE_QUEUE = os.getenv("RESPONSE_QUEUE", "response_queue")  # Add response queue name
SENTINEL_PORT = int(os.getenv("SENTINEL_PORT", 9002))
OUTPUT_FILE = os.getenv("OUTPUT_FILE", "/app/output/received_messages.txt")
TARGET_BATCH = 7
EXPECTED_PRODUCERS = int(os.getenv("EXPECTED_PRODUCERS", 3))  # Add this

class ConsumerWorker:
    def __init__(self, consumer_queue=CONSUMER_QUEUE, response_queue=RESPONSE_QUEUE):
        self._running = True
        self.consumer_queue = consumer_queue
        self.response_queue = response_queue
        self.rabbitmq = RabbitMQClient()
        self.target_batch = TARGET_BATCH
        self.expected_producers = EXPECTED_PRODUCERS  # Store it

        # Create output directory if it doesn't exist and ensure it has proper permissions
        output_dir = os.path.dirname(OUTPUT_FILE)
        try:
            os.makedirs(output_dir, exist_ok=True)
            # Set permissions to ensure we can write to this directory
            if os.path.exists(output_dir):
                os.chmod(output_dir, 0o755)  # rwxr-xr-x
        except Exception as e:
            logging.warning(f"Could not set permissions on output directory: {e}")
        
        # Clear the output file when starting
        try:
            logging.info(f"Clearing output file: {OUTPUT_FILE}")
            with open(OUTPUT_FILE, 'w') as f:
                f.write("")  # Clear the file
                f.flush()
                os.fsync(f.fileno())  # Ensure write is committed to disk
            logging.info("Output file cleared")
        except Exception as e:
            logging.error(f"Error clearing output file: {e}")
            # Try to create an empty file
            try:
                open(OUTPUT_FILE, 'a').close()
                logging.info(f"Created empty output file at {OUTPUT_FILE}")
            except Exception as e2:
                logging.error(f"Failed to create output file: {e2}")
        
        # Initialize Data Persistance with Write-Ahead Log
        self.data_persistance = WriteAheadLog(
            state_interpreter=ConsumerStateInterpreter(),
            storage=FileSystemStorage(),
            service_name="consumer_worker",
            base_dir="/app/persistence"
        )
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
        # Initialize sentinel beacon
        self.sentinel_beacon = SentinelBeacon(SENTINEL_PORT, "Consumer Worker")
        
        logging.info(f"Consumer Worker initialized to consume from queue '{consumer_queue}'")
    
    async def run(self):
        """Run the worker, connecting to RabbitMQ and consuming messages"""
        try:
            # Connect to RabbitMQ
            if not await self._setup_rabbitmq():
                logging.error("Failed to set up RabbitMQ connection. Exiting.")
                return False
            
            logging.info(f"Consumer Worker running and consuming from queue '{self.consumer_queue}'")
            
            # Process any pending client data from previous runs
            if hasattr(self, 'clients_to_process') and self.clients_to_process:
                logging.info(f"Processing {len(self.clients_to_process)} clients with pending data")
                for client_id in self.clients_to_process:
                    try:
                        await self._write_to_file(client_id)
                    except Exception as e:
                        logging.error(f"Error processing pending data for client {client_id}: {e}")
            
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
        # Cancel all pending tasks that might be using the event loop
        try:
            # Don't cancel the current task (cleanup itself)
            current_task = asyncio.current_task()
            tasks = [t for t in asyncio.all_tasks() if t is not current_task]
            if tasks:
                logging.info(f"Cancelling {len(tasks)} pending tasks")
                for task in tasks:
                    task.cancel()
                # Give them a chance to clean up
                await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            logging.error(f"Error cancelling tasks during cleanup: {e}")
        
        # Close RabbitMQ connection
        if hasattr(self, 'rabbitmq'):
            try:
                await self.rabbitmq.close()
                logging.info("RabbitMQ connection closed")
            except Exception as e:
                logging.error(f"Error closing RabbitMQ connection: {e}")
    
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
            
        # Declare the response queue
        response_queue = await self.rabbitmq.declare_queue(self.response_queue, durable=True)
        if not response_queue:
            logging.error(f"Failed to declare response queue '{self.response_queue}'")
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
            
            value = deserialized_message.get('value')
            # Extract message ID with fallbacks for compatibility
            message_id = deserialized_message.get('batch')
            node_id = deserialized_message.get('node_id')
            
            # Generate a message ID if none exists
            if message_id is None:
                logging.warning("Message received without ID field, generating one")
                raise ValueError("Message ID is required but not found in the message")
                
            message_id = int(message_id) if not isinstance(message_id, int) else message_id
            
            # Default node_id if not provided
            if node_id is None:
                raise ValueError("Node ID is required but not found in the message")
            
            logging.info(f"Received message - Message ID: {message_id}, Node ID: {node_id}")
            
            client_id = "default"
                        
            if self.data_persistance.is_message_processed(client_id, node_id, message_id):
                logging.info(f"\033[33mMessage {message_id} from node {node_id} already processed, skipping\033[0m")
                await message.ack()
                return

            # Send to the response queue for further processing
            response_message = {
                'data': value,
                'message_id': message_id,
                'node_id': node_id,
                'client_id': client_id,
            }
            
            # Serialize and publish to response queue
            serialized_response = Serializer.serialize(response_message)
            publish_result = await self.rabbitmq.publish_to_queue(
                self.response_queue,
                serialized_response,
                persistent=True
            )
            
            if publish_result:
                logging.info(f"Published response for message ID {message_id} to '{self.response_queue}'")
            else:
                logging.error(f"Failed to publish response for message ID {message_id} to '{self.response_queue}'")
            
            # Persist message to WAL with node_id - let WAL handle any format normalization
            if self.data_persistance.persist(client_id, node_id, deserialized_message, message_id):
                if message_id == self.target_batch:
                    logging.info(f"Received target batch {self.target_batch}, processing data")
                    await self._write_to_file(client_id)
                
                await message.ack()
                
            else:
                # WAL persistence failed
                logging.error(f"Failed to persist message {message_id} to WAL for node {node_id}")
                await message.reject(requeue=True)
            
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            # Reject the message and requeue it
            await message.reject(requeue=True)
    
    async def _write_to_file(self, client_id):
        """Write client_id's messages to output file - retrieves ALL nodes for the client"""
        try:
            # Retrieve consolidated data from ALL nodes for this client
            data = self.data_persistance.retrieve(client_id)
            if not data:
                logging.info(f"No data to write for client {client_id}")
                return
                
            logging.info(f"Writing data to file for client {client_id} (all nodes)")
            
            # Handle the new numeric data structure from our state interpreter
            if isinstance(data, dict):
                # Check if this is our new numeric format
                if "total" in data and "count" in data:
                    total = data.get("total", 0)
                    count = data.get("count", 0)
                    processed_batches = data.get("processed_batches", [])
                    
                    # Count how many producers we have (by counting different node IDs in processed batches)
                    # Each producer sends target_batch messages, so total expected = num_producers * target_batch
                    expected_total_messages = count  # count includes all messages from all producers
                    
                    # For now, assume we expect at least 2 producers * 15 batches = 30 messages minimum
                    # You could make this configurable via environment variable
                    min_expected_messages = self.expected_producers * self.target_batch
                    
                    # Only clear data if we have received expected amount
                    should_clear = count >= min_expected_messages
                    
                    # Create a formatted summary text
                    summary_text = f"PROCESSING SUMMARY\n"
                    summary_text += f"==================\n"
                    summary_text += f"Total value accumulated: {total}\n"
                    summary_text += f"Number of batches processed: {count}\n"
                    summary_text += f"Minimum expected messages: {min_expected_messages}\n"
                    summary_text += f"Status: {'✓ COMPLETE' if should_clear else '⏳ WAITING FOR MORE DATA'}\n"
                    
                    if should_clear:
                        summary_text += f"Expected total for {count} batches: {count}\n"
                        summary_text += f"Math check: {'✓ CORRECT' if total == count else '✗ MISMATCH'}\n"
                    
                    # Write to file
                    await self._write_formatted_text(summary_text)
                    logging.info(f"Successfully wrote summary to file: total={total}, count={count}, clearing={should_clear}")
                    
                    # Only clear if we have enough data
                    if should_clear:
                        self.data_persistance.clear(client_id)
                        logging.info(f"Cleared all data for client {client_id}")
                    else:
                        logging.info(f"Not clearing data yet - waiting for more producers (have {count}, need {min_expected_messages})")
                    
                    return
            
            # If we get here, the data structure wasn't as expected
            logging.warning(f"Unexpected data structure returned from persistence layer: {type(data)}")
            logging.info(f"Data content: {data}")
            
        except Exception as e:
            logging.error(f"Error writing to file: {e}")
    
    async def _write_formatted_text(self, formatted_text):
        """Write formatted text to file with proper error handling"""
        try:
            # Try to get the running loop, and handle the case where there's no loop
            try:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, self._write_to_file_sync, formatted_text)
            except RuntimeError:
                # If there's no running event loop, perform synchronous write
                logging.info("No running event loop, performing synchronous write")
                self._write_to_file_sync(formatted_text)
        except Exception as e:
            logging.error(f"Error during file write operation: {e}")
            # Fall back to synchronous write
            self._write_to_file_sync(formatted_text)
    
    def _format_text_with_linebreaks(self, text, line_length=100):
        """Format text with line breaks after each full stop (period)"""
        # Replace all periods that are followed by a space with a period and newline
        # This preserves periods inside words (like numbers) and won't create newlines there
        formatted_text = text.replace('. ', '.\n')
        
        # Handle the case where the text ends with a period without a space after it
        if formatted_text and formatted_text[-1] == '.' and not formatted_text.endswith('.\n'):
            formatted_text += '\n'
            
        # Make sure there are no empty lines
        lines = [line for line in formatted_text.split('\n') if line.strip()]
        
        logging.info(f"Formatted text with {len(lines)} sentences")
        return '\n'.join(lines)
    
    def _write_to_file_sync(self, data):
        """Synchronous file write operation"""
        with open(OUTPUT_FILE, 'w') as f:  # Using 'w' to overwrite instead of append
            f.write(data)
            f.write("\n\n")
            f.flush()
            os.fsync(f.fileno())
    
    def _handle_shutdown(self, *_):
        """Handle shutdown signals"""
        logging.info("Shutting down consumer worker...")
        self._running = False
        
        # Shut down the sentinel beacon
        if hasattr(self, 'sentinel_beacon'):
            self.sentinel_beacon.shutdown()
