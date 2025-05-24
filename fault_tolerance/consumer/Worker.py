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
SENTINEL_PORT = int(os.getenv("SENTINEL_PORT", 9002))
OUTPUT_FILE = os.getenv("OUTPUT_FILE", "/app/output/received_messages.txt")

class ConsumerWorker:
    def __init__(self, consumer_queue=CONSUMER_QUEUE):
        self._running = True
        self.consumer_queue = consumer_queue
        self.rabbitmq = RabbitMQClient()
        
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
        
        # Recover any previous state
        recovered_data = self.data_persistance.recover_state()
        if recovered_data:
            logging.info(f"Recovered data for {len(recovered_data)} clients")
            # We'll store client IDs that need processing
            clients_to_process = []
            
            # Identify clients with message_id 10 messages that need to be written
            for client_id, data in recovered_data.items():
                if data:
                    # Check if we have any message_id 10 messages that need to be written
                    for op_id, entry in data.items():
                        if isinstance(entry, dict) and (
                            entry.get('message_id') == '10' or 
                            entry.get('batch') == '10'  # Legacy support
                        ):
                            clients_to_process.append(client_id)
                            break
            
            # We'll process these clients during the run method
            self.clients_to_process = clients_to_process
            if clients_to_process:
                logging.info(f"Found {len(clients_to_process)} clients with message_id 10 messages to process")
        
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
            batch = deserialized_message.get('batch')  # Still handle 'batch' from producer
            message_id = batch  # Use batch as message_id
            logging.info(f"Received message - Message ID: {message_id}")

            client_id = "default"  # Using single client_id for this example
            
            # Log the message structure for debugging
            logging.info(f"Message content length: {len(str(deserialized_message.get('content', '')))}")
            
            # Persist message to WAL before processing (WAL will convert batch to message_id)
            if self.data_persistance.persist(client_id, deserialized_message, message_id):
                logging.info(f"Message {message_id} successfully persisted to WAL")
                
                # If this is message_id 10, write all data to file
                try:
                    msg_num = int(message_id) if isinstance(message_id, str) else message_id
                    if message_id and msg_num == 10:
                        logging.info(f"Found message_id 10 message, writing all data to file")
                        await self._write_to_file(client_id)
                        logging.info(f"Successfully processed message_id 10 message")
                    else:
                        logging.info(f"Message {message_id} processed (not message_id 10)")
                except (ValueError, TypeError) as e:
                    logging.warning(f"Error processing message_id {message_id}: {e}")
                    logging.info(f"Message with non-numeric message_id {message_id} processed")
                
                # Acknowledge message
                await message.ack()
                
            else:
                # WAL persistence failed
                logging.error(f"Failed to persist message {message_id} to WAL")
                await message.reject(requeue=True)
            
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            # Reject the message and requeue it
            await message.reject(requeue=True)
    
    async def _write_to_file(self, client_id):
        """Write client_id's messages to output file"""
        try:
            data = self.data_persistance.retrieve(client_id)
            if not data:
                logging.info(f"No data to write for client {client_id}")
                return
                
            logging.info(f"Writing data to file for client {client_id}")
            
            # Debug log for available keys
            logging.info(f"Data keys available: {', '.join(data.keys())}")
            
            # Check for checkpoint data first
            combined_content = ""
            
            # Look for checkpoint data with content
            for op_id, entry in data.items():
                if isinstance(entry, dict):
                    # Direct checkpoint data in _checkpoint_data
                    if op_id == "_checkpoint_data" and "content" in entry:
                        combined_content = entry.get("content", "")
                        logging.info(f"Found checkpoint content with length {len(combined_content)}")
                        break
                    # Check for data from checkpoint operations in the data field
                    if op_id.startswith(self.data_persistance.CHECKPOINT_PREFIX) and isinstance(entry, dict) and "data" in entry:
                        checkpoint_data = entry.get("data")
                        if isinstance(checkpoint_data, dict) and "content" in checkpoint_data:
                            combined_content = checkpoint_data.get("content", "")
                            logging.info(f"Found checkpoint content with length {len(combined_content)}")
                            break
            
            # If no combined content from checkpoint, create it from messages
            if not combined_content:
                # We'll collect all message content in order
                message_contents = {}
                message_ids = []
                
                # Collect content from each log entry
                for op_id, entry in data.items():
                    # Only process entries that are message dictionaries
                    if not isinstance(entry, dict):
                        continue
                        
                    # Look for message_id and content
                    msg_id = None
                    if "message_id" in entry:
                        msg_id = entry.get("message_id")
                    elif "_message_id" in entry:
                        msg_id = entry.get("_message_id")
                    elif "batch" in entry:  # Legacy support
                        msg_id = entry.get("batch")
                        
                    if msg_id and "content" in entry:
                        content = entry.get("content", "")
                        if not content:
                            continue
                            
                        try:
                            # Try to convert message ID to int for proper ordering
                            if isinstance(msg_id, str) and msg_id.isdigit():
                                msg_id = int(msg_id)
                            message_contents[msg_id] = content
                            message_ids.append(msg_id)
                        except (ValueError, TypeError):
                            # If message_id can't be interpreted as a number, use it as a string
                            message_contents[msg_id] = content
                            message_ids.append(msg_id)
                
                # Build combined content in message_id order
                logging.info("Building combined content from individual messages")
                
                # Sort unique message IDs to combine in order
                unique_ids = sorted(set(message_ids))
                for msg_id in unique_ids:
                    if msg_id in message_contents:
                        combined_content += message_contents[msg_id] + " "
                combined_content = combined_content.strip()
                logging.info(f"Built combined content with length {len(combined_content)}")
            
            # Format text with line breaks every ~100 characters for readability
            formatted_text = self._format_text_with_linebreaks(combined_content, 100)
            
            # Write to file (async)
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
            
            logging.info(f"Successfully wrote lorem text to file")
            
        except Exception as e:
            logging.error(f"Error writing to file: {e}")
    
    def _format_text_with_linebreaks(self, text, line_length=100):
        """Format text with line breaks for readability"""
        words = text.split()
        lines = []
        current_line = []
        current_length = 0
        
        for word in words:
            # Include space before word except for first word in line
            word_length = len(word)
            space_length = 1 if current_line else 0
            
            # If adding this word would exceed line length, start a new line
            if current_length + space_length + word_length > line_length and current_line:
                lines.append(" ".join(current_line))
                current_line = [word]
                current_length = word_length
            else:
                if current_line:  # Not the first word in the line
                    current_length += space_length
                current_line.append(word)
                current_length += word_length
        
        # Add the last line if it has content
        if current_line:
            lines.append(" ".join(current_line))
            
        return "\n".join(lines)
    
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
