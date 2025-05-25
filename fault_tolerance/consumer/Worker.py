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
                    found_message10 = False
                    message10_count = 0
                    
                    # Iterate through entries looking for message_id 10
                    for key, entry in data.items():
                        if isinstance(entry, dict):
                            # Look for message_id or batch field using a generic approach
                            for id_field in ['message_id', 'batch']:
                                if id_field in entry:
                                    msg_id = entry.get(id_field)
                                    
                                    # Check if this is message_id 10
                                    if msg_id == '10' or msg_id == 10:
                                        if not found_message10:
                                            clients_to_process.append(client_id)
                                            found_message10 = True
                                        message10_count += 1
                                        break
                    
                    if found_message10:
                        logging.info(f"Found client {client_id} with {message10_count} 'message_id 10' entries")
            
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
            
            # Extract message ID with fallbacks for compatibility
            message_id = None
            for id_field in ['message_id', 'batch']:
                if id_field in deserialized_message:
                    message_id = deserialized_message.get(id_field)
                    break
            
            # Generate a message ID if none exists
            if message_id is None:
                logging.warning("Message received without ID field, generating one")
                message_id = str(int(time.time() * 1000))  # Use timestamp as fallback
                
            # Convert message_id to string for consistent handling
            message_id = str(message_id)
            
            logging.info(f"Received message - Message ID: {message_id}")
            
            client_id = "default"  # Using single client_id for this example
            
            # Log the message structure for debugging
            content_len = len(str(deserialized_message.get('content', '')))
            logging.info(f"Message content length: {content_len}")
            
            # Create a normalized copy of the message
            message_to_persist = deserialized_message.copy()
            
            # Ensure message_id is consistently set
            message_to_persist['message_id'] = message_id
            
            # Persist message to WAL - let WAL handle any format normalization
            if self.data_persistance.persist(client_id, message_to_persist, message_id):
                logging.info(f"Message {message_id} successfully persisted to WAL")
                
                # If this is message_id 10, write all data to file
                try:
                    msg_num = int(message_id) 
                    if msg_num == 10:
                        logging.info(f"Found message_id 10, writing all data to file")
                        await self._write_to_file(client_id)
                        logging.info(f"Successfully processed message_id 10")
                    else:
                        logging.info(f"Message {message_id} processed (not message_id 10)")
                except (ValueError, TypeError) as e:
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
            # Retrieve consolidated data from the persistence layer
            data = self.data_persistance.retrieve(client_id)
            if not data:
                logging.info(f"No data to write for client {client_id}")
                return
                
            logging.info(f"Writing data to file for client {client_id}")
            
            # Check for consolidated data from the WAL
            if isinstance(data, dict) and "_consolidated_data" in data:
                consolidated = data.get("_consolidated_data")
                if isinstance(consolidated, dict) and "content" in consolidated:
                    # We have pre-consolidated content from the WAL
                    content = consolidated.get("content", "")
                    if content:
                        message_count = len(consolidated.get("messages_id", []))
                        logging.info(f"Using pre-consolidated content from WAL with {message_count} messages")
                        # Format text with line breaks for readability
                        formatted_text = self._format_text_with_linebreaks(content, 100)
                        
                        # Write to file
                        await self._write_formatted_text(formatted_text)
                        logging.info(f"Successfully wrote consolidated text from WAL to file")
                        
                        self.data_persistance.clear(client_id)
                        return
            
            # If we don't have pre-consolidated content, extract it ourselves
            message_contents = {}
            
            # Process all entries from the persistence layer
            for key, entry in data.items():
                # Skip special keys and non-dictionary entries
                if not isinstance(entry, dict) or key.startswith("_"):
                    continue
                
                # Extract content and message_id without assuming specific internal structure
                content = entry.get("content")
                
                # Extract message_id with fallbacks for compatibility
                msg_id = None
                for id_field in ["message_id", "_message_id", "batch"]:
                    if id_field in entry:
                        msg_id = entry.get(id_field)
                        break
                
                # If key is numeric and we don't have a message_id, use the key
                if msg_id is None and isinstance(key, str) and key.isdigit():
                    msg_id = key
                
                # Store content with its message_id for ordering
                if content and msg_id is not None:
                    # Try to normalize message ID for proper sorting
                    try:
                        if isinstance(msg_id, str) and msg_id.isdigit():
                            msg_id = int(msg_id)
                    except (ValueError, TypeError):
                        # Keep as is if conversion fails
                        pass
                    
                    # Store the content
                    message_contents[msg_id] = content
            
            # If we found no valid content, exit
            if not message_contents:
                logging.info(f"No valid message content found for client {client_id}")
                return
                
            # Build combined content in message_id order
            logging.info(f"Building combined content from {len(message_contents)} messages")
            
            # Sort message IDs for ordered processing
            sorted_ids = sorted(message_contents.keys())
            
            # Combine message content in order
            combined_content = " ".join(message_contents[msg_id] for msg_id in sorted_ids if message_contents[msg_id])
            combined_content = combined_content.strip()
            
            logging.info(f"Built combined content with length {len(combined_content)}")
            
            # Format text with line breaks for readability
            formatted_text = self._format_text_with_linebreaks(combined_content, 100)
            
            # Write to file
            await self._write_formatted_text(formatted_text)
            logging.info(f"Successfully wrote lorem text to file")
            
            # No longer clearing WAL data - keeping it for improved fault tolerance
            logging.info(f"Keeping WAL data for client {client_id} for fault tolerance")
            
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
