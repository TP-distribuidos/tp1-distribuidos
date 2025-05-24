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
        
        # Create output directory if it doesn't exist
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        
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
            # Process any pending data from previous runs
            for client_id, data in recovered_data.items():
                if data:
                    # Check if we have any batch 10 messages that need to be written
                    for op_id, entry in data.items():
                        if isinstance(entry, dict) and entry.get('batch') == 10:
                            loop = asyncio.get_event_loop()
                            loop.create_task(self._write_to_file(client_id))
                            break

        logging.info(f"Consumer Worker initialized to consume from queue '{consumer_queue}'")
    
    
    async def run(self):
        """Run the worker, connecting to RabbitMQ and consuming messages"""
        try:
            # Connect to RabbitMQ
            if not await self._setup_rabbitmq():
                logging.error("Failed to set up RabbitMQ connection. Exiting.")
                return False
            
            logging.info(f"Consumer Worker running and consuming from queue '{self.consumer_queue}'")
            
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

            client_id = "default"  # Using single client_id for this example
            
            # Use batch as operation_id (the WAL will handle internal IDs)
            logging.info(f"Processing message {batch}")
            
            # Persist message to WAL before processing
            if self.data_persistance.persist(client_id, deserialized_message, batch):
                logging.info(f"Message {batch} successfully persisted to WAL")
                
                # If this is batch 10, write all data to file
                try:
                    batch_num = int(batch) if isinstance(batch, str) else batch
                    if batch and batch_num == 10:
                        logging.info(f"Found batch 10 message, writing all data to file")
                        await self._write_to_file(client_id)
                        logging.info(f"Successfully processed batch 10 message")
                    else:
                        logging.info(f"Message {batch} processed (not batch 10)")
                except (ValueError, TypeError) as e:
                    logging.warning(f"Error processing batch {batch}: {e}")
                    logging.info(f"Message with non-numeric batch {batch} processed")
                    
                # Note: Not clearing data to allow for recovery testing

                # After successful processing, clean up WAL entry
                # self.data_persistance.clear(client_id)
                
                # logging.info(f"\033[33mMessage {batch} cleared from WAL, waiting 5s before acknowledging...\033[0m")
                # await asyncio.sleep(5)
                
                # Acknowledge message
                await message.ack()
                # logging.info(f"\033[91mMessage {batch} acknowledged, waiting 5s before processing next message...\033[0m")
                # await asyncio.sleep(5)
            else:
                # WAL persistence failed
                logging.error(f"Failed to persist message {batch} to WAL")
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
            
            # We'll collect all batch content in order
            batch_contents = {}
            batch_nums = []
            
            # First check if we have a full_content_entry from a checkpoint
            full_content = ""
            if "_full_content_entry" in data:
                full_content = data["_full_content_entry"].get("content", "")
            
            # Also collect individual messages
            for op_id, entry in data.items():
                # Skip the synthetic entry
                if op_id == "_full_content_entry":
                    continue
                    
                # Only process entries that are message dictionaries
                if isinstance(entry, dict) and 'batch' in entry and 'content' in entry:
                    batch_num = entry.get('batch')
                    try:
                        # Try to convert batch number to int for proper ordering
                        if isinstance(batch_num, str) and batch_num.isdigit():
                            batch_num = int(batch_num)
                        batch_contents[batch_num] = entry.get('content', '')
                        batch_nums.append(batch_num)
                    except (ValueError, TypeError):
                        # If batch can't be interpreted as a number, use it as a string
                        batch_contents[batch_num] = entry.get('content', '')
                        batch_nums.append(batch_num)
            
            # If we don't have full content from the checkpoint, build it from individual messages
            if not full_content:
                # Sort unique batch numbers to combine in order
                unique_batches = sorted(set(batch_nums))
                combined_content = ""
                for batch in unique_batches:
                    if batch in batch_contents:
                        combined_content += batch_contents[batch] + " "
                full_content = combined_content.strip()
            
            # Format text with line breaks every ~100 characters for readability
            formatted_text = self._format_text_with_linebreaks(full_content, 100)
            
            # Write to file (async)
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._write_to_file_sync, formatted_text)
            
            logging.info(f"Successfully wrote lorem text to file")
            
            # Clear all logs and checkpoints from persistence folder for this client
            # Commented out to allow for recovery testing - uncomment to enable cleanup after writing
            # logging.info(f"Cleaning up persistence data for client {client_id}")
            # self.data_persistance.clear(client_id)
            # logging.info(f"Persistence data cleared for client {client_id}")
            
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
        with open(OUTPUT_FILE, 'a') as f:
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
