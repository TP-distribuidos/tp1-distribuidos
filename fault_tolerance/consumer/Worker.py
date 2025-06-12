import logging
import os
import signal
import time

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
MONITORING_QUEUE = os.getenv("MONITORING_QUEUE", "monitoring_queue")
SENTINEL_PORT = int(os.getenv("SENTINEL_PORT", 9002))
OUTPUT_FILE = os.getenv("OUTPUT_FILE", "/app/output/received_messages.txt")
TARGET_BATCH = 7
EXPECTED_PRODUCERS = int(os.getenv("EXPECTED_PRODUCERS", 3))

class ConsumerWorker:
    def __init__(self, consumer_queue=CONSUMER_QUEUE, monitoring_queue=MONITORING_QUEUE):
        self._running = True
        self.consumer_queue = consumer_queue
        self.monitoring_queue = monitoring_queue
        self.rabbitmq = RabbitMQClient()
        self.target_batch = TARGET_BATCH
        self.expected_producers = EXPECTED_PRODUCERS

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
        
        # Log the recovered counter value
        try:
            counter_value = self.data_persistance.get_counter_value()
            logging.info(f"Recovered message counter: {counter_value}")
        except Exception as e:
            logging.error(f"Error retrieving counter value: {e}")
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
        # Initialize sentinel beacon - needs to run in a separate thread
        self.sentinel_beacon = SentinelBeacon(SENTINEL_PORT, "Consumer Worker")
        
        logging.info(f"Consumer Worker initialized to consume from queue '{consumer_queue}'")
    
    def run(self):
        """Run the worker, connecting to RabbitMQ and consuming messages"""
        try:
            # Connect to RabbitMQ
            if not self._setup_rabbitmq():
                logging.error("Failed to set up RabbitMQ connection. Exiting.")
                return False
            
            logging.info(f"Consumer Worker running and consuming from queue '{self.consumer_queue}'")
            
            # Process any pending client data from previous runs
            if hasattr(self, 'clients_to_process') and self.clients_to_process:
                logging.info(f"Processing {len(self.clients_to_process)} clients with pending data")
                for client_id in self.clients_to_process:
                    try:
                        self._write_to_file(client_id)
                    except Exception as e:
                        logging.error(f"Error processing pending data for client {client_id}: {e}")
            
            # Start consuming messages (blocking call)
            while self._running:
                try:
                    self.rabbitmq.start_consuming()
                except Exception as e:
                    logging.error(f"Error in consuming messages: {e}")
                    if self._running:
                        logging.info("Attempting to reconnect in 5 seconds...")
                        time.sleep(5)
                        self._setup_rabbitmq()
            
            return True
        finally:
            # Always clean up resources
            self.cleanup()
    
    def cleanup(self):
        """Clean up resources properly"""
        logging.info("Cleaning up resources...")
        
        # Stop consuming messages
        try:
            if hasattr(self, 'rabbitmq'):
                self.rabbitmq.stop_consuming()
        except Exception as e:
            logging.error(f"Error stopping consumer: {e}")
        
        # Close RabbitMQ connection
        try:
            if hasattr(self, 'rabbitmq'):
                self.rabbitmq.close()
                logging.info("RabbitMQ connection closed")
        except Exception as e:
            logging.error(f"Error closing RabbitMQ connection: {e}")
    
    def _setup_rabbitmq(self, retry_count=1):
        """Set up RabbitMQ connection and consumer"""
        # Connect to RabbitMQ
        connected = self.rabbitmq.connect()
        if not connected:
            logging.error(f"Failed to connect to RabbitMQ, retrying in {retry_count} seconds...")
            time.sleep(min(30, 2 ** retry_count))
            return self._setup_rabbitmq(retry_count + 1)
        
        # Declare the consumer queue
        queue_declared = self.rabbitmq.declare_queue(self.consumer_queue, durable=True)
        if not queue_declared:
            logging.error(f"Failed to declare queue '{self.consumer_queue}'")
            return False
        
        # Declare the monitoring queue for forwarding messages
        monitoring_queue_declared = self.rabbitmq.declare_queue(self.monitoring_queue, durable=True)
        if not monitoring_queue_declared:
            logging.error(f"Failed to declare monitoring queue '{self.monitoring_queue}'")
            return False
        
        # Set up consumer - EXPLICITLY SET PREFETCH
        success = self.rabbitmq.consume(
            queue_name=self.consumer_queue,
            callback=self._process_message,
            no_ack=False,
            prefetch_count=1  # EXPLICITLY SET TO 1
        )
        if not success:
            logging.error(f"Failed to set up consumer for queue '{self.consumer_queue}'")
            return False
            
        return True
    
    def _process_message(self, channel, method, properties, body):
        """Process a message from RabbitMQ"""
        try:
            # Deserialize the message
            deserialized_message = Serializer.deserialize(body)
            
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
            
            # First check if the message is already processed (using WAL's API)
            if self.data_persistance.is_message_processed(client_id, node_id, message_id):
                logging.info(f"\033[33mMessage {message_id} from node {node_id} already processed, acknowledging AND incrementing counter\033[0m")
                self.data_persistance.increment_counter()  # Increment counter even for duplicates
                channel.basic_ack(delivery_tag=method.delivery_tag)  # Acknowledge the message
                return

            # Forward message to monitoring queue for observation in RabbitMQ GUI
            monitoring_message = {
                "original_message": deserialized_message,
                "received_timestamp": time.time(),
                "processed_by": "consumer_worker",
                "message_id": self.data_persistance.get_counter_value(),
            }
            
            # Publish to monitoring queue
            forwarding_success = self.rabbitmq.publish_to_queue(
                queue_name=self.monitoring_queue,
                message=Serializer.serialize(monitoring_message),
                persistent=True
            )
            
            if forwarding_success:
                logging.info(f"Forwarded message {message_id} to monitoring queue")
            else:
                logging.warning(f"Failed to forward message {message_id} to monitoring queue")
            
            # TEST POINT 1. Right before persisting to WAL, we can log the message
            logging.info(f"TEST POINT 1: Preparing to persist message {message_id} from node {node_id} to WAL. Sleep for 3 seconds")
            time.sleep(3)
            
            # Try to persist the message - this might raise exceptions or return False in different scenarios
            try:
                persist_result = self.data_persistance.persist(client_id, node_id, deserialized_message, message_id)
                # TEST POINT 2. After successful persistence, we can log the message
                logging.info(f"TEST POINT 2: Successfully persisted message {message_id} from node {node_id} to WAL and right before incrementing the counter. Sleep for 3 seconds")
                time.sleep(3)
                
                if persist_result:
                    # Message was newly persisted, increment the counter
                    try:
                        # Increment the counter after successful persistence
                        self.data_persistance.increment_counter()
                        # TEST POINT 3. After incrementing the counter, we can log the new value
                        logging.info(f"TEST POINT 3: Incremented message counter for client {client_id}. Sleep for 3 seconds")
                        time.sleep(3)
                        current_count = self.data_persistance.get_counter_value()
                        logging.info(f"Message counter incremented to {current_count}")
                    except Exception as e:
                        logging.error(f"Error incrementing counter: {e}")
                    
                    if message_id == self.target_batch:
                        logging.info(f"Received target batch {self.target_batch}, processing data")
                        self._write_to_file(client_id)
                    
                    channel.basic_ack(delivery_tag=method.delivery_tag)  # Acknowledge the message
                else:
                    # If persist returns False but didn't raise an exception, the message was:
                    # - Either already processed (which we already checked above with is_message_processed)
                    # - Or failed persistence for a non-fatal reason
                    logging.warning(f"Message {message_id} from node {node_id} was not persisted because it is a duplicated message, so we acknowledge it anyway")
                    channel.basic_ack(delivery_tag=method.delivery_tag)  # Acknowledge anyway
                
            except RuntimeError as e:
                # WAL persistence failed with a runtime error
                logging.error(f"Failed to persist message {message_id} to WAL for node {node_id}: {e}")
                channel.basic_reject(delivery_tag=method.delivery_tag, requeue=True)  # Requeue the message
            
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            # Reject the message and requeue it
            channel.basic_reject(delivery_tag=method.delivery_tag, requeue=True)
    
    def _write_to_file(self, client_id):
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
                    
                    # Get current counter value
                    try:
                        message_counter = self.data_persistance.get_counter_value()
                    except Exception as e:
                        logging.error(f"Error getting counter value: {e}")
                        message_counter = "Unknown"
                    
                    # Create a formatted summary text
                    summary_text = f"PROCESSING SUMMARY\n"
                    summary_text += f"==================\n"
                    summary_text += f"Total value accumulated: {total}\n"
                    summary_text += f"Number of batches processed: {count}\n"
                    summary_text += f"Messages processed (counter): {message_counter}\n"
                    summary_text += f"Minimum expected messages: {min_expected_messages}\n"
                    summary_text += f"Status: {'✓ COMPLETE' if should_clear else '⏳ WAITING FOR MORE DATA'}\n"
                    
                    if should_clear:
                        summary_text += f"Expected total for {count} batches: {count}\n"
                        summary_text += f"Math check: {'✓ CORRECT' if total == count else '✗ MISMATCH'}\n"
                    
                    # Write to file
                    self._write_to_file_sync(summary_text)
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
        
        # Stop consuming messages
        try:
            self.rabbitmq.stop_consuming()
        except Exception as e:
            logging.error(f"Error stopping consumer: {e}")
        
        # Shut down the sentinel beacon
        if hasattr(self, 'sentinel_beacon'):
            self.sentinel_beacon.shutdown()

if __name__ == "__main__":
    worker = ConsumerWorker()
    worker.run()
