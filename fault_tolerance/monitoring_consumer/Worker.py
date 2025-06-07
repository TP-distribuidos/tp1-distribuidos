import logging
import os
import signal
import time

from rabbitmq.Rabbitmq_client import RabbitMQClient
from common.Serializer import Serializer
from common.SentinelBeacon import SentinelBeacon
from ConsumerStateInterpreter import ConsumerStateInterpreter  # Reuse or create similar interpreter
from common.data_persistance.WriteAheadLog import WriteAheadLog
from common.data_persistance.FileSystemStorage import FileSystemStorage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)

# Environment variables
MONITORING_QUEUE = os.getenv("MONITORING_QUEUE", "monitoring_queue")
SENTINEL_PORT = int(os.getenv("SENTINEL_PORT", 9003))  # Different port
OUTPUT_FILE = os.getenv("OUTPUT_FILE", "/app/output/monitoring_summary.txt")

class MonitoringConsumer:
    def __init__(self, monitoring_queue=MONITORING_QUEUE):
        self._running = True
        self.monitoring_queue = monitoring_queue
        self.rabbitmq = RabbitMQClient()
        
        # Create output directory
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        
        # Initialize WAL with a different service name
        self.data_persistance = WriteAheadLog(
            state_interpreter=ConsumerStateInterpreter(),
            storage=FileSystemStorage(),
            service_name="monitoring_consumer",
            base_dir="/app/persistence/monitoring"  # Different directory
        )
        
        # Initialize sentinel beacon
        self.sentinel_beacon = SentinelBeacon(SENTINEL_PORT, "Monitoring Consumer")

        # Add expected producers/messages
        self.expected_producers = int(os.getenv("EXPECTED_PRODUCERS", 3))
        self.target_batch = 7
        self.expected_messages = self.expected_producers * self.target_batch
        
        logging.info(f"Monitoring Consumer initialized, expecting {self.expected_messages} total messages")

    def run(self):
        """Run the monitoring consumer"""
        try:
            # Connect to RabbitMQ
            if not self._setup_rabbitmq():
                logging.error("Failed to set up RabbitMQ connection. Exiting.")
                return False
            
            logging.info(f"Monitoring Consumer running and consuming from queue '{self.monitoring_queue}'")
            
            # Start consuming messages
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
            self.cleanup()
    
    def _setup_rabbitmq(self, retry_count=1):
        """Set up RabbitMQ connection and consumer"""
        # Connect to RabbitMQ
        connected = self.rabbitmq.connect()
        if not connected:
            logging.error(f"Failed to connect to RabbitMQ, retrying in {retry_count} seconds...")
            time.sleep(min(30, 2 ** retry_count))
            return self._setup_rabbitmq(retry_count + 1)
        
        # Declare the monitoring queue
        queue_declared = self.rabbitmq.declare_queue(self.monitoring_queue, durable=True)
        if not queue_declared:
            logging.error(f"Failed to declare queue '{self.monitoring_queue}'")
            return False
        
        # Set up consumer with prefetch=1
        success = self.rabbitmq.consume(
            queue_name=self.monitoring_queue,
            callback=self._process_message,
            no_ack=False,
            prefetch_count=1  # Ensure one message at a time
        )
        if not success:
            logging.error(f"Failed to set up consumer for queue '{self.monitoring_queue}'")
            return False
            
        return True
    
    def _process_message(self, channel, method, properties, body):
        """Process a message from the monitoring queue"""
        try:
            # Deserialize the message
            monitoring_message = Serializer.deserialize(body)
            
            # Extract the original message and metadata
            original_message = monitoring_message.get('original_message')
            message_id = monitoring_message.get('message_id', None)
            
            if not original_message or message_id is None:
                logging.error(f"Invalid monitoring message structure: {monitoring_message}")
                channel.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            # Extract details from original message
            batch = original_message.get('batch')
            node_id = original_message.get('node_id')
            
            logging.info(f"Processing monitoring message - ID: {message_id}, Node: {node_id}, Batch: {batch}")
            
            client_id = "monitoring_client"
            
            # Check if already processed
            if self.data_persistance.is_message_processed(client_id, node_id, message_id):
                logging.info(f"Monitoring message {message_id} already processed, acknowledging")
                # Still increment counter for duplicates as in our main consumer
                self.data_persistance.increment_counter()
                channel.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            # Try to persist the message
            try:
                persist_result = self.data_persistance.persist(client_id, node_id, original_message, message_id)
                
                if persist_result:
                    # Increment counter
                    self.data_persistance.increment_counter()
                    current_count = self.data_persistance.get_counter_value()
                    logging.info(f"Incremented counter to {current_count} for monitoring message {message_id}")
                    
                    # Check if we should update the summary - ONLY write summary when we have all expected batches
                    if batch == 7:  # When we see a batch 7, check if we have enough data
                        data = self.data_persistance.retrieve(client_id)
                        if data and isinstance(data, dict) and "count" in data:
                            count = data.get("count", 0) 
                            expected_count = self.expected_producers * self.target_batch
                            
                            if count >= expected_count:
                                logging.info(f"All {expected_count} messages received, writing final summary")
                                self._write_summary(client_id)
                            else:
                                logging.info(f"Batch 7 received but only {count}/{expected_count} messages so far, waiting for more")
                    
                    channel.basic_ack(delivery_tag=method.delivery_tag)
                else:
                    logging.warning(f"Monitoring message {message_id} not persisted, acknowledging anyway")
                    channel.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as e:
                logging.error(f"Error persisting monitoring message: {e}")
                channel.basic_reject(delivery_tag=method.delivery_tag, requeue=True)
                
        except Exception as e:
            logging.error(f"Error processing monitoring message: {e}")
            channel.basic_reject(delivery_tag=method.delivery_tag, requeue=True)
    
    def _write_summary(self, client_id):
        """Write summary of all processed monitoring messages"""
        try:
            # Retrieve consolidated data
            data = self.data_persistance.retrieve(client_id)
            if not data:
                logging.info(f"No monitoring data to write")
                return
            
            # Similar to main consumer, extract totals  
            if isinstance(data, dict) and "total" in data and "count" in data:
                total = data.get("total", 0)
                count = data.get("count", 0)
                
                # Get counter value
                counter = self.data_persistance.get_counter_value()
                
                # Create a formatted summary
                summary = f"MONITORING CONSUMER SUMMARY\n"
                summary += f"==========================\n"
                summary += f"Total monitoring messages processed: {count}\n"
                summary += f"Total value accumulated: {total}\n"
                summary += f"Internal counter value: {counter}\n"
                summary += f"Validation: {'PASSED' if total == count else 'FAILED'}\n"
                
                # Write to file
                with open(OUTPUT_FILE, 'w') as f:
                    f.write(summary)
                    f.flush()
                    os.fsync(f.fileno())
                
                logging.info(f"Updated monitoring summary: messages={count}, total={total}, counter={counter}")
        except Exception as e:
            logging.error(f"Error writing monitoring summary: {e}")
    
    def cleanup(self):
        """Clean up resources"""
        logging.info("Cleaning up monitoring consumer resources...")
        if hasattr(self, 'rabbitmq'):
            try:
                self.rabbitmq.stop_consuming()
                self.rabbitmq.close()
            except Exception as e:
                logging.error(f"Error closing RabbitMQ connection: {e}")
    
    def _handle_shutdown(self, *_):
        """Handle shutdown signals"""
        logging.info("Shutting down monitoring consumer...")
        self._running = False
        
        if hasattr(self, 'sentinel_beacon'):
            self.sentinel_beacon.shutdown()

if __name__ == "__main__":
    consumer = MonitoringConsumer()
    consumer.run()
