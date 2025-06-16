import logging
import signal
import os
import time
import traceback
from rabbitmq.Rabbitmq_client import RabbitMQClient
from common.Serializer import Serializer
from dotenv import load_dotenv
from common.SentinelBeacon import SentinelBeacon
from StateInterpreter import StateInterpreter
from common.data_persistance.WriteAheadLog import WriteAheadLog
from common.data_persistance.FileSystemStorage import FileSystemStorage

# Load environment variables
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)

logging.getLogger("pika").setLevel(logging.ERROR)

# Constants and configuration
NODE_ID = os.getenv("NODE_ID")
ROUTER_CONSUME_QUEUE = os.getenv("ROUTER_CONSUME_QUEUE", "average_sentiment_worker")
COLLECTOR_QUEUE = os.getenv("COLLECTOR_QUEUE", "average_sentiment_collector_router")
QUERY_5 = os.getenv("QUERY_5", "5")
SENTINEL_PORT = int(os.getenv("SENTINEL_PORT", "5000"))
MAX_RECONNECT_ATTEMPTS = int(os.getenv("MAX_RECONNECT_ATTEMPTS", "10"))

class Worker:
    def __init__(self, consumer_queue_name=ROUTER_CONSUME_QUEUE, producer_queue_name=COLLECTOR_QUEUE):
        self._running = True
        self.consumer_queue_name = consumer_queue_name
        self.producer_queue_name = producer_queue_name
        self.rabbitmq = RabbitMQClient()

        self.max_retries = 5  
        self.retry_delay = 2  
        
        self.sentinel_beacon = SentinelBeacon(SENTINEL_PORT)
        
        # Initialize WAL with StateInterpreter for persistence
        self.data_persistence = WriteAheadLog(
            state_interpreter=StateInterpreter(),
            storage=FileSystemStorage(),
            service_name="average_sentiment_worker",
            base_dir="/app/persistence"
        )

        # Store the node ID for message identification
        self.node_id = NODE_ID
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
        logging.info(f"Average Sentiment Worker initialized for queue '{consumer_queue_name}', producer queue '{self.producer_queue_name}'")
    
    def run(self):
        """Run the worker, connecting to RabbitMQ and consuming messages"""
        reconnect_delay = 1
        attempt = 0
        
        while self._running:
            try:
                # Connect to RabbitMQ
                if not self._setup_rabbitmq():
                    logging.error(f"Failed to set up RabbitMQ connection. Retrying in {reconnect_delay} seconds...")
                    time.sleep(reconnect_delay)
                    reconnect_delay = min(30, reconnect_delay * 2)  # Exponential backoff
                    attempt += 1
                    
                    if attempt >= MAX_RECONNECT_ATTEMPTS:
                        logging.error(f"Exceeded maximum reconnection attempts ({MAX_RECONNECT_ATTEMPTS}). Exiting.")
                        return False
                    
                    continue
                
                # Reset reconnect parameters on successful connection
                reconnect_delay = 1
                attempt = 0
                
                # Start consuming messages (blocking call)
                logging.info("Starting to consume messages...")
                self.rabbitmq.start_consuming()
                
                # If we get here, consuming has stopped for some reason but no exception was raised
                logging.warning("Message consumption stopped unexpectedly. Reconnecting...")
                time.sleep(1)  # Brief pause before reconnection
                
            except KeyboardInterrupt:
                self._handle_shutdown()
                break
                
            except Exception as e:
                if not self._running:  # Don't log errors if we're shutting down
                    break
                    
                logging.error(f"Error in message consumption loop: {e}")
                logging.debug(traceback.format_exc())
                
                # Brief pause before reconnection
                time.sleep(reconnect_delay)
                reconnect_delay = min(30, reconnect_delay * 2)  # Exponential backoff
        
        return True
    
    def _setup_rabbitmq(self, retry_count=1):
        """Set up RabbitMQ connection and consumer"""
        try:
            # Add connection recovery callback if method exists
            if hasattr(self.rabbitmq, 'add_connection_recovery_callback'):
                self.rabbitmq.add_connection_recovery_callback(self._on_connection_recovered)
            
            # Connect to RabbitMQ
            connected = self.rabbitmq.connect()
            if not connected:
                logging.error(f"Failed to connect to RabbitMQ, retrying in {retry_count} seconds...")
                wait_time = min(30, 2 ** retry_count)
                time.sleep(wait_time)
                return self._setup_rabbitmq(retry_count + 1)
            
            # Declare queues (idempotent operation)
            queue = self.rabbitmq.declare_queue(self.consumer_queue_name, durable=True)
            if not queue:
                return False
                
            producer_queue = self.rabbitmq.declare_queue(self.producer_queue_name, durable=True)
            if not producer_queue:
                return False

            # Set up consumer
            success = self.rabbitmq.consume(
                queue_name=self.consumer_queue_name,
                callback=self._process_message,
                no_ack=False,
                prefetch_count=1
            )
            if not success:
                logging.error(f"Failed to set up consumer for queue '{self.consumer_queue_name}'")
                return False

            return True
            
        except Exception as e:
            logging.error(f"Error setting up RabbitMQ: {e}")
            return False
    
    def _on_connection_recovered(self):
        """Called when RabbitMQ connection is recovered"""
        logging.info("RabbitMQ connection recovered, re-establishing consumer...")
        
        try:
            # Re-declare queues
            self.rabbitmq.declare_queue(self.consumer_queue_name, durable=True)
            self.rabbitmq.declare_queue(self.producer_queue_name, durable=True)
            
            # Re-establish consumer
            self.rabbitmq.consume(
                queue_name=self.consumer_queue_name,
                callback=self._process_message,
                no_ack=False,
                prefetch_count=1
            )
        except Exception as e:
            logging.error(f"Error re-establishing connection: {e}")
    
    def _process_message(self, channel, method, properties, body):
        """Process a message from the queue"""
        try:
            deserialized_message = Serializer.deserialize(body)
            
            # Extract client_id and data from the deserialized message
            client_id = deserialized_message.get("clientId", deserialized_message.get("client_id"))
            data = deserialized_message.get("data", [])
            eof_marker = deserialized_message.get("EOF_MARKER", False)
            disconnect_marker = deserialized_message.get("DISCONNECT")
            operation_id = deserialized_message.get("operation_id")
            node_id = deserialized_message.get("node_id")
            new_operation_id = self.data_persistence.get_counter_value()

            if disconnect_marker:
                self.send_data(client_id, data, False, disconnect_marker=True, operation_id=new_operation_id)
                self.data_persistence.clear(client_id)
                self.data_persistence.increment_counter()
                channel.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            if self.data_persistence.is_message_processed(client_id, node_id, operation_id):
                self.data_persistence.increment_counter()
                channel.basic_ack(delivery_tag=method.delivery_tag)
                return

            # Process the data based on message type
            if eof_marker:
                # Retrieve the consolidated sentiment data
                client_sentiment_totals = self.data_persistence.retrieve(client_id)
                
                if client_sentiment_totals:
                    # Create the result with sum and count for each sentiment type
                    result = [{
                        "sentiment": "POSITIVE",
                        "sum": client_sentiment_totals["POSITIVE"]["sum"],
                        "count": client_sentiment_totals["POSITIVE"]["count"]
                    }, {
                        "sentiment": "NEGATIVE", 
                        "sum": client_sentiment_totals["NEGATIVE"]["sum"],
                        "count": client_sentiment_totals["NEGATIVE"]["count"]
                    }]
                    
                    # First: Send the data 
                    self.send_data(client_id, result, False, QUERY_5, operation_id=new_operation_id)
                    
                    self.data_persistence.increment_counter()
                    new_operation_id = self.data_persistence.get_counter_value()
                    
                    # Second: Send message with EOF=True
                    self.send_data(client_id, [], True, QUERY_5, operation_id=new_operation_id)
                    
                    # Clean up client data after sending
                    self.data_persistence.clear(client_id)
                    self.data_persistence.increment_counter()
                    
                else:
                    logging.warning(f"Received EOF for client {client_id} but no data found")
                
                channel.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            # Process the sentiment data 
            elif data:
                # Persist data to WAL (processing logic moved to StateInterpreter)
                self.data_persistence.persist(client_id, self.node_id, data, operation_id)
                channel.basic_ack(delivery_tag=method.delivery_tag)
            else:
                logging.warning(f"Received empty data from client {client_id}")
                channel.basic_ack(delivery_tag=method.delivery_tag)
                
        except ValueError as ve:
            if "was previously cleared, cannot recreate directory" in str(ve):
                channel.basic_ack(delivery_tag=method.delivery_tag)
            else:
                logging.error(f"ValueError processing message: {ve}")
                try:
                    channel.basic_reject(delivery_tag=method.delivery_tag, requeue=True)
                except Exception:
                    logging.error("Failed to reject message - channel may be closed")
        
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            try:
                channel.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
            except Exception:
                logging.error("Failed to reject message - channel may be closed")
    
    def send_data(self, client_id, data, eof_marker=False, query=None, disconnect_marker=False, operation_id=None):
        """Send data to the producer queue with query in metadata with retry logic"""
        message = Serializer.add_metadata(client_id, data, eof_marker, query, disconnect_marker, operation_id, self.node_id)
        serialized_message = Serializer.serialize(message)
        
        # Implement retry logic
        for attempt in range(1, self.max_retries + 1):
            try:
                # Check connection before sending if method exists
                if hasattr(self.rabbitmq, 'is_connected') and not self.rabbitmq.is_connected():
                    logging.warning("RabbitMQ connection lost, attempting to reconnect...")
                    self.rabbitmq.connect()
                    
                    # Re-declare queue before publishing
                    self.rabbitmq.declare_queue(self.producer_queue_name, durable=True)
                
                success = self.rabbitmq.publish_to_queue(
                    queue_name=self.producer_queue_name,
                    message=serialized_message,
                    persistent=True
                )
                
                if success:
                    return True
                
                logging.warning(f"Failed to publish message, attempt {attempt}/{self.max_retries}")
            except Exception as e:
                logging.error(f"Error publishing message (attempt {attempt}/{self.max_retries}): {e}")
            
            # Wait before retrying with exponential backoff
            retry_wait = min(60, self.retry_delay * (2 ** (attempt - 1)))
            time.sleep(retry_wait)
        
        logging.error(f"Failed to send data after {self.max_retries} attempts for client {client_id}")
        return False
    
    def _handle_shutdown(self, *_):
        """Handle shutdown signals"""
        if not self._running:
            return
            
        logging.info(f"Shutting down average sentiment worker...")
        self._running = False
        
        # Stop consuming and close connection
        if hasattr(self, 'rabbitmq'):
            try:
                self.rabbitmq.stop_consuming()
                self.rabbitmq.close()
            except Exception as e:
                logging.error(f"Error during shutdown: {e}")
        
        # Shut down the sentinel beacon
        if hasattr(self, 'sentinel_beacon'):
            try:
                self.sentinel_beacon.shutdown()
            except Exception as e:
                logging.error(f"Error shutting down sentinel beacon: {e}")