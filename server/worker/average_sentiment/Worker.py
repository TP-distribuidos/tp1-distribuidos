import logging
import signal
import os
import time
from rabbitmq.Rabbitmq_client import RabbitMQClient
from common.Serializer import Serializer
from dotenv import load_dotenv
from common.SentinelBeacon import SentinelBeacon
from StateInterpreter import StateInterpreter
from common.data_persistance.WriteAheadLog import WriteAheadLog
from common.data_persistance.FileSystemStorage import FileSystemStorage


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)

# Load environment variables
load_dotenv()

logging.getLogger("pika").setLevel(logging.ERROR)

# Constants and configuration
NODE_ID = os.getenv("NODE_ID")
ROUTER_CONSUME_QUEUE = os.getenv("ROUTER_CONSUME_QUEUE", "average_sentiment_worker")
COLLECTOR_QUEUE = os.getenv("COLLECTOR_QUEUE", "average_sentiment_collector_router")
EXCHANGE_NAME_PRODUCER = os.getenv("PRODUCER_EXCHANGE", "average_sentiment_exchange")
EXCHANGE_TYPE_PRODUCER = os.getenv("PRODUCER_EXCHANGE_TYPE", "direct")
QUERY_5 = os.getenv("QUERY_5", "5")
SENTINEL_PORT = int(os.getenv("SENTINEL_PORT", "5000"))

class Worker:
    def __init__(self, 
                 consumer_queue_name=ROUTER_CONSUME_QUEUE, 
                 exchange_name_producer=EXCHANGE_NAME_PRODUCER,
                 exchange_type_producer=EXCHANGE_TYPE_PRODUCER,
                 producer_queue_names=[COLLECTOR_QUEUE]):
        self._running = True
        self.consumer_queue_name = consumer_queue_name
        self.producer_queue_names = producer_queue_names
        self.exchange_name_producer = exchange_name_producer
        self.exchange_type_producer = exchange_type_producer
        self.rabbitmq = RabbitMQClient()
        
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
        
        # Message counter for incremental IDs
        self.message_counter = 0
        
        # Store the node ID for message identification
        self.node_id = NODE_ID
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
        logging.info(f"Average Sentiment Worker initialized for queue '{consumer_queue_name}', producer queues '{producer_queue_names}'")
        logging.info(f"Exchange producer: '{exchange_name_producer}', type: '{exchange_type_producer}'")
    
    def run(self):
        """Run the worker, connecting to RabbitMQ and consuming messages"""
        # Connect to RabbitMQ
        if not self._setup_rabbitmq():
            logging.error(f"Failed to set up RabbitMQ connection. Exiting.")
            return False
        
        
        # Start consuming messages (blocking call)
        try:
            self.rabbitmq.start_consuming()
        except KeyboardInterrupt:
            self._handle_shutdown()
        
        return True
    
    def _setup_rabbitmq(self, retry_count=1):
        """Set up RabbitMQ connection and consumer"""
        # Connect to RabbitMQ
        connected = self.rabbitmq.connect()
        if not connected:
            logging.error(f"Failed to connect to RabbitMQ, retrying in {retry_count} seconds...")
            wait_time = min(30, 2 ** retry_count)
            time.sleep(wait_time)
            return self._setup_rabbitmq(retry_count + 1)
        
        # -------------------- CONSUMER --------------------
        # Declare input queue (from router)
        queue = self.rabbitmq.declare_queue(self.consumer_queue_name, durable=True)
        if not queue:
            logging.error(f"Failed to declare consumer queue '{self.consumer_queue_name}'")
            return False
        # --------------------------------------------------

        # -------------------- PRODUCER --------------------
        # Declare exchange
        exchange = self.rabbitmq.declare_exchange(
            name=self.exchange_name_producer,
            exchange_type=self.exchange_type_producer,
            durable=True
        )
        if not exchange:
            logging.error(f"Failed to declare exchange '{self.exchange_name_producer}'")
            return False
        
        # Declare output queues
        for queue_name in self.producer_queue_names:
            queue = self.rabbitmq.declare_queue(queue_name, durable=True)
            if not queue:
                logging.error(f"Failed to declare producer queue '{queue_name}'")
                return False        
            
            # Bind queues to exchange
            success = self.rabbitmq.bind_queue(
                queue_name=queue_name,
                exchange_name=self.exchange_name_producer,
                routing_key=queue_name,
                exchange_type=self.exchange_type_producer
            )
            if not success:
                logging.error(f"Failed to bind queue '{queue_name}' to exchange '{self.exchange_name_producer}'")
                return False
        # --------------------------------------------------
        
        # Set up consumer for the input queue
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
                self.data_persistence.persist(client_id, node_id, data, operation_id)
                channel.basic_ack(delivery_tag=method.delivery_tag)
            else:
                logging.warning(f"Received empty data from client {client_id}")
                channel.basic_ack(delivery_tag=method.delivery_tag)
                
        except ValueError as ve:
            if "was previously cleared, cannot recreate directory" in str(ve):
                channel.basic_ack(delivery_tag=method.delivery_tag)
            else:
                logging.error(f"ValueError processing message: {ve}")
                channel.basic_reject(delivery_tag=method.delivery_tag, requeue=True)        
        
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            channel.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
    
    def send_data(self, client_id, data, eof_marker=False, query=None, disconnect_marker=False, operation_id=None):
        """Send data to the producer exchange with query in metadata"""
        message = Serializer.add_metadata(client_id, data, eof_marker, query, disconnect_marker, operation_id, self.node_id)
        queue_name = self.producer_queue_names[0]
        success = self.rabbitmq.publish(
            exchange_name=self.exchange_name_producer,
            routing_key=queue_name,
            message=Serializer.serialize(message),
            persistent=True,
            exchange_type=self.exchange_type_producer
        )
        if not success:
            logging.error(f"Failed to send data with query '{query}' for client {client_id}")
    
    def _handle_shutdown(self, *_):
        """Handle shutdown signals"""
        if not self._running:
            return
            
        logging.info(f"Shutting down average sentiment worker...")
        self._running = False
        
        # Stop consuming and close connection
        if hasattr(self, 'rabbitmq'):
            self.rabbitmq.stop_consuming()
            self.rabbitmq.close()
        
        # Shut down the sentinel beacon
        if hasattr(self, 'sentinel_beacon'):
            self.sentinel_beacon.shutdown()