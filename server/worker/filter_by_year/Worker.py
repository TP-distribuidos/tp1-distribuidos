import logging
import signal
import os
import time
from rabbitmq.Rabbitmq_client import RabbitMQClient
from common.Serializer import Serializer
from common.SentinelBeacon import SentinelBeacon
from dotenv import load_dotenv
from common.data_persistance.StatelessStateInterpreter import StatelessStateInterpreter
from common.data_persistance.WriteAheadLog import WriteAheadLog
from common.data_persistance.FileSystemStorage import FileSystemStorage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)

logging.getLogger("pika").setLevel(logging.ERROR)

load_dotenv()

# Node identification
NODE_ID = os.getenv("NODE_ID")

ROUTER_CONSUME_QUEUE = os.getenv("ROUTER_CONSUME_QUEUE")
MIN_YEAR = 2000
MAX_YEAR = 2010
RELEASE_DATE = "release_date"

ROUTER_PRODUCER_QUEUE = os.getenv("ROUTER_PRODUCER_QUEUE")
EXCHANGE_NAME_PRODUCER = os.getenv("PRODUCER_EXCHANGE", "filtered_data_exchange")
EXCHANGE_TYPE_PRODUCER = os.getenv("PRODUCER_EXCHANGE_TYPE", "direct")
SENTINEL_PORT = int(os.getenv("SENTINEL_PORT", "5000"))


QUERY_EQ_YEAR = "eq_year"
QUERY_GT_YEAR = "gt_year"

class Worker:
    def __init__(self, 
                 exchange_name_consumer=None, 
                 exchange_type_consumer=None, 
                 consumer_queue_names=[ROUTER_CONSUME_QUEUE], 
                 exchange_name_producer=EXCHANGE_NAME_PRODUCER, 
                 exchange_type_producer=EXCHANGE_TYPE_PRODUCER, 
                 producer_queue_name=ROUTER_PRODUCER_QUEUE):

        self._running = True
        self.consumer_queue_names = consumer_queue_names
        self.producer_queue_name = producer_queue_name
        self.exchange_name_consumer = exchange_name_consumer
        self.exchange_name_producer = exchange_name_producer
        self.exchange_type_consumer = exchange_type_consumer
        self.exchange_type_producer = exchange_type_producer
        self.rabbitmq = RabbitMQClient()

        self.sentinel_beacon = SentinelBeacon(SENTINEL_PORT)
        
        # Add WAL for message deduplication and operation IDs
        self.data_persistence = WriteAheadLog(
            state_interpreter=StatelessStateInterpreter(),
            storage=FileSystemStorage(),
            service_name="filter_by_year_worker",
            base_dir="/app/persistence"
        )
        
        # Store the node ID for message identification
        self.node_id = NODE_ID

        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
        logging.info(f"Worker initialized for consumer queues '{consumer_queue_names}', producer queue '{producer_queue_name}', exchange consumer '{exchange_name_consumer}' and exchange producer '{exchange_name_producer}'")
    
    def run(self):
        """Run the worker, connecting to RabbitMQ and consuming messages"""
        # Connect to RabbitMQ
        if not self._setup_rabbitmq():
            logging.error(f"Failed to set up RabbitMQ connection. Exiting.")
            return False
        
        logging.info(f"Worker running and consuming from queue '{self.consumer_queue_names}'")
        
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
        # Declare queues (idempotent operation)
        for queue_name in self.consumer_queue_names:
            queue = self.rabbitmq.declare_queue(queue_name, durable=True)
            if not queue:
                return False
        # --------------------------------------------------


        # -------------------- PRODUCER --------------------
        # Declare exchange (idempotent operation)
        exchange = self.rabbitmq.declare_exchange(
            name=self.exchange_name_producer,
            exchange_type=self.exchange_type_producer,
            durable=True
        )
        if not exchange:
            logging.error(f"Failed to declare exchange '{self.exchange_name_producer}'")
            return False
        
        # Declare the producer queue (router input queue)
        queue = self.rabbitmq.declare_queue(self.producer_queue_name, durable=True)
        if not queue:
            return False        
        
        # Bind queue to exchange
        success = self.rabbitmq.bind_queue(
            queue_name=self.producer_queue_name,
            exchange_name=self.exchange_name_producer,
            routing_key=self.producer_queue_name
        )
        if not success:
            logging.error(f"Failed to bind queue '{self.producer_queue_name}' to exchange '{self.exchange_name_producer}'")
            return False
        # --------------------------------------------------
        
        # Set up consumers
        for queue_name in self.consumer_queue_names:
            success = self.rabbitmq.consume(
                queue_name=queue_name,
                callback=self._process_message,
                no_ack=False
            )
            if not success:
                logging.error(f"Failed to set up consumer for queue '{queue_name}'")
                return False

        return True
    
    def _process_message(self, channel, method, properties, body):
        """Process a message from the queue"""
        try:
            deserialized_message = Serializer.deserialize(body)
            
            # Extract client_id and data from the deserialized message
            client_id = deserialized_message.get("client_id")
            data = deserialized_message.get("data")
            eof_marker = deserialized_message.get("EOF_MARKER")
            disconnect_marker = deserialized_message.get("DISCONNECT")
            query = deserialized_message.get("query", "")
            operation_id = deserialized_message.get("operation_id")
            node_id = deserialized_message.get("node_id")
            
            # Check if this message was already processed (deduplication)
            if self.data_persistence.is_message_processed(client_id, node_id, operation_id):
                logging.info(f"Message {operation_id} from node {node_id} already processed for client {client_id}")
                self.data_persistence.increment_counter()
                channel.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            # Get a new operation ID for outgoing messages
            new_operation_id = self.data_persistence.get_counter_value()

            if disconnect_marker:
                # Propagate DISCONNECT to downstream components
                self.send_disconnect(client_id, query)
                self.data_persistence.clear(client_id)
                self.data_persistence.increment_counter()
                channel.basic_ack(delivery_tag=method.delivery_tag)
                return

            if eof_marker:
                # Generate a new operation ID for this EOF message
                self.send_data(client_id, data, QUERY_GT_YEAR, True, new_operation_id)
                # Mark this message as processed
                self.data_persistence.persist(client_id, node_id, {}, operation_id)
                self.data_persistence.increment_counter()
                channel.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            # Process the movie data
            if data:
                data_eq_year, data_gt_year = self._filter_data(data)
                if data_eq_year:
                    self.send_data(client_id, data_eq_year, QUERY_EQ_YEAR, operation_id=new_operation_id)
                if data_gt_year:
                    self.send_data(client_id, data_gt_year, QUERY_GT_YEAR, operation_id=new_operation_id)
            
            # Mark this message as processed in WAL
            self.data_persistence.persist(client_id, node_id, {}, operation_id)
            self.data_persistence.increment_counter()
            
            # Acknowledge message
            channel.basic_ack(delivery_tag=method.delivery_tag)

        except ValueError as ve:
            if "was previously cleared, cannot recreate directory" in str(ve):
                channel.basic_ack(delivery_tag=method.delivery_tag)
            else:
                logging.error(f"ValueError processing message: {ve}")
                channel.basic_reject(delivery_tag=method.delivery_tag, requeue=True) 
                            
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            # Reject the message and requeue it
            channel.basic_reject(delivery_tag=method.delivery_tag, requeue=True)

    def send_disconnect(self, client_id, query=""):
        """Send DISCONNECT notification to downstream components"""
        # Generate an operation ID for this message
        operation_id = self.data_persistence.get_counter_value()
        self.data_persistence.increment_counter()
        
        message = Serializer.add_metadata(client_id, {}, False, query, True, operation_id, self.node_id)
        success = self.rabbitmq.publish(
            exchange_name=self.exchange_name_producer,
            routing_key=self.producer_queue_name,
            message=Serializer.serialize(message),
            persistent=True
        )
        if not success:
            logging.error(f"Failed to send disconnect notification to queue {self.producer_queue_name}")

    def send_data(self, client_id, data, query, eof_marker=False, operation_id=None):
        """Send data to the router queue with query type in metadata"""
        if operation_id is None:
            operation_id = self.data_persistence.get_counter_value()
            self.data_persistence.increment_counter()
            
        message = Serializer.add_metadata(client_id, data, eof_marker, query, False, operation_id, self.node_id)
        success = self.rabbitmq.publish(
            exchange_name=self.exchange_name_producer,
            routing_key=self.producer_queue_name,
            message=Serializer.serialize(message),
            persistent=True
        )
        if not success:
            logging.error(f"Failed to send data with query type '{query}' to router queue")

    def _filter_data(self, data):
        """Filter data into two lists based on the year"""
        data_eq_year, data_gt_year = [], []
        
        for record in data:
            try:
                release_date = str(record.get(RELEASE_DATE, ''))
                if not release_date:
                    continue
                year_part = release_date.split("-")[0]
                if not year_part:
                    continue
                    
                year = int(year_part)
                
                del record[RELEASE_DATE]
                
                if self._query1(year):
                    data_eq_year.append(record)
                elif self._query2(year):
                    data_gt_year.append(record)
                    
            except (ValueError, IndexError, AttributeError) as e:
                logging.error(f"Error processing record {record}: {e}")
                continue
            
        return data_eq_year, data_gt_year
    
    def _query1(self, year):
        """Check if the year is equal to the specified year"""
        return MIN_YEAR <= year and year < MAX_YEAR
    
    def _query2(self, year):
        """Check if the year is greater than the specified year"""
        return year > MIN_YEAR
        
    def _handle_shutdown(self, *_):
        """Handle shutdown signals"""
        if not self._running:
            return
            
        logging.info(f"Shutting down worker...")
        self._running = False
        
        # Close RabbitMQ connection
        if hasattr(self, 'rabbitmq'):
            self.rabbitmq.stop_consuming()
            self.rabbitmq.close()
            
        # Shut down the sentinel beacon
        if hasattr(self, 'sentinel_beacon'):
            self.sentinel_beacon.shutdown()