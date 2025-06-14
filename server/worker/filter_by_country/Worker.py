import logging
import signal
import os
import time
from rabbitmq.Rabbitmq_client import RabbitMQClient
from common.Serializer import Serializer
from dotenv import load_dotenv
import ast
from common.SentinelBeacon import SentinelBeacon
from common.data_persistance.StatelessStateInterpreter import StatelessStateInterpreter
from common.data_persistance.WriteAheadLog import WriteAheadLog
from common.data_persistance.FileSystemStorage import FileSystemStorage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)

logging.getLogger("pika").setLevel(logging.ERROR)

# Load environment variables
load_dotenv()

SENTINEL_PORT = int(os.getenv("SENTINEL_PORT", "5000"))

# Node identification
NODE_ID = os.getenv("NODE_ID")

# Constants for query types - these match what the previous worker outputs
QUERY_EQ_YEAR = "eq_year"
QUERY_GT_YEAR = "gt_year"
QUERY_1 = os.getenv("QUERY_1", "1")

# Constants for data processing
PRODUCTION_COUNTRIES = "production_countries"
ISO_3166_1 = "iso_3166_1"
ONE_COUNTRY = "AR"
N_COUNTRIES = ["AR", "ES"]

# Output queues and exchange
ROUTER_PRODUCER_QUEUE = os.getenv("ROUTER_PRODUCER_QUEUE")
RESPONSE_QUEUE = os.getenv("RESPONSE_QUEUE", "response_queue")
EXCHANGE_NAME_PRODUCER = os.getenv("PRODUCER_EXCHANGE", "filtered_by_country_exchange")
EXCHANGE_TYPE_PRODUCER = os.getenv("PRODUCER_EXCHANGE_TYPE", "direct")

ROUTER_CONSUME_QUEUE = os.getenv("ROUTER_CONSUME_QUEUE")

class Worker:
    def __init__(self, 
                 consumer_queue_name=ROUTER_CONSUME_QUEUE, 
                 exchange_name_producer=EXCHANGE_NAME_PRODUCER, 
                 exchange_type_producer=EXCHANGE_TYPE_PRODUCER, 
                 producer_queue_names=[ROUTER_PRODUCER_QUEUE, RESPONSE_QUEUE]):

        self._running = True
        self.consumer_queue_name = consumer_queue_name
        self.producer_queue_names = producer_queue_names
        self.exchange_name_producer = exchange_name_producer
        self.exchange_type_producer = exchange_type_producer
        self.rabbitmq = RabbitMQClient()
        
        self.sentinel_beacon = SentinelBeacon(SENTINEL_PORT)
        
        self.data_persistence = WriteAheadLog(
            state_interpreter=StatelessStateInterpreter(),
            storage=FileSystemStorage(),
            service_name="filter_by_country_worker",
            base_dir="/app/persistence"
        )
        
        # Store the node ID for message identification
        self.node_id = NODE_ID
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
        logging.info(f"Worker initialized for consumer queue '{consumer_queue_name}', producer queues '{producer_queue_names}'")
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
            logging.info("Keyboard interrupt received")
        except Exception as e:
            logging.error(f"Error in consuming messages: {e}")
            return False
        finally:
            self._cleanup()
            
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
                routing_key=queue_name
            )
            if not success:
                logging.error(f"Failed to bind queue '{queue_name}' to exchange '{self.exchange_name_producer}'")
                return False
        # --------------------------------------------------
        
        # Set up consumer for the input queue
        success = self.rabbitmq.consume(
            queue_name=self.consumer_queue_name,
            callback=self._process_message,
            no_ack=False
        )
        
        if not success:
            logging.error(f"Failed to set up consumer for queue '{self.consumer_queue_name}'")
            return False

        return True
    
    def _process_message(self, channel, method, properties, body):
        """Process a message based on its query type"""
        try:
            # Deserialize the message
            deserialized_message = Serializer.deserialize(body)
            
            # Extract client_id, data, and query from the deserialized message
            client_id = deserialized_message.get("client_id")
            data = deserialized_message.get("data")
            query = deserialized_message.get("query")
            eof_marker = deserialized_message.get("EOF_MARKER")
            disconnect_marker = deserialized_message.get("DISCONNECT")
            operation_id = deserialized_message.get("operation_id")
            node_id = deserialized_message.get("node_id")
            new_operation_id = self.data_persistence.get_counter_value()
            
            
            if disconnect_marker:
                logging.info(f"\033[91mDisconnect marker received for client_id '{client_id}'\033[0m")
                # Propagate DISCONNECT to all downstream components
                self.send_disconnect(client_id, query, new_operation_id)
                self.data_persistence.clear(client_id)
                self.data_persistence.increment_counter()
                channel.basic_ack(delivery_tag=method.delivery_tag)
                return

            if self.data_persistence.is_message_processed(client_id, node_id, operation_id):
                logging.info(f"Message {operation_id} from node {node_id} already processed for client {client_id}")
                self.data_persistence.increment_counter()
                channel.basic_ack(delivery_tag=method.delivery_tag)
                return
                
            if eof_marker:
                # Generate a new operation ID for this EOF message
                self.send_eq_one_country(client_id, data, self.producer_queue_names[0], True, new_operation_id)
                self.data_persistence.increment_counter()
                channel.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            if not data:
                logging.warning(f"Received message with no data, client ID: {client_id}")
                self.data_persistence.increment_counter()
                channel.basic_ack(delivery_tag=method.delivery_tag)
                return
                
            # Regular message processing...
            if query == QUERY_EQ_YEAR:
                data_eq_one_country, data_response_queue = self._filter_data(data)
                if data_eq_one_country:
                    projected_data = self._project_to_columns(data_eq_one_country)
                    self.send_eq_one_country(client_id, projected_data, self.producer_queue_names[0], operation_id=new_operation_id)
                if data_response_queue:
                    self.send_response_queue(client_id, data_response_queue, self.producer_queue_names[1], operation_id=new_operation_id)
                    
            elif query == QUERY_GT_YEAR:
                data_eq_one_country, _ = self._filter_data(data)
                if data_eq_one_country:
                    projected_data = self._project_to_columns(data_eq_one_country)
                    self.send_eq_one_country(client_id, projected_data, self.producer_queue_names[0], operation_id=new_operation_id)
                    
            else:
                logging.warning(f"Unknown query type: {query}, client ID: {client_id}")
            
            # Persist the fact that we processed this message
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
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    def send_disconnect(self, client_id, query=None, operation_id=None):
        """Send DISCONNECT notification to all downstream components"""
        # Generate an operation ID for this message
        
        # Send to primary output queue (for next stage processing)
        message = Serializer.add_metadata(client_id, None, False, query, True, operation_id, self.node_id)
        success = self.rabbitmq.publish(
            exchange_name=self.exchange_name_producer,
            routing_key=self.producer_queue_names[0],
            message=Serializer.serialize(message),
            persistent=True
        )
        if not success:
            logging.error(f"Failed to send disconnect notification to queue {self.producer_queue_names[0]}")

    def _project_to_columns(self, data):
        """Project the data to only include the 'id' column"""
        if not data:
            return []

        projected_data = []
        for record in data:
            if 'id' in record:
                projected_data.append({
                    "id": record.get("id", ""),
                    "name": record.get("original_title", ""),
                })

        return projected_data

    def send_eq_one_country(self, client_id, data, queue_name=ROUTER_PRODUCER_QUEUE, eof_marker=False, operation_id=None):
        """Send data to the eq_one_country queue in our exchange"""
            
        message = Serializer.add_metadata(client_id, data, eof_marker, None, False, operation_id, self.node_id)
        success = self.rabbitmq.publish(
            exchange_name=self.exchange_name_producer,
            routing_key=queue_name,
            message=Serializer.serialize(message),
            persistent=True
        )
        if not success:
            logging.error(f"Failed to send data to eq_one_country queue")

    def send_response_queue(self, client_id, data, queue_name=RESPONSE_QUEUE, query=QUERY_1, operation_id=None):
        """Send data to the response queue in our exchange"""
            
        message = Serializer.add_metadata(client_id, data, False, query, False, operation_id, self.node_id)
        success = self.rabbitmq.publish(
            exchange_name=self.exchange_name_producer,
            routing_key=queue_name,
            message=Serializer.serialize(message),
            persistent=True
        )
        if not success:
            logging.error(f"Failed to send data to response queue")

    def _filter_data(self, data):
        """Filter data into two lists based on the country"""
        data_eq_one_country, data_response_queue = [], []
        
        for record in data:
            countries = (record.pop(PRODUCTION_COUNTRIES, None))
            if countries is None:
                logging.info(f"Record missing '{PRODUCTION_COUNTRIES}' field: {record}")
                continue
            
            if isinstance(countries, str):
                try:
                    countries = ast.literal_eval(countries)
                except (SyntaxError, ValueError):
                    logging.error(f"Failed to parse countries string: {countries}")
                    continue

            if not isinstance(countries, list):
                logging.error(f"Countries is not a list after processing: {countries}")
                continue
                    
            record_copy = record.copy()
            has_one_country = False
            found_countries = set()
            
            for country_obj in countries:
                if not isinstance(country_obj, dict):
                    logging.warning(f"Country object is not a dictionary: {country_obj}")
                    continue
                    
                if ISO_3166_1 in country_obj:
                    country = country_obj.get(ISO_3166_1)
                    if country == ONE_COUNTRY:
                        has_one_country = True
                    if country in N_COUNTRIES:
                        found_countries.add(country)
            
            if has_one_country:
                data_eq_one_country.append(record_copy)
                
            # Only add records that contain ALL countries from N_COUNTRIES
            if found_countries == set(N_COUNTRIES):
                data_response_queue.append(record_copy)
            
        return data_eq_one_country, data_response_queue
        
    def _handle_shutdown(self, *_):
        """Handle shutdown signals"""
        if not self._running:
            return
            
        logging.info(f"Shutting down worker...")
        self._running = False
        
        # Stop consuming and close connection
        if hasattr(self, 'rabbitmq'):
            self.rabbitmq.stop_consuming()
            self.rabbitmq.close()
    
    def _cleanup(self):
        """Clean up resources"""
        try:
            if hasattr(self, 'rabbitmq'):
                self.rabbitmq.close()
                logging.info("RabbitMQ connection closed")
        except Exception as e:
            logging.error(f"Error during cleanup: {e}")