import logging
import signal
import os
import time
from rabbitmq.Rabbitmq_client import RabbitMQClient
from common.Serializer import Serializer
from dotenv import load_dotenv
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
NODE_ID = os.getenv("NODE_ID") 

# Output queues and exchange
ROUTER_PRODUCER_QUEUE = os.getenv("ROUTER_PRODUCER_QUEUE")
EXCHANGE_NAME_PRODUCER = os.getenv("PRODUCER_EXCHANGE", "filtered_by_country_exchange")
EXCHANGE_TYPE_PRODUCER = os.getenv("PRODUCER_EXCHANGE_TYPE", "direct")

ROUTER_CONSUME_QUEUE = os.getenv("ROUTER_CONSUME_QUEUE")

class Worker:
    def __init__(self, 
                 consumer_queue_name=ROUTER_CONSUME_QUEUE, 
                 exchange_name_producer=EXCHANGE_NAME_PRODUCER, 
                 exchange_type_producer=EXCHANGE_TYPE_PRODUCER, 
                 producer_queue_names=[ROUTER_PRODUCER_QUEUE]):

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
          service_name="count_worker",
          base_dir="/app/persistence"
        )
        
        # WAL metadata tracking
        self.node_id = NODE_ID
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
        logging.info(f"Count Worker initialized with NODE_ID: {self.node_id}")
        logging.info(f"Consumer queue: '{consumer_queue_name}', producer queues: '{producer_queue_names}'")
        logging.info(f"Exchange producer: '{exchange_name_producer}', type: '{exchange_type_producer}'")

    def run(self):
        """Run the worker, connecting to RabbitMQ and consuming messages"""
        # Connect to RabbitMQ
        if not self._setup_rabbitmq():
            logging.error(f"Failed to set up RabbitMQ connection. Exiting.")
            return False
        
        logging.info(f"Worker running and consuming from queue '{self.consumer_queue_name}'")
        
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
        """Process a message"""
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
            new_operation_id = self.data_persistence.get_counter_value()
            node_id = deserialized_message.get("node_id")

            if self.data_persistence.is_message_processed(client_id, node_id, operation_id):
                logging.info(f"Message {operation_id} from node {node_id} already processed for client {client_id}")
                self.data_persistence.increment_counter()
                channel.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            if disconnect_marker:
                # Get message ID for this operation
                self.send_data(client_id, data, False, disconnect_marker=True, operation_id=new_operation_id)
                self.data_persistence.clear(client_id)

            elif eof_marker:
                logging.info(f"EOF marker received for client_id '{client_id}'")
                self.send_data(client_id, data, True, query, operation_id=new_operation_id)
                self.data_persistence.clear(client_id)

            elif data:
                participation_counts = {}

                for actor in data:
                  actor_name = actor.get("name")
                  if not actor_name in participation_counts:
                      participation_counts[actor_name] = 0
                  participation_counts[actor_name] += 1

                parsed_data = self._parse_data(participation_counts)

                self.send_data(client_id, parsed_data, False, query, operation_id=new_operation_id)
                self.data_persistence.persist(client_id, node_id, {}, operation_id)
            else:
                logging.warning(f"No data in message: {deserialized_message}")

            self.data_persistence.increment_counter()
            channel.basic_ack(delivery_tag=method.delivery_tag)

        except ValueError as ve:
            if "was previously cleared, cannot recreate directory" in str(ve):
                channel.basic_ack(delivery_tag=method.delivery_tag)
            else:
                logging.error(f"ValueError processing message: {ve}")
                channel.basic_reject(delivery_tag=method.delivery_tag, requeue=True)        
                    
        except Exception as e:
            logging.error(f"Failed to deserialize message: {e}")
            channel.basic_reject(delivery_tag=method.delivery_tag, requeue=True)
            return
    
    def _parse_data(self, data):
        """
        Parse the data to extract the average movies by rating
        """
        parsed_data = []
        for actor, count in data.items():
            parsed_data.append({"name": actor, "count": count})
        return parsed_data

    def send_data(self, client_id, data, eof_marker=False, query=None, disconnect_marker=False, operation_id=None):
        """Send data to the router queue with query in metadata and WAL metadata"""

        message = Serializer.add_metadata(client_id, data, eof_marker, query, disconnect_marker, operation_id, self.node_id)        
        
        success = self.rabbitmq.publish(
            exchange_name=self.exchange_name_producer,
            routing_key=self.producer_queue_names[0],
            message=Serializer.serialize(message),
            persistent=True
        )
        if not success:
            logging.error(f"Failed to send data with query '{query}' to router queue")
        else:
            logging.debug(f"Sent message to router queue for client {client_id} with node_id {self.node_id}, message_id {operation_id}")

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
