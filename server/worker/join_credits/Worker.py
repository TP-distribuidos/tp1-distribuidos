import logging
import signal
import os
import time
from rabbitmq.Rabbitmq_client import RabbitMQClient
from common.Serializer import Serializer
from dotenv import load_dotenv
from common.SentinelBeacon import SentinelBeacon
from common.data_persistance.ClientsStateStateInterpreter import ClientsStateStateInterpreter
from common.data_persistance.MoviesStateInterpreter import MoviesStateInterpreter 
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

SENTINEL_PORT = int(os.getenv("SENTINEL_PORT", "5000"))

# Node identification
NODE_ID = os.getenv("NODE_ID")

# Constants
MOVIES_ROUTER_CONSUME_QUEUE = os.getenv("ROUTER_CONSUME_QUEUE_MOVIES")
CREDITS_ROUTER_CONSUME_QUEUE = os.getenv("ROUTER_CONSUME_QUEUE_CREDITS")
EOF_MARKER = os.getenv("EOF_MARKER", "EOF_MARKER")

# Router configuration
ROUTER_PRODUCER_QUEUE = os.getenv("ROUTER_PRODUCER_QUEUE")
EXCHANGE_NAME_PRODUCER = os.getenv("PRODUCER_EXCHANGE", "filtered_data_exchange")
EXCHANGE_TYPE_PRODUCER = os.getenv("PRODUCER_EXCHANGE_TYPE", "direct")

REQUEUE_DELAY = float(os.getenv("REQUEUE_DELAY", "0.1"))  # seconds

class Worker:
    def __init__(self, 
                 consumer_queue_names=[MOVIES_ROUTER_CONSUME_QUEUE, CREDITS_ROUTER_CONSUME_QUEUE], 
                 exchange_name_producer=EXCHANGE_NAME_PRODUCER, 
                 exchange_type_producer=EXCHANGE_TYPE_PRODUCER, 
                 producer_queue_name=ROUTER_PRODUCER_QUEUE):

        self._running = True
        self.consumer_queue_names = consumer_queue_names
        self.producer_queue_name = producer_queue_name
        self.exchange_name_producer = exchange_name_producer
        self.exchange_type_producer = exchange_type_producer
        self.rabbitmq = RabbitMQClient()
        
        self.sentinel_beacon = SentinelBeacon(SENTINEL_PORT)
        
        # 1. Client states data_persistence - tracks which clients have completed movies processing
        self.client_states_data_persistence = WriteAheadLog(
            state_interpreter=ClientsStateStateInterpreter(),
            storage=FileSystemStorage(),
            service_name="join_credits_worker_client_states",
            base_dir="/app/persistence"
        )
        
        # 2. Movie data data_persistence - stores movie data for joining
        self.movies_data_persistence = WriteAheadLog(
            state_interpreter=MoviesStateInterpreter(),
            storage=FileSystemStorage(),
            service_name="join_credits_worker_movies",
            base_dir="/app/persistence"
        )
        
        # 3. Credits processing data_persistence (for deduplication only)
        self.credits_data_persistence = WriteAheadLog(
            state_interpreter=StatelessStateInterpreter(),
            storage=FileSystemStorage(),
            service_name="join_credits_worker_credits",
            base_dir="/app/persistence"
        )
        
        # For requeue delay to avoid overwhelming the broker
        self.requeue_delay = REQUEUE_DELAY
        
        # Store the node ID for message identification
        self.node_id = NODE_ID

        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
        logging.info(f"Worker initialized for consumer queues '{consumer_queue_names}', producer queue '{producer_queue_name}' and exchange producer '{exchange_name_producer}'")
    
    def run(self):
        """Run the worker, connecting to RabbitMQ and consuming messages"""
        # Connect to RabbitMQ
        if not self._setup_rabbitmq():
            logging.error("Failed to set up RabbitMQ connection. Exiting.")
            return False
        
        
        # Keep the worker running until shutdown is triggered
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
        # Declare all queues (idempotent operation)
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
        
        # Start consuming from both queues simultaneously
        for i, queue_name in enumerate(self.consumer_queue_names):
            callback = self._process_movies_message if i == 0 else self._process_credits_message
            success = self.rabbitmq.consume(
                queue_name=queue_name,
                callback=callback,
                no_ack=False
            )
            if not success:
                logging.error(f"Failed to set up consumer for queue '{queue_name}'")
                return False
            logging.debug(f"Started consuming from {queue_name}")

        return True
    
    def _process_movies_message(self, channel, method, properties, body):
        """Process a message from the movies queue"""
        try:
            deserialized_message = Serializer.deserialize(body)
            client_id = deserialized_message.get("client_id")
            data = deserialized_message.get("data")
            eof_marker = deserialized_message.get("EOF_MARKER")
            disconnect_marker = deserialized_message.get("DISCONNECT")
            operation_id = deserialized_message.get("operation_id")
            node_id = deserialized_message.get("node_id")
            new_operation_id = self.credits_data_persistence.get_counter_value()

            if disconnect_marker:
                self.send_data(client_id, data, False, disconnect_marker=True, operation_id=new_operation_id)
                self.client_states_data_persistence.clear(client_id)
                self.movies_data_persistence.clear(client_id)
                self.credits_data_persistence.clear(client_id)
                self.credits_data_persistence.increment_counter()
                channel.basic_ack(delivery_tag=method.delivery_tag)
                logging.info(f"\033[91mDisconnect marker received for client_id '{client_id}'\033[0m")
                return
            
            if self.movies_data_persistence.is_message_processed(client_id, node_id, operation_id or self.client_states_data_persistence.is_message_processed(client_id, self.node_id, operation_id)):
                logging.info(f"Movie message {operation_id} from node {node_id} already processed for client {client_id}")
                self.credits_data_persistence.increment_counter()
                channel.basic_ack(delivery_tag=method.delivery_tag)
                return
                
            # Handle EOF marker for movies
            elif eof_marker:
                logging.info(f"Received EOF marker for movies from client '{client_id}'")
                # Mark that we've received all movies for this client
                self.client_states_data_persistence.persist(client_id, self.node_id, True, operation_id)
            
            # Process movie data
            elif data:
                # Store movie data in WAL
                self.movies_data_persistence.persist(client_id, node_id, data, operation_id)
            
            channel.basic_ack(delivery_tag=method.delivery_tag)

        except ValueError as ve:
            if "was previously cleared, cannot recreate directory" in str(ve):
                channel.basic_ack(delivery_tag=method.delivery_tag)
            else:
                logging.error(f"ValueError processing message: {ve}")
                channel.basic_reject(delivery_tag=method.delivery_tag, requeue=True) 
                            
        except Exception as e:
            logging.error(f"Error processing movies message: {e}")
            # Reject the message and requeue it
            channel.basic_reject(delivery_tag=method.delivery_tag, requeue=True)
    
    def _process_credits_message(self, channel, method, properties, body):
        """Process a message from the credits queue"""
        try:
            deserialized_message = Serializer.deserialize(body)
            client_id = deserialized_message.get("client_id")
            data = deserialized_message.get("data")
            eof_marker = deserialized_message.get("EOF_MARKER")
            disconnect_marker = deserialized_message.get("DISCONNECT")
            operation_id = deserialized_message.get("operation_id")
            node_id = deserialized_message.get("node_id")
            new_operation_id = self.credits_data_persistence.get_counter_value()
            
            if disconnect_marker:
                self.send_data(client_id, data, False, disconnect_marker=True, operation_id=new_operation_id)
                self.client_states_data_persistence.clear(client_id)
                self.movies_data_persistence.clear(client_id)
                self.credits_data_persistence.clear(client_id)
                self.credits_data_persistence.increment_counter()
                channel.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            # Check if this message was already processed (deduplication)
            if self.credits_data_persistence.is_message_processed(client_id, node_id, operation_id):
                logging.info(f"Credits message {operation_id} from node {node_id} already processed for client {client_id}")
                self.credits_data_persistence.increment_counter()
                channel.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            movies_done = self.client_states_data_persistence.retrieve(client_id)

            if movies_done is None:
                # We haven't received all movies yet, reject and requeue the message
                logging.debug(f"Not all movies received for client {client_id}, requeuing credits message")
                channel.basic_reject(delivery_tag=method.delivery_tag, requeue=True)
                time.sleep(self.requeue_delay)
                return

            if eof_marker:
                logging.info(f"Received EOF marker for credits from client '{client_id}'")
                self._finalize_client(client_id, new_operation_id)
            
            elif data:
                all_movies = self.movies_data_persistence.retrieve(client_id)
                
                joined_data = self._join_data(all_movies, data)
                
                if joined_data:
                    self.send_data(client_id, joined_data, operation_id=new_operation_id)

                self.credits_data_persistence.persist(client_id, node_id, None, operation_id)

            self.credits_data_persistence.increment_counter()
            channel.basic_ack(delivery_tag=method.delivery_tag)

        except ValueError as ve:
            if "was previously cleared, cannot recreate directory" in str(ve):
                channel.basic_ack(delivery_tag=method.delivery_tag)
            else:
                logging.error(f"ValueError processing message: {ve}")
                channel.basic_reject(delivery_tag=method.delivery_tag, requeue=True) 

        except Exception as e:
            logging.error(f"Error processing credits message: {e}")
            channel.basic_reject(delivery_tag=method.delivery_tag, requeue=True)
    
    def _finalize_client(self, client_id, operation_id):
        """Finalize processing for a client whose data is complete"""        
        self.send_data(client_id, [], True, operation_id=operation_id)
        
        # self.client_states_data_persistence.clear(client_id)
        # self.movies_data_persistence.clear(client_id)
        # self.credits_data_persistence.clear(client_id)
        
        logging.info(f"Client {client_id} processing completed")
    
    def _join_data(self, movies_data, credits_data):
        """
        Join the movies and credits data
        
        Args:
            movies_data (dict): Dict of movie_id to movie_name
            credits_data (list): List of credits objects with movie_id and cast fields
            
        Returns:
            list: List of dicts with actor names
        """
        try:
            if not movies_data or not credits_data:
                return []
                
            # Create a set of movie ids from movies_data for efficient lookup
            movie_ids = set(movies_data.keys())
            
            # List to collect actors
            actors = []
            
            # Process each credit, adding actors to our result if the movie is in our dataset
            for credits in credits_data:
                movie_id = credits.get("movie_id")
                if movie_id and movie_id in movie_ids:
                    for actor in credits.get("cast", []):
                        actors.append({
                            "name": actor
                        })
            
            return actors
            
        except Exception as e:
            logging.error(f"Error joining data: {e}")
            return []

    def send_data(self, client_id, data, eof_marker=False, disconnect_marker=False, operation_id=None):
        """Send processed data to the output queue"""
        try:
            message = Serializer.add_metadata(client_id, data, eof_marker, None, disconnect_marker, operation_id, self.node_id)
            success = self.rabbitmq.publish(
                exchange_name=self.exchange_name_producer,
                routing_key=self.producer_queue_name,
                message=Serializer.serialize(message),
                persistent=True
            )
            if not success:
                logging.error(f"Failed to send joined data to output queue for client {client_id}")
        except Exception as e:
            logging.error(f"Error sending data to output queue: {e}")
            raise e
        
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