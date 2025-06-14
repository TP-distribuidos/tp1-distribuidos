import logging
import signal
import os
import time
from rabbitmq.Rabbitmq_client import RabbitMQClient
from common.Serializer import Serializer
from dotenv import load_dotenv
from collections import defaultdict
from common.SentinelBeacon import SentinelBeacon
from StateInterpreter import StateInterpreter
from common.data_persistance.WriteAheadLog import WriteAheadLog
from common.data_persistance.FileSystemStorage import FileSystemStorage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)

logging.getLogger("pika").setLevel(logging.WARNING)

# Load environment variables
load_dotenv()

SENTINEL_PORT = int(os.getenv("SENTINEL_PORT", "5000"))

# Constants
ROUTER_CONSUME_QUEUE = os.getenv("ROUTER_CONSUME_QUEUE")
ROUTER_PRODUCER_QUEUE = os.getenv("ROUTER_PRODUCER_QUEUE")
EXCHANGE_NAME_PRODUCER = os.getenv("PRODUCER_EXCHANGE", "max_min_movies_exchange")
EXCHANGE_TYPE_PRODUCER = os.getenv("PRODUCER_EXCHANGE_TYPE", "direct")
NODE_ID = os.getenv("NODE_ID")

class Worker:
    def __init__(self, 
                 consumer_queue_name=ROUTER_CONSUME_QUEUE, 
                 exchange_name_producer=EXCHANGE_NAME_PRODUCER, 
                 exchange_type_producer=EXCHANGE_TYPE_PRODUCER, 
                 producer_queue_name=[ROUTER_PRODUCER_QUEUE]):

        self._running = True
        self.consumer_queue_name = consumer_queue_name
        self.producer_queue_name = producer_queue_name
        self.exchange_name_producer = exchange_name_producer
        self.exchange_type_producer = exchange_type_producer
        self.rabbitmq = RabbitMQClient()
        
        self.sentinel_beacon = SentinelBeacon(SENTINEL_PORT)

        self.data_persistence = WriteAheadLog(
            state_interpreter=StateInterpreter(),
            storage=FileSystemStorage(),
            service_name="max_min_worker",
            base_dir="/app/persistence"
        )
        
        self.node_id = NODE_ID

        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
        logging.info(f"max_min Worker initialized for consumer queue '{consumer_queue_name}', producer queues '{producer_queue_name}'")
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
        # Declare input queue
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
        for queue_name in self.producer_queue_name:
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
            no_ack=False
        )
        
        if not success:
            logging.error(f"Failed to set up consumer for queue '{self.consumer_queue_name}'")
            return False

        return True
    
    def _process_message(self, channel, method, properties, body):
        """Process a message and update movies data for the client"""
        try:
            # Deserialize the message
            deserialized_message = Serializer.deserialize(body)
            
            # Extract client_id, data and EOF marker
            client_id = deserialized_message.get("client_id")
            data = deserialized_message.get("data")
            eof_marker = deserialized_message.get("EOF_MARKER", False)
            disconnect_marker = deserialized_message.get("DISCONNECT", False)
            operation_id = deserialized_message.get("operation_id", None)
            new_operation_id = self.data_persistence.get_counter_value()
            node_id = deserialized_message.get("node_id")
            
            
            if disconnect_marker:
                logging.info(f"Processing disconnect marker for client {client_id}")
                self._send_data(client_id, data, queue_name=self.producer_queue_name[0], disconnect_marker=True, operation_id=new_operation_id)
                self.data_persistence.clear(client_id)
                self.data_persistence.increment_counter()
            
            elif eof_marker:
                if self.data_persistence.is_message_processed(client_id, node_id, operation_id):
                    self.data_persistence.increment_counter()
                    channel.basic_ack(delivery_tag=method.delivery_tag)
                    return
                
                data_persisted = self.data_persistence.retrieve(client_id)
                max_min = self._get_max_min(data_persisted)

                self._send_data(client_id, max_min, self.producer_queue_name[0], operation_id=new_operation_id)

                self.data_persistence.increment_counter()
                new_operation_id = self.data_persistence.get_counter_value() # DONT FORGET THIS
                self._send_data(client_id, {}, self.producer_queue_name[0], True, operation_id=new_operation_id)
                # Clean up client data after sending
                self.data_persistence.clear(client_id)
                self.data_persistence.increment_counter()
                
            elif data:
                self.data_persistence.persist(client_id, node_id, data, operation_id)
            else:
                logging.warning(f"Received message with no data for client {client_id}")
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

    
    def _get_max_min(self, data):
        """
        Calculate and get the movies with max and min ratings from persisted data
        
        Args:
            data: Dictionary of movie data retrieved from WAL
                {movie_id: {'sum': float, 'count': int, 'name': str, 'id': str}, ...}
            
        Returns:
            dict: Dictionary with max and min movie entries
        """
        if not data:
            return {'max': None, 'min': None}
        
        max_movie = None
        min_movie = None
        max_avg = -float('inf')
        min_avg = float('inf')
        
        # Find max and min from all stored movies
        for movie_id, movie_data in data.items():
            # Skip movies with no count to avoid division by zero
            if movie_data.get('count', 0) <= 0:
                continue
                
            # Calculate average rating
            avg_rating = movie_data.get('sum', 0) / movie_data.get('count', 1)
            
            # Update max if this rating is higher
            if avg_rating > max_avg:
                max_avg = avg_rating
                max_movie = {
                    'id': movie_id,
                    'avg': avg_rating,
                    'name': movie_data.get('name', '')
                }
                
            # Update min if this rating is lower
            if avg_rating < min_avg:
                min_avg = avg_rating
                min_movie = {
                    'id': movie_id,
                    'avg': avg_rating,
                    'name': movie_data.get('name', '')
                }
        
        result = {
            'max': max_movie,
            'min': min_movie
        }
        
        return result

    
    def _send_data(self, client_id, data, queue_name=None, eof_marker=False, query=None, disconnect_marker=False, operation_id=None):
        """Send data to the specified router producer queue"""
        if queue_name is None:
            queue_name = self.producer_queue_name[0]

        message = Serializer.add_metadata(client_id, data, eof_marker, query, disconnect_marker, operation_id, self.node_id)
        success = self.rabbitmq.publish(
            exchange_name=self.exchange_name_producer,
            routing_key=queue_name,
            message=Serializer.serialize(message),
            persistent=True,
            exchange_type=self.exchange_type_producer
        )
        
        if not success:
            logging.error(f"Failed to send data to {queue_name} for client {client_id}")
        
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