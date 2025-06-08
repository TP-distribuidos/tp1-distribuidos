import logging
import signal
import os
import time
from rabbitmq.Rabbitmq_client import RabbitMQClient
from common.Serializer import Serializer
from dotenv import load_dotenv
from collections import defaultdict
from common.SentinelBeacon import SentinelBeacon
import uuid

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)

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
        
        self.client_data = defaultdict(dict)
        self.node_id = NODE_ID
        self.message_counter = 0

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
    
        # Add this method after _get_max_min
    def _get_next_message_id(self):
        """Get the next incremental message ID for this node"""
        self.message_counter += 1
        return self.message_counter
    
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
            new_operation_id = self._get_next_message_id()
            
            # If we receive a message for a client that no longer exists in our states,
            # it means the client was disconnected already - just acknowledge and ignore
            if client_id not in self.client_data and not disconnect_marker and not eof_marker and data:
                logging.debug(f"Initializing data for client {client_id}")
            
            if disconnect_marker:
                logging.info(f"Processing disconnect marker for client {client_id}")
                self._send_data(client_id, data, queue_name=self.producer_queue_name[0], disconnect_marker=True, operation_id=new_operation_id, node_id=self.node_id)
                self.client_data.pop(client_id, None)
            
            elif eof_marker:
                # If we have data for this client, send it to router producer queue
                if client_id in self.client_data:
                    max_min = self._get_max_min(client_id)
                    self._send_data(client_id, max_min, self.producer_queue_name[0], operation_id=new_operation_id, node_id=self.node_id)
                    new_operation_id = self._get_next_message_id() # DONT FORGET THIS
                    self._send_data(client_id, {}, self.producer_queue_name[0], True, operation_id=new_operation_id, node_id=self.node_id)
                    # Clean up client data after sending
                    del self.client_data[client_id]
                    logging.info(f"\033[92mSent max/min ratings for client {client_id} and cleaned up\033[0m")
                else:
                    logging.warning(f"Received EOF for client {client_id} but no data found")
            elif data:
                self._update_movie_data(client_id, data)
            else:
                logging.warning(f"Received message with no data for client {client_id}")
            
            channel.basic_ack(delivery_tag=method.delivery_tag)
            
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            # Reject the message and requeue it
            channel.basic_reject(delivery_tag=method.delivery_tag, requeue=True)

    
    def _update_movie_data(self, client_id, data):
        """
        Update the movie data for a client
        
        Args:
            client_id (str): Client identifier
            data (dict or list): Movie ratings data from upstream
        """
        if not data:
            logging.info(f"Received empty data batch for client {client_id}")
            return
        
        for movie in data:
            movie_id = movie.get('id')
            if movie_id is None:
                logging.warning(f"\033[93mSkipping movie with missing id: {movie}\033[0m")
                continue
            if movie.get('count', 0) <= 0 or movie.get('sum', 0) <= 0:
                logging.warning(f"\033[93mSkipping movie with zero count or zero sum or empty values: {movie}\033[0m")
                continue

            # Initialize client data if not present
            if client_id not in self.client_data:
                self.client_data[client_id] = {}

            # Initialize movie data if not present
            if movie_id not in self.client_data[client_id]:
                self.client_data[client_id][movie_id] = {
                    'sum': 0,
                    'count': 0,
                    'name': movie.get('name', '')
                }
            # Update the sum and count
            self.client_data[client_id][movie_id]['sum'] += movie.get('sum')
            self.client_data[client_id][movie_id]['count'] += movie.get('count')
            self.client_data[client_id][movie_id]['avg'] = self.client_data[client_id][movie_id]['sum'] / self.client_data[client_id][movie_id]['count']

    def _get_max_min(self, client_id):
        """
        Calculate and get the movies with max and min ratings for a client
        
        Args:
            client_id (str): Client identifier
            
        Returns:
            dict: Dictionary with max and min movie entries
        """
        if client_id not in self.client_data or not self.client_data[client_id]:
            logging.warning(f"No data found for client {client_id}")
            return {'max': None, 'min': None}
        
        max_movie = None
        min_movie = None
        max_avg = -float('inf')
        min_avg = float('inf')
        
        # Find max and min from all stored movies
        for movie_id, movie_data in self.client_data[client_id].items():
            avg_rating = movie_data.get('avg', 0)
            
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

    
    def _send_data(self, client_id, data, queue_name=None, eof_marker=False, query=None, disconnect_marker=False, operation_id=None, node_id=None):
        """Send data to the specified router producer queue"""
        if queue_name is None:
            queue_name = self.producer_queue_name[0]

        message = Serializer.add_metadata(client_id, data, eof_marker, query, disconnect_marker, operation_id, node_id)
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