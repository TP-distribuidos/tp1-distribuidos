import asyncio
import logging
import signal
import os
from rabbitmq.Rabbitmq_client import RabbitMQClient
from common.Serializer import Serializer
from dotenv import load_dotenv
from common.SentinelBeacon import SentinelBeacon

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)

# Load environment variables
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
        
        # Client state tracking
        self.client_states = {}  # {client_id: {'movies_done': bool}}
        
        # Data store for processing - only store movies, not credits
        self.collected_data = {}  # {client_id: {movie_id: movie_name}}
        
        # For requeue delay to avoid overwhelming the broker
        self.requeue_delay = REQUEUE_DELAY
        
        # Message counter for incremental IDs
        self.message_counter = 0
        
        # Store the node ID for message identification
        self.node_id = NODE_ID

        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
        logging.info(f"Worker initialized for consumer queues '{consumer_queue_names}', producer queue '{producer_queue_name}' and exchange producer '{exchange_name_producer}'")
    
    def _get_next_message_id(self):
        """Get the next incremental message ID for this node"""
        self.message_counter += 1
        return self.message_counter
    
    async def run(self):
        """Run the worker, connecting to RabbitMQ and consuming messages"""
        # Connect to RabbitMQ
        if not await self._setup_rabbitmq():
            logging.error("Failed to set up RabbitMQ connection. Exiting.")
            return False
        
        logging.info("Worker running and consuming from both queues simultaneously")
        
        # Keep the worker running until shutdown is triggered
        while self._running:
            await asyncio.sleep(1)
            
        return True
    
    async def _setup_rabbitmq(self, retry_count=1):
        """Set up RabbitMQ connection and consumer"""
        # Connect to RabbitMQ
        connected = await self.rabbitmq.connect()
        if not connected:
            logging.error(f"Failed to connect to RabbitMQ, retrying in {retry_count} seconds...")
            wait_time = min(30, 2 ** retry_count)
            await asyncio.sleep(wait_time)
            return await self._setup_rabbitmq(retry_count + 1)
        
        # -------------------- CONSUMER --------------------
        # Declare all queues (idempotent operation)
        for queue_name in self.consumer_queue_names:
            queue = await self.rabbitmq.declare_queue(queue_name, durable=True)
            if not queue:
                return False
        # --------------------------------------------------

        # -------------------- PRODUCER --------------------
        # Declare exchange (idempotent operation)
        exchange = await self.rabbitmq.declare_exchange(
            name=self.exchange_name_producer,
            exchange_type=self.exchange_type_producer,
            durable=True
        )
        if not exchange:
            logging.error(f"Failed to declare exchange '{self.exchange_name_producer}'")
            return False
        
        # Declare the producer queue (router input queue)
        queue = await self.rabbitmq.declare_queue(self.producer_queue_name, durable=True)
        if not queue:
            return False        
        
        # Bind queue to exchange
        success = await self.rabbitmq.bind_queue(
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
            success = await self.rabbitmq.consume(
                queue_name=queue_name,
                callback=callback,
                no_ack=False
            )
            if not success:
                logging.error(f"Failed to set up consumer for queue '{queue_name}'")
                return False
            logging.info(f"Started consuming from {queue_name}")

        return True
    
    async def _process_movies_message(self, message):
        """Process a message from the movies queue"""
        try:
            deserialized_message = Serializer.deserialize(message.body)
            client_id = deserialized_message.get("client_id")
            data = deserialized_message.get("data")
            eof_marker = deserialized_message.get("EOF_MARKER")
            disconnect_marker = deserialized_message.get("DISCONNECT")
            operation_id = deserialized_message.get("operation_id")

            # Initialize client state if this is a new client
            if client_id not in self.client_states:
                self.client_states[client_id] = {'movies_done': False}
                
            if disconnect_marker:
                await self.send_data(client_id, data, False, disconnect_marker=True, operation_id=operation_id)
                self.client_states.pop(client_id, None)
                self.collected_data.pop(client_id, None)

            # Handle EOF marker for movies
            elif eof_marker:
                logging.info(f"Received EOF marker for movies from client '{client_id}'")
                self.client_states[client_id]['movies_done'] = True
            
            # Process movie data
            elif data:
                # Initialize movie data storage for this client
                if client_id not in self.collected_data:
                    self.collected_data[client_id] = {}
                
                # Store movie data
                for movie in data:
                    movie_id = movie.get('id')
                    movie_name = movie.get('name')
                    if movie_id and movie_name:
                        self.collected_data[client_id][movie_id] = movie_name
            
            await message.ack()
            
        except Exception as e:
            logging.error(f"Error processing movies message: {e}")
            # Reject the message and requeue it
            await message.reject(requeue=True)
    
    async def _process_credits_message(self, message):
        """Process a message from the credits queue"""
        try:
            deserialized_message = Serializer.deserialize(message.body)
            client_id = deserialized_message.get("client_id")
            data = deserialized_message.get("data")
            eof_marker = deserialized_message.get("EOF_MARKER")
            disconnect_marker = deserialized_message.get("DISCONNECT")
            operation_id = deserialized_message.get("operation_id")
            
            # Initialize client state if this is a new client
            if client_id not in self.client_states:
                self.client_states[client_id] = {'movies_done': False}
                
            if disconnect_marker:
                await self.send_data(client_id, data, False, disconnect_marker=True)
                self.client_states.pop(client_id, None)
                self.collected_data.pop(client_id, None)
                await message.ack()
                return
            
            # Check if we've received all movies for this client
            movies_done = self.client_states[client_id]['movies_done']
            if not movies_done:
                # We haven't received all movies yet, reject and requeue the message
                logging.debug(f"Not all movies received for client {client_id}, requeuing credits message")
                await message.reject(requeue=True)
                await asyncio.sleep(self.requeue_delay)
                return

            if eof_marker:
                logging.info(f"Received EOF marker for credits from client '{client_id}'")
                await self._finalize_client(client_id)
            
            elif data:
                new_operation_id = self._get_next_message_id()
                if client_id in self.collected_data:
                    joined_data = self._join_data(
                        self.collected_data[client_id],
                        data
                    )
                    if joined_data:
                        await self.send_data(client_id, joined_data, operation_id=new_operation_id)

            await message.ack()
            
        except Exception as e:
            logging.error(f"Error processing credits message: {e}")
            # Reject the message and requeue it
            await message.reject(requeue=True)
    
    async def _finalize_client(self, client_id):
        """Finalize processing for a client whose data is complete"""        
        # Send EOF marker to next stage with a new operation ID
        new_operation_id = self._get_next_message_id()
        await self.send_data(client_id, [], True, operation_id=new_operation_id)
        
        # Clean up client data to free memory
        if client_id in self.collected_data:
            del self.collected_data[client_id]
        
        # Completely remove all traces of the client
        del self.client_states[client_id]
        
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

    async def send_data(self, client_id, data, eof_marker=False, disconnect_marker=False, operation_id=None):
        """Send processed data to the output queue"""
        try:    
            if operation_id is None:
                operation_id = self._get_next_message_id()
                
            message = Serializer.add_metadata(client_id, data, eof_marker, None, disconnect_marker, operation_id, self.node_id)
            success = await self.rabbitmq.publish(
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
        logging.info(f"Shutting down worker...")
        self._running = False
        
        # Close RabbitMQ connection - note we need to create a task
        # since this is called from a signal handler
        if hasattr(self, 'rabbitmq'):
            asyncio.create_task(self.rabbitmq.close())
