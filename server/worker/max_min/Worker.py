import asyncio
import logging
import signal
import os
from typing import Dict, List, Any
from rabbitmq.Rabbitmq_client import RabbitMQClient
from common.Serializer import Serializer
from dotenv import load_dotenv
from collections import defaultdict
from common.SentinelBeacon import SentinelBeacon
import uuid

# WAL imports
from common.data_persistance.WriteAheadLog import WriteAheadLog
from common.data_persistance.FileSystemStorage import FileSystemStorage
from MaxMinStateInterpreter import MaxMinStateInterpreter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)

# Load environment variables
load_dotenv()

SENTINEL_PORT = int(os.getenv("SENTINEL_PORT", "5000"))
NODE_ID = os.getenv("NODE_ID")

# Constants
ROUTER_CONSUME_QUEUE = os.getenv("ROUTER_CONSUME_QUEUE")
ROUTER_PRODUCER_QUEUE = os.getenv("ROUTER_PRODUCER_QUEUE")
EXCHANGE_NAME_PRODUCER = os.getenv("PRODUCER_EXCHANGE", "max_min_movies_exchange")
EXCHANGE_TYPE_PRODUCER = os.getenv("PRODUCER_EXCHANGE_TYPE", "direct")

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
        
        # WAL REPLACES IN-MEMORY STORAGE!
        # No more self.client_data = defaultdict(dict) - WAL handles persistence
        self.node_id = NODE_ID
        
        # Initialize WAL
        storage = FileSystemStorage()
        state_interpreter = MaxMinStateInterpreter()
        self.wal = WriteAheadLog(
            state_interpreter=state_interpreter,
            storage=storage,
            service_name="max_min_worker",
            base_dir="/app/persistence"
        )

        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
        logging.info(f"Consumer queue: '{consumer_queue_name}', producer queues: '{producer_queue_name}'")
        logging.info(f"Exchange producer: '{exchange_name_producer}', type: '{exchange_type_producer}'")
    
    async def run(self):
        """Run the worker, connecting to RabbitMQ and consuming messages"""
        # Connect to RabbitMQ
        if not await self._setup_rabbitmq():
            logging.error(f"Failed to set up RabbitMQ connection. Exiting.")
            return False
        
        logging.info(f"Worker running and consuming from queue '{self.consumer_queue_name}'")
        
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
        # Declare input queue
        queue = await self.rabbitmq.declare_queue(self.consumer_queue_name, durable=True)
        if not queue:
            logging.error(f"Failed to declare consumer queue '{self.consumer_queue_name}'")
            return False
        # --------------------------------------------------

        # -------------------- PRODUCER --------------------
        # Declare exchange
        exchange = await self.rabbitmq.declare_exchange(
            name=self.exchange_name_producer,
            exchange_type=self.exchange_type_producer,
            durable=True
        )
        if not exchange:
            logging.error(f"Failed to declare exchange '{self.exchange_name_producer}'")
            return False
        
        # Declare output queues
        for queue_name in self.producer_queue_name:
            queue = await self.rabbitmq.declare_queue(queue_name, durable=True)
            if not queue:
                logging.error(f"Failed to declare producer queue '{queue_name}'")
                return False        
            
            # Bind queues to exchange
            success = await self.rabbitmq.bind_queue(
                queue_name=queue_name,
                exchange_name=self.exchange_name_producer,
                routing_key=queue_name
            )
            if not success:
                logging.error(f"Failed to bind queue '{queue_name}' to exchange '{self.exchange_name_producer}'")
                return False
        # --------------------------------------------------
        
        # Set up consumer for the input queue
        success = await self.rabbitmq.consume(
            queue_name=self.consumer_queue_name,
            callback=self._process_message,
            no_ack=False
        )
        
        if not success:
            logging.error(f"Failed to set up consumer for queue '{self.consumer_queue_name}'")
            return False

        return True
    
    async def _process_message(self, message):
        """Process a message and update movies data for the client using WAL"""
        try:
            # Deserialize the message
            deserialized_message = Serializer.deserialize(message.body)
            
            # Extract client_id, data and EOF marker
            client_id = deserialized_message.get("client_id")
            data = deserialized_message.get("data")
            eof_marker = deserialized_message.get("EOF_MARKER", False)
            disconnect_marker = deserialized_message.get("DISCONNECT", False)
            operation_id = deserialized_message.get("operation_id", None)
            node_id = deserialized_message.get("node_id")
            
            message_id = operation_id if operation_id else str(uuid.uuid4())
            
            if disconnect_marker:
                # Clear WAL data for this client on disconnect
                self.wal.clear(client_id)
                await self._send_data(client_id, data, queue_name=self.producer_queue_name[0], disconnect_marker=True)
            
            elif eof_marker:
                # Retrieve consolidated data from WAL and send max/min movies
                current_data = self.wal.retrieve(client_id)
                
                if current_data:
                    max_min = self._get_max_min_from_wal_data(current_data)
                    new_operation_id = str(uuid.uuid4())
                    await self._send_data(client_id, max_min, self.producer_queue_name[0], operation_id=new_operation_id)
                    await self._send_data(client_id, {}, self.producer_queue_name[0], True)
                    
                    # Clean up WAL data after sending
                    self.wal.clear(client_id)
                    logging.info(f"Sent max/min ratings for client {client_id} and cleared WAL data")
                else:
                    logging.warning(f"Received EOF for client {client_id} but no data found in WAL")
                    
            elif data:
                # Persist movie data using WAL
                success = self.wal.persist(client_id, node_id, data, message_id)
                
                if success:
                    logging.debug(f"Persisted movie data for client {client_id}, message {message_id}")
                else:
                    logging.error(f"Failed to persist movie data for client {client_id}")
                    
            else:
                logging.warning(f"Received message with no data for client {client_id}")
            
            await message.ack()
            
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            # Reject the message and requeue it
            await message.reject(requeue=True)
    
    def _get_max_min_from_wal_data(self, movie_data):
        """
        Calculate and get the movies with max and min ratings from WAL data
        
        Args:
            movie_data: Dictionary of movies from WAL
            
        Returns:
            dict: Dictionary with max and min movie entries
        """
        if not movie_data or not isinstance(movie_data, dict):
            logging.warning(f"No valid movie data found in WAL")
            return {'max': None, 'min': None}
        
        max_movie = None
        min_movie = None
        max_avg = -float('inf')
        min_avg = float('inf')
        
        # Find max and min from all stored movies
        for movie_id, movie in movie_data.items():
            # Skip entries that aren't proper movie data
            if not isinstance(movie, dict):
                continue
            
            avg_rating = None
                
            # Only calculate avg based on sum and count - we don't store avg values
            if movie.get('count', 0) > 0 and movie.get('sum', 0) > 0:
                avg_rating = movie['sum'] / movie['count']
            else:
                # Skip movies without valid sum and count
                continue
                
            # Update max if this rating is higher
            if avg_rating > max_avg:
                max_avg = avg_rating
                max_movie = {
                    'id': movie_id,
                    'avg': avg_rating,
                    'name': movie.get('name', '')
                }
                
            # Update min if this rating is lower
            if avg_rating < min_avg:
                min_avg = avg_rating
                min_movie = {
                    'id': movie_id,
                    'avg': avg_rating,
                    'name': movie.get('name', '')
                }
        
        result = {
            'max': max_movie,
            'min': min_movie
        }
        
        return result
    
    async def _send_data(self, client_id, data, queue_name=None, eof_marker=False, query=None, disconnect_marker=False, operation_id=None):
        """Send data to the specified router producer queue"""
        if queue_name is None:
            queue_name = self.producer_queue_name[0]
            
        message = Serializer.add_metadata(client_id, data, eof_marker, query, disconnect_marker, operation_id, self.node_id)
        success = await self.rabbitmq.publish(
            exchange_name=self.exchange_name_producer,
            routing_key=queue_name,
            message=Serializer.serialize(message),
            persistent=True
        )
        
        if not success:
            logging.error(f"Failed to send data to {queue_name} for client {client_id}")
        
    def _handle_shutdown(self, *_):
        """Handle shutdown signals"""
        logging.info(f"Shutting down worker...")
        self._running = False
        
        # Close RabbitMQ connection - note we need to create a task
        # since this is called from a signal handler
        if hasattr(self, 'rabbitmq'):
            asyncio.create_task(self.rabbitmq.close())
