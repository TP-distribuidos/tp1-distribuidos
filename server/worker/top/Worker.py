import logging
import signal
import os
import time
from typing import Dict, List, Any
from rabbitmq.Rabbitmq_client import RabbitMQClient
from common.Serializer import Serializer
from dotenv import load_dotenv
import heapq
from collections import defaultdict
import uuid
from common.SentinelBeacon import SentinelBeacon
# WAL imports
from common.data_persistance.WriteAheadLog import WriteAheadLog
from common.data_persistance.FileSystemStorage import FileSystemStorage
from TopStateInterpreter import TopStateInterpreter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)

# Load environment variables
load_dotenv()

# Constants
ROUTER_CONSUME_QUEUE = os.getenv("ROUTER_CONSUME_QUEUE")
ROUTER_PRODUCER_QUEUE = os.getenv("ROUTER_PRODUCER_QUEUE",)
EXCHANGE_NAME_PRODUCER = os.getenv("PRODUCER_EXCHANGE", "top_actors_exchange")
EXCHANGE_TYPE_PRODUCER = os.getenv("PRODUCER_EXCHANGE_TYPE", "direct")
TOP_N = int(os.getenv("TOP_N", 10))
SENTINEL_PORT = int(os.getenv("SENTINEL_PORT", "5000"))
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
        
        # WAL REPLACES IN-MEMORY STORAGE!
        # No more self.client_data = {} - WAL handles persistence
        self.top_n = TOP_N
        self.node_id = NODE_ID
        
        # Initialize WAL
        storage = FileSystemStorage()
        state_interpreter = TopStateInterpreter()
        self.wal = WriteAheadLog(
            state_interpreter=state_interpreter,
            storage=storage,
            service_name="top_worker",
            base_dir="/app/persistence"
        )
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
        logging.info(f"Top Worker initialized with WAL persistence")
        logging.info(f"Consumer queue: '{consumer_queue_name}', producer queues: '{producer_queue_name}'")
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
        """Process a message and update top actors for the client using WAL"""
        try:
            # Deserialize the message
            deserialized_message = Serializer.deserialize(body)
            
            # Extract client_id, data and EOF marker
            client_id = deserialized_message.get("client_id")
            data = deserialized_message.get("data")
            eof_marker = deserialized_message.get("EOF_MARKER", False)
            disconnect_marker = deserialized_message.get("DISCONNECT", False)
            operation_id = deserialized_message.get("operation_id", None)
            
            # Extract WAL metadata from count workers
            node_id = deserialized_message.get("node_id")
            message_id = operation_id

            if disconnect_marker:
                # Clear WAL data for this client on disconnect
                self.wal.clear(client_id)
                self._send_data(client_id, data, self.producer_queue_name[0], False, disconnect_marker=True)
            
            elif eof_marker:
                # Retrieve consolidated data from WAL and send top actors
                current_data = self.wal.retrieve(client_id)
                
                if current_data:
                    top_actors = self._get_top_actors_from_dict(current_data)
                    new_operation_id = str(uuid.uuid4())
                    self._send_data(client_id, top_actors, self.producer_queue_name[0], operation_id=new_operation_id)
                    self._send_data(client_id, [], self.producer_queue_name[0], True)
                    
                    # Clean up WAL data after sending
                    self.wal.clear(client_id)
                    logging.info(f"Sent top actors for client {client_id} and cleared WAL data")
                else:
                    logging.warning(f"Received EOF for client {client_id} but no data found in WAL")
                    
            elif data and node_id and message_id:
                # Persist actor data using WAL
                success = self.wal.persist(client_id, node_id, data, message_id)
                
                if success:
                    logging.debug(f"Persisted actor data for client {client_id} from count worker {node_id}, message {message_id}")
                else:
                    logging.error(f"Failed to persist actor data for client {client_id}")
                    
            else:
                if not data:
                    logging.warning(f"Received message with no data for client {client_id}")

            channel.basic_ack(delivery_tag=method.delivery_tag)
            
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            # Reject the message and requeue it
            channel.basic_reject(delivery_tag=method.delivery_tag, requeue=True)
    
    def _get_top_actors_from_dict(self, actor_counts: Dict[str, int]) -> List[Dict[str, Any]]:
        """
        Calculate and return the top N actors from a dictionary of counts.
        
        Args:
            actor_counts: Dictionary of actor_name -> count
            
        Returns:
            List[Dict]: Top N actors formatted as [{"name": "Actor", "count": 5}, ...]
        """
        if not actor_counts:
            return []
        
        # Use heapq to get the top N actors
        top_actors = heapq.nlargest(self.top_n, actor_counts.items(), key=lambda x: x[1])
        
        # Format the result as a list of dictionaries
        return [{"name": actor, "count": count} for actor, count in top_actors]
    
    def _send_data(self, client_id, data, queue_name=None, eof_marker=False, query=None, disconnect_marker=False, operation_id=None):
        """Send data to the specified router producer queue"""
        if queue_name is None:
            queue_name = self.producer_queue_name[0]
            
        message = Serializer.add_metadata(client_id, data, eof_marker, query, disconnect_marker, operation_id, self.node_id)
        success = self.rabbitmq.publish(
            exchange_name=self.exchange_name_producer,
            routing_key=queue_name,
            message=Serializer.serialize(message),
            persistent=True
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
