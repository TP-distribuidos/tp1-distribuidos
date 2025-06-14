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

logging.getLogger("pika").setLevel(logging.ERROR)

# Load environment variables
load_dotenv()

SENTINEL_PORT = int(os.getenv("SENTINEL_PORT", "5000"))

# Constants
NODE_ID = os.getenv("NODE_ID")
ROUTER_CONSUME_QUEUE = os.getenv("ROUTER_CONSUME_QUEUE")
RESPONSE_QUEUE = os.getenv("RESPONSE_QUEUE")
EXCHANGE_NAME_PRODUCER = os.getenv("PRODUCER_EXCHANGE", "average_sentiment_exchange")
EXCHANGE_TYPE_PRODUCER = os.getenv("PRODUCER_EXCHANGE_TYPE", "direct")
QUERY_5 = os.getenv("QUERY_5", "5")

class Worker:
    def __init__(self, 
                 consumer_queue_name=ROUTER_CONSUME_QUEUE, 
                 exchange_name_producer=EXCHANGE_NAME_PRODUCER, 
                 exchange_type_producer=EXCHANGE_TYPE_PRODUCER, 
                 producer_queue_name=[RESPONSE_QUEUE]):

        self._running = True
        self.consumer_queue_name = consumer_queue_name
        self.producer_queue_name = producer_queue_name
        self.exchange_name_producer = exchange_name_producer
        self.exchange_type_producer = exchange_type_producer
        self.rabbitmq = RabbitMQClient()
        
        self.sentinel_beacon = SentinelBeacon(SENTINEL_PORT)
        
        # Replace in-memory client_data with WAL
        self.data_persistence = WriteAheadLog(
            state_interpreter=StateInterpreter(),
            storage=FileSystemStorage(),
            service_name="average_sentiment_collector",
            base_dir="/app/persistence"
        )
        
        # Store the node ID for message identification
        self.node_id = NODE_ID
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
        logging.info(f"Average Sentiment Collector initialized for consumer queue '{consumer_queue_name}', producer queues '{producer_queue_name}'")
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
        """Process a message from the various average sentiment workers"""
        try:
            deserialized_message = Serializer.deserialize(body)
            
            client_id = deserialized_message.get("client_id")
            data = deserialized_message.get("data")
            eof_marker = deserialized_message.get("EOF_MARKER", False)
            disconnect_marker = deserialized_message.get("DISCONNECT")
            operation_id = deserialized_message.get("operation_id")
            node_id = deserialized_message.get("node_id")
            
            if self.data_persistence.is_message_processed(client_id, node_id, operation_id):
                logging.info(f"Message {operation_id} from node {node_id} already processed for client {client_id}")
                self.data_persistence.increment_counter()
                channel.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            new_operation_id = self.data_persistence.get_counter_value()
    
            if disconnect_marker:
                self.data_persistence.clear(client_id)
                logging.info(f"\033[91mDisconnect marker received for client_id '{client_id}'\033[0m")
                channel.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            elif eof_marker:
                try:
                    sentiment_data = self.data_persistence.retrieve(client_id)
                    if sentiment_data:
                        # Calculate final averages and send
                        self._calculate_and_send_final_average(client_id, sentiment_data, new_operation_id)
                        
                        # Clean up client data
                        self.data_persistence.clear(client_id)
                        logging.info(f"Sent final average sentiment for client {client_id} and cleaned up data")
                    else:
                        logging.warning(f"Received EOF for client {client_id} but no data found")
                except ValueError as e:
                    logging.warning(f"Failed to retrieve WAL data for client {client_id}, error: {e}")
            
            elif data:
                try:
                    self.data_persistence.persist(client_id, node_id, data, operation_id)
                except ValueError as e:
                    logging.warning(f"Error persisting sentiment data for client {client_id}, error: {e}")
            
            self.data_persistence.increment_counter()
            channel.basic_ack(delivery_tag=method.delivery_tag)
    
        except ValueError as ve:
            if "was previously cleared, cannot recreate directory" in str(ve):
                channel.basic_ack(delivery_tag=method.delivery_tag)
            else:
                logging.error(f"ValueError processing message: {ve}")
                channel.basic_reject(delivery_tag=method.delivery_tag, requeue=True)        
    
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            channel.basic_reject(delivery_tag=method.delivery_tag, requeue=True)
    
    def _update_sentiment_totals(self, client_data, data):
        """Update the sentiment totals for a client from new data"""
        if not client_data:
            client_data = {
                "POSITIVE": {"sum": 0, "count": 0},
                "NEGATIVE": {"sum": 0, "count": 0}
            }
            
        for sentiment_data in data:
            sentiment = sentiment_data.get("sentiment")
            sum_value = sentiment_data.get("sum", 0)
            count = sentiment_data.get("count", 0)
            
            # Only process if we have valid sentiment type
            if sentiment in ["POSITIVE", "NEGATIVE"]:
                # Add to the running totals
                client_data[sentiment]["sum"] += sum_value
                client_data[sentiment]["count"] += count
                
                logging.debug(f"Updated {sentiment} sum: {client_data[sentiment]['sum']}, count: {client_data[sentiment]['count']}")
        
        return client_data
    
    def _calculate_and_send_final_average(self, client_id, client_sentiment_totals, operation_id):
        """Calculate final average and send to response queue"""
        if not client_sentiment_totals:
            logging.warning(f"No data found for client {client_id} when trying to calculate final average")
            return
        
        # Calculate the averages
        positive_avg = 0
        if client_sentiment_totals["POSITIVE"]["count"] > 0:
            positive_avg = client_sentiment_totals["POSITIVE"]["sum"] / client_sentiment_totals["POSITIVE"]["count"]
            # Round to 6 significant digits
            positive_avg = float(f"{positive_avg:.6g}")
        
        negative_avg = 0
        if client_sentiment_totals["NEGATIVE"]["count"] > 0:
            negative_avg = client_sentiment_totals["NEGATIVE"]["sum"] / client_sentiment_totals["NEGATIVE"]["count"]
            # Round to 6 significant digits
            negative_avg = float(f"{negative_avg:.6g}")
        
        # Prepare detailed results
        positive_count = client_sentiment_totals["POSITIVE"]["count"]
        negative_count = client_sentiment_totals["NEGATIVE"]["count"]
        
        # Create the final response
        result = [{
            "sentiment": "POSITIVE",
            "average_ratio": positive_avg,
            "movie_count": positive_count
        }, {
            "sentiment": "NEGATIVE", 
            "average_ratio": negative_avg,
            "movie_count": negative_count
        }]
        
        # Send the final results
        self._send_data(client_id, result, True, operation_id)
    
    def _send_data(self, client_id, data, eof_marker=False, operation_id=None):
        """Send data to the response queue"""
        queue_name = self.producer_queue_name[0]
        
        if operation_id is None:
            operation_id = self.data_persistence.get_counter_value()
            self.data_persistence.increment_counter()
            
        message = Serializer.add_metadata(client_id, data, eof_marker, QUERY_5, False, operation_id, self.node_id)
        success = self.rabbitmq.publish(
            exchange_name=self.exchange_name_producer,
            routing_key=queue_name,
            message=Serializer.serialize(message),
            persistent=True
        )
        
        if not success:
            logging.error(f"Failed to send final sentiment data for client {client_id}")
        
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
            
        # Shut down the sentinel beacon
        if hasattr(self, 'sentinel_beacon'):
            self.sentinel_beacon.shutdown()