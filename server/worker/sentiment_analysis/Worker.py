import logging
import signal
import time
import os
from rabbitmq.Rabbitmq_client import RabbitMQClient
from common.Serializer import Serializer
from transformers import pipeline
from dotenv import load_dotenv
from common.SentinelBeacon import SentinelBeacon
from common.data_persistance.StatelessStateInterpreter import StatelessStateInterpreter
from common.data_persistance.WriteAheadLog import WriteAheadLog
from common.data_persistance.FileSystemStorage import FileSystemStorage

# Load environment variables
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)

logging.getLogger("pika").setLevel(logging.CRITICAL)

# Queue names and constants
CONSUMER_QUEUE = os.getenv("ROUTER_CONSUME_QUEUE")
PRODUCER_QUEUE = os.getenv("ROUTER_PRODUCER_QUEUE")
SENTINEL_PORT = int(os.getenv("SENTINEL_PORT", "5000"))
NODE_ID = os.getenv("NODE_ID")
EXCHANGE_NAME = os.getenv("EXCHANGE_NAME", "sentiment_exchange")
EXCHANGE_TYPE = os.getenv("EXCHANGE_TYPE", "direct")
RECONNECT_DELAY = 5  # Base delay for reconnection attempts in seconds
MAX_RECONNECT_DELAY = 60  # Maximum reconnect delay

class SentimentWorker:
    def __init__(self, consumer_queue_name=CONSUMER_QUEUE, router_queue_name=PRODUCER_QUEUE):
        self._running = True
        self.consumer_queue_name = consumer_queue_name
        
        # Handle single queue or multiple queues
        if isinstance(router_queue_name, str):
            self.producer_queue_name = [router_queue_name]
        else:
            self.producer_queue_name = router_queue_name
            
        # Exchange configuration
        self.exchange_name_producer = EXCHANGE_NAME
        self.exchange_type_producer = EXCHANGE_TYPE
        
        self.rabbitmq = RabbitMQClient()
        
        self.sentinel_beacon = SentinelBeacon(SENTINEL_PORT)
        
        # Initialize WAL with StatelessStateInterpreter for persistence
        self.data_persistence = WriteAheadLog(
            state_interpreter=StatelessStateInterpreter(),
            storage=FileSystemStorage(),
            service_name="sentiment_analysis_worker",
            base_dir="/app/persistence"
        )
        
        # Store the node ID for message identification
        self.node_id = NODE_ID
        
        logging.info("Initializing sentiment analysis model...")
        # Load the sentiment analysis pipeline from transformers
        self.sentiment_pipeline = pipeline("sentiment-analysis")
        logging.info("Sentiment analysis model loaded successfully!")
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
        logging.info(f"Sentiment Analysis Worker initialized for consumer queue '{consumer_queue_name}', response queues '{self.producer_queue_name}'")
    
    def run(self):
        """Run the worker, connecting to RabbitMQ and consuming messages with reconnection logic"""
        retry_count = 0
        
        while self._running:
            try:
                # Connect to RabbitMQ
                if not self._setup_rabbitmq():
                    retry_count += 1
                    wait_time = min(MAX_RECONNECT_DELAY, RECONNECT_DELAY * (2 ** min(retry_count, 5)))
                    logging.error(f"Failed to set up RabbitMQ connection. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                
                # Reset retry count on successful connection
                retry_count = 0
                
                # Start consuming messages (blocking call)
                self.rabbitmq.start_consuming()
                
            except KeyboardInterrupt:
                self._handle_shutdown()
                break
            except Exception as e:
                logging.error(f"Error in message consumption: {e}")
                if self._running:
                    retry_count += 1
                    wait_time = min(MAX_RECONNECT_DELAY, RECONNECT_DELAY * (2 ** min(retry_count, 5)))
                    logging.info(f"Attempting to reconnect in {wait_time} seconds...")
                    time.sleep(wait_time)
                    # Close any existing connection before reconnecting
                    try:
                        self.rabbitmq.close()
                    except:
                        pass
                    continue
        
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
            no_ack=False,
            prefetch_count=1
        )
        
        if not success:
            logging.error(f"Failed to set up consumer for queue '{self.consumer_queue_name}'")
            return False

        return True
    
    def _safe_publish_to_exchange(self, exchange_name, routing_key, message, persistent=True, retries=0):
        """
        Safely publish a message to an exchange with unlimited retry logic
        Will NEVER give up until the message is successfully published
        Returns True only when the message is successfully published
        """
        try:
            success = self.rabbitmq.publish(
                exchange_name=exchange_name,
                routing_key=routing_key,
                message=message,
                persistent=persistent
            )
            
            if success:
                # Message successfully published
                if retries > 0:
                    logging.info(f"Successfully published message after {retries} retries")
                return True
            else:
                # Publishing returned False but didn't raise an exception
                backoff_time = min(30, 1 + (retries % 10))  # Cap backoff but keep retrying
                logging.warning(f"Failed to publish to exchange {exchange_name}, retry {retries+1}. Retrying in {backoff_time}s...")
                time.sleep(backoff_time)
                return self._safe_publish_to_exchange(exchange_name, routing_key, message, persistent, retries + 1)
                
        except Exception as e:
            # An exception occurred during publishing
            backoff_time = min(30, 1 + (retries % 10))  # Cap backoff but keep retrying
            logging.error(f"Error publishing to exchange (retry {retries+1}): {e}")
            
            # If connection issues, try to reconnect
            if "connection" in str(e).lower() or "channel" in str(e).lower():
                logging.warning(f"Connection issue detected. Reconnecting before retry {retries+1}...")
                try:
                    self.rabbitmq.close()
                except:
                    pass
                
                # Try to reconnect
                reconnect_success = False
                reconnect_attempts = 0
                max_reconnect_attempts = 5  # Try a few times before backing off
                
                while not reconnect_success and reconnect_attempts < max_reconnect_attempts:
                    try:
                        reconnect_success = self._setup_rabbitmq()
                        if reconnect_success:
                            logging.info("Successfully reconnected to RabbitMQ")
                            break
                    except Exception as reconnect_error:
                        logging.warning(f"Failed reconnection attempt {reconnect_attempts+1}: {reconnect_error}")
                    
                    reconnect_attempts += 1
                    time.sleep(2)  # Short delay between reconnection attempts
                
                if not reconnect_success:
                    logging.error(f"Failed to reconnect after {max_reconnect_attempts} attempts. Will retry again in {backoff_time}s")
            
            # Sleep with backoff before retrying
            time.sleep(backoff_time)
            
            # Always retry, no matter what - unlimited persistence
            return self._safe_publish_to_exchange(exchange_name, routing_key, message, persistent, retries + 1)
    
    def _process_message(self, channel, method, properties, body):
        """Process a message from the queue"""
        try:
            deserialized_message = Serializer.deserialize(body)
            
            # Extract client_id and data from the deserialized message
            client_id = deserialized_message.get("client_id")
            data = deserialized_message.get("data")
            eof_marker = deserialized_message.get("EOF_MARKER", False)
            disconnect_marker = deserialized_message.get("DISCONNECT", False)
            operation_id = deserialized_message.get("operation_id")
            node_id = deserialized_message.get("node_id")
            new_operation_id = self.data_persistence.get_counter_value()
            
            # Default routing key - use the first queue name
            routing_key = self.producer_queue_name[0]
            
            # Check if this message was already processed
            if self.data_persistence.is_message_processed(client_id, node_id, operation_id):
                logging.info(f"Message {operation_id} from node {node_id} already processed for client {client_id}")
                self.data_persistence.increment_counter()
                channel.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            if disconnect_marker:
                # Generate an operation ID for this message
                response_message = Serializer.add_metadata(
                    client_id=client_id,
                    data=None,
                    eof_marker=False,
                    query=None,
                    disconnect_marker=True,
                    operation_id=new_operation_id,
                    node_id=self.node_id
                )
                
                # Send processed data to exchange with retry logic
                success = self._safe_publish_to_exchange(
                    exchange_name=self.exchange_name_producer,
                    routing_key=routing_key,
                    message=Serializer.serialize(response_message),
                    persistent=True
                )
                
                # Message WILL be published with unlimited retries, so this should never happen
                if not success:
                    logging.error("Failed to send disconnect marker to exchange")
                    channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                    return
                
                # Clear client data from WAL
                self.data_persistence.clear(client_id)
                self.data_persistence.increment_counter()
                channel.basic_ack(delivery_tag=method.delivery_tag)
            
            elif eof_marker:
                # Pass through EOF marker to exchange using add_metadata
                response_message = Serializer.add_metadata(
                    client_id=client_id,
                    data=[],
                    eof_marker=True,
                    query=None,
                    disconnect_marker=False,
                    operation_id=new_operation_id,
                    node_id=self.node_id
                )

                success = self._safe_publish_to_exchange(
                    exchange_name=self.exchange_name_producer,
                    routing_key=routing_key,
                    message=Serializer.serialize(response_message),
                    persistent=True
                )
                
                # Message WILL be published with unlimited retries, so this should never happen
                if not success:
                    logging.error("Failed to send EOF marker to exchange")
                    channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                    return
                
                # Persist this operation in WAL
                self.data_persistence.persist(client_id, node_id, None, operation_id)
                self.data_persistence.increment_counter()
                
                channel.basic_ack(delivery_tag=method.delivery_tag)
            
            # Process the movie data for sentiment analysis
            elif data:
                logging.info(f"Processing {len(data)} movies for sentiment analysis")
                processed_data = self._analyze_sentiment_and_calculate_ratios(data)
                
                # Prepare response message using the standardized add_metadata method
                response_message = Serializer.add_metadata(
                    client_id=client_id,
                    data=processed_data,
                    eof_marker=False,
                    query=None,
                    disconnect_marker=False,
                    operation_id=new_operation_id,
                    node_id=self.node_id
                )
                
                logging.info(f"Sending message with operation ID {new_operation_id} to Exchange, node ID {self.node_id}")
                
                # Send processed data to exchange with retry logic - will never give up
                success = self._safe_publish_to_exchange(
                    exchange_name=self.exchange_name_producer,
                    routing_key=routing_key,
                    message=Serializer.serialize(response_message),
                    persistent=True
                )
                
                # Message WILL be published with unlimited retries, so this should never happen
                if not success:
                    logging.error("Failed to send processed data to exchange")
                    channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                    return
                
                # Persist this operation in WAL
                self.data_persistence.persist(client_id, node_id, None, operation_id)
                self.data_persistence.increment_counter()
                channel.basic_ack(delivery_tag=method.delivery_tag)
            else:
                logging.warning(f"Received empty data from client {client_id}")
                self.data_persistence.increment_counter()
                channel.basic_ack(delivery_tag=method.delivery_tag)

        except ValueError as ve:
            if "was previously cleared, cannot recreate directory" in str(ve):
                channel.basic_ack(delivery_tag=method.delivery_tag)
            else:
                logging.error(f"ValueError processing message: {ve}")
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True) 

        except Exception as e:
            logging.error(f"Error processing message: {e}")
            # Check if it's a connection issue
            if "connection" in str(e).lower() or "channel" in str(e).lower():
                logging.error("Connection issue detected, will retry message")
                try:
                    channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                except Exception as ex:
                    logging.error(f"Failed to nack message due to {ex}, will be requeued on reconnection")
            else:
                # For non-connection errors, don't requeue to avoid infinite loops
                try:
                    channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                except:
                    logging.error("Failed to nack message, channel may be closed")

    def _analyze_sentiment_and_calculate_ratios(self, data):
        processed_movies = []
        start_time = time.time()
        
        # Process one movie at a time
        batch_size = 1
        total_movies = len(data)
        log_interval = max(1, min(50, total_movies // 10))  # Logs to see visualize progress
        
        logging.info(f"Starting sentiment analysis on {total_movies} movies...")
        
        for i in range(0, total_movies, batch_size):
            current_batch = data[i:i+batch_size]
            current_movie_num = i + 1
            
            # Log progress periodically
            if current_movie_num % log_interval == 0 or current_movie_num == 1:
                elapsed = time.time() - start_time
                progress_pct = (current_movie_num / total_movies) * 100
                movies_per_sec = current_movie_num / max(elapsed, 0.1)
                
                logging.info(f"Progress: {current_movie_num}/{total_movies} movies ({progress_pct:.1f}%) - " +
                            f"Speed: {movies_per_sec:.1f} movies/sec")
            
            for movie in current_batch:
                try:
                    # Extract required fields
                    original_title = movie.get('original_title', 'Unknown')
                    overview = movie.get('overview', '')
                    budget = float(movie.get('budget', 0))
                    revenue = float(movie.get('revenue', 0))
                    
                    # Calculate ratio
                    ratio = revenue / budget if budget > 0 else 0
                    
                    # Get sentiment - using same approach as our test
                    sentiment_result = self.analyze_sentiment(overview)
                    sentiment_label = sentiment_result[0]
                    confidence = sentiment_result[1]
                    
                    # Create processed movie record
                    processed_movie = {
                        "name": original_title, 
                        "sentiment": sentiment_label,
                        "ratio": ratio,
                        "confidence": confidence
                    }
                    
                    processed_movies.append(processed_movie)
                    
                except Exception as e:
                    logging.error(f"Error processing movie {movie.get('original_title', 'Unknown')}: {e}")
                    continue
            
            # Sleep a tiny bit to allow other operations to run
            time.sleep(0.01)
        
        total_time = time.time() - start_time
        
        logging.info(f"\033[32mCompleted sentiment analysis of {total_movies} movies in {total_time:.2f} seconds\033[0m")
        
        return processed_movies
    
    def analyze_sentiment(self, text):
        """
        Analyze the sentiment of the given text.
        Returns a tuple of (sentiment_label, confidence_score)
        """
        if not text or text.strip() == "":
            logging.debug("Received empty text for sentiment analysis.")
            return ("NEUTRAL", 0.5)
        
        try:
            # Truncate text to avoid exceeding the model's maximum token limit (512)
            # A simple character-based truncation as a reasonable approximation
            max_chars = 1000  # Approximate character count that would result in ~500 tokens
            if len(text) > max_chars:
                logging.debug(f"Truncating overview text from {len(text)} to {max_chars} characters")
                text = text[:max_chars]
            
            # Use the Hugging Face transformers pipeline directly - exact same approach from our test
            result = self.sentiment_pipeline(text)[0]
            label = result['label']  # Will be POSITIVE or NEGATIVE
            score = result['score']  # Confidence score
            
            return (label, score)
                
        except Exception as e:
            logging.error(f"Error during sentiment analysis: {e}")
            return ("NEUTRAL", 0.5)
    
    def _handle_shutdown(self, *_):
        logging.info(f"Shutting down sentiment analysis worker...")
        self._running = False
        if hasattr(self, 'rabbitmq'):
            self.rabbitmq.stop_consuming()
            self.rabbitmq.close()
            
        # Shut down the sentinel beacon
        if hasattr(self, 'sentinel_beacon'):
            self.sentinel_beacon.shutdown()