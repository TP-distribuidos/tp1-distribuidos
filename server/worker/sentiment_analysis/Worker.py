import logging
import signal
import time
import os
from rabbitmq.Rabbitmq_client import RabbitMQClient
from common.Serializer import Serializer
from transformers import pipeline
from dotenv import load_dotenv
from common.SentinelBeacon import SentinelBeacon

# Load environment variables
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)

# Queue names and constants
CONSUMER_QUEUE = os.getenv("ROUTER_CONSUME_QUEUE")
PRODUCER_QUEUE = os.getenv("ROUTER_PRODUCER_QUEUE")
SENTINEL_PORT = int(os.getenv("SENTINEL_PORT", "5000"))
NODE_ID = os.getenv("NODE_ID")

class SentimentWorker:
    def __init__(self, consumer_queue_name=CONSUMER_QUEUE, response_queue_name=PRODUCER_QUEUE):
        self._running = True
        self.consumer_queue_name = consumer_queue_name
        self.response_queue_name = response_queue_name
        self.rabbitmq = RabbitMQClient()
        
        self.sentinel_beacon = SentinelBeacon(SENTINEL_PORT)
        
        # Message counter for incremental IDs
        self.message_counter = 0
        
        # Store the node ID for message identification
        self.node_id = NODE_ID
        
        logging.info("Initializing sentiment analysis model...")
        # Load the sentiment analysis pipeline from transformers
        self.sentiment_pipeline = pipeline("sentiment-analysis")
        logging.info("Sentiment analysis model loaded successfully!")
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
        logging.info(f"Sentiment Analysis Worker initialized for consumer queue '{consumer_queue_name}', response queue '{response_queue_name}'")
    
    def _get_next_message_id(self):
        """Get the next incremental message ID for this node"""
        self.message_counter += 1
        return self.message_counter
    
    def run(self):
        """Run the worker, connecting to RabbitMQ and consuming messages"""
        # Connect to RabbitMQ
        if not self._setup_rabbitmq():
            logging.error(f"Failed to set up RabbitMQ connection. Exiting.")
            return False
        
        logging.info(f"Sentiment Analysis Worker running and consuming from queue '{self.consumer_queue_name}'")
        
        # Start consuming messages (this will block until shutdown)
        try:
            self.rabbitmq.start_consuming()
        except KeyboardInterrupt:
            logging.info("Received keyboard interrupt, shutting down...")
            self._running = False
        except Exception as e:
            logging.error(f"Error during message consumption: {e}")
            return False
            
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
        
        # Declare queues (idempotent operation)
        queue = self.rabbitmq.declare_queue(self.consumer_queue_name, durable=True)
        if not queue:
            return False
            
        response_queue = self.rabbitmq.declare_queue(self.response_queue_name, durable=True)
        if not response_queue:
            return False

        # Set up consumer
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
    
    def _process_message(self, ch, method, properties, body):
        """Process a message from the queue"""
        try:
            deserialized_message = Serializer.deserialize(body)
            
            # Extract client_id and data from the deserialized message
            client_id = deserialized_message.get("client_id")
            data = deserialized_message.get("data")
            eof_marker = deserialized_message.get("EOF_MARKER", False)
            disconnect_marker = deserialized_message.get("DISCONNECT", False)
            operation_id = deserialized_message.get("operation_id")
            
            if disconnect_marker:
                # Generate an operation ID for this message
                new_operation_id = self._get_next_message_id()
                
                response_message = Serializer.add_metadata(
                    client_id=client_id,
                    data=None,
                    eof_marker=False,
                    query=None,
                    disconnect_marker=True,
                    operation_id=new_operation_id,
                    node_id=self.node_id
                )
                
                # Send processed data to response queue
                success = self.rabbitmq.publish_to_queue(
                    queue_name=self.response_queue_name,
                    message=Serializer.serialize(response_message),
                    persistent=True
                )
                
                if not success:
                    logging.error("Failed to send processed data to response queue")
            
            elif eof_marker:
                logging.info(f"\033[93mReceived EOF marker for client_id '{client_id}'\033[0m")
                # Generate an operation ID for the EOF message
                new_operation_id = self._get_next_message_id()
                
                # Pass through EOF marker to response queue using add_metadata
                response_message = Serializer.add_metadata(
                    client_id=client_id,
                    data=[],
                    eof_marker=True,
                    query=None,
                    disconnect_marker=False,
                    operation_id=new_operation_id,
                    node_id=self.node_id
                )
                
                self.rabbitmq.publish_to_queue(
                    queue_name=self.response_queue_name,
                    message=Serializer.serialize(response_message),
                    persistent=True
                )
                
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            # Process the movie data for sentiment analysis
            elif data:
                logging.info(f"Processing {len(data)} movies for sentiment analysis")
                processed_data = self._analyze_sentiment_and_calculate_ratios(data)
                
                # Use incremental ID if no operation_id is provided
                if operation_id is None:
                    operation_id = self._get_next_message_id()
                    
                # Prepare response message using the standardized add_metadata method
                response_message = Serializer.add_metadata(
                    client_id=client_id,
                    data=processed_data,
                    eof_marker=False,
                    query=None,
                    disconnect_marker=False,
                    operation_id=operation_id,
                    node_id=self.node_id
                )
                
                # Send processed data to response queue
                success = self.rabbitmq.publish_to_queue(
                    queue_name=self.response_queue_name,
                    message=Serializer.serialize(response_message),
                    persistent=True
                )
                
                if not success:
                    logging.error("Failed to send processed data to response queue")
            else:
                logging.warning(f"Received empty data from client {client_id}")
            
            # Acknowledge message
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

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
            self.rabbitmq.close()
