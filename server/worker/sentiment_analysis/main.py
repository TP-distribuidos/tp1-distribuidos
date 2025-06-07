import logging
import os
from dotenv import load_dotenv
from Worker import SentimentWorker

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)

def main():
    """Main entry point for the sentiment analysis worker service"""
    # Load environment variables
    load_dotenv()
    
    # Get configuration from environment variables
    consumer_queue = os.getenv("ROUTER_CONSUME_QUEUE")
    producer_queue = os.getenv("ROUTER_PRODUCER_QUEUE")
    
    try:
        # Create worker with the environment configuration
        worker = SentimentWorker(
            consumer_queue_name=consumer_queue,
            response_queue_name=producer_queue
        )
        
        success = worker.run()
        
        if success:
            logging.info("Sentiment worker completed successfully")
        else:
            logging.error("Sentiment worker failed to run properly")
            
    except Exception as e:
        logging.error(f"Error running sentiment worker: {e}")
        raise


if __name__ == "__main__":
    logging.info("Starting sentiment analysis worker service...")
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Worker stopped by user")
    except Exception as e:
        logging.error(f"Error in main: {e}")
