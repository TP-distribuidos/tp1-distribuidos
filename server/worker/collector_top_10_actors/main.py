import logging
import os
from dotenv import load_dotenv
from Worker import Worker

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)

def main():
    """Main entry point for the worker service"""
    # Load environment variables
    load_dotenv()
    
    # Get configuration from environment variables
    consumer_queue = os.getenv("ROUTER_CONSUME_QUEUE")
    response_queue = os.getenv("RESPONSE_QUEUE")
    
    if not consumer_queue or not response_queue:
        logging.error("Environment variables for queues are not set properly.")
        return

    try:
        # Create worker with the environment configuration
        worker = Worker(
            consumer_queue_name=consumer_queue,
            producer_queue_name=[response_queue],
        )
        
        # Run the worker (this will block until shutdown)
        success = worker.run()
        
        if success:
            logging.info("Worker completed successfully")
        else:
            logging.error("Worker failed to run properly")
            
    except Exception as e:
        logging.error(f"Error running worker: {e}")
        raise


if __name__ == "__main__":
    logging.debug("Starting top actors worker service...")
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Worker stopped by user")
    except Exception as e:
        logging.error(f"Error in main: {e}")
