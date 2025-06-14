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
    movies_consumer_queue = os.getenv("ROUTER_CONSUME_QUEUE_MOVIES")
    credits_consumer_queue = os.getenv("ROUTER_CONSUME_QUEUE_CREDITS")
    producer_queue = os.getenv("ROUTER_PRODUCER_QUEUE")
    producer_exchange = os.getenv("PRODUCER_EXCHANGE", "filtered_data_exchange")
    producer_exchange_type = os.getenv("PRODUCER_EXCHANGE_TYPE", "direct")

    # Create worker with the environment configuration
    worker = Worker(
        consumer_queue_names=[movies_consumer_queue, credits_consumer_queue],
        producer_queue_name=producer_queue,
        exchange_name_producer=producer_exchange,
        exchange_type_producer=producer_exchange_type,
    )
    
    # Run the worker (this is a blocking call)
    success = worker.run()
    
    if success:
        logging.info("Worker completed successfully")
    else:
        logging.error("Worker failed to run properly")


if __name__ == "__main__":
    logging.debug("Starting join_credits worker service...")
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Worker stopped by user")
    except Exception as e:
        logging.error(f"Error in main: {e}")
