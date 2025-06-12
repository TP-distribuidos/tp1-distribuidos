import logging
import os
import signal
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
    producer_queue = os.getenv("ROUTER_PRODUCER_QUEUE")
    producer_exchange = os.getenv("PRODUCER_EXCHANGE", "filtered_data_exchange")
    producer_exchange_type = os.getenv("PRODUCER_EXCHANGE_TYPE", "direct")
    
    if not consumer_queue or not producer_queue:
        logging.error("Environment variables for queues are not set properly.")
        return

    # Create worker with the environment configuration
    worker = Worker(
        consumer_queue_names=[consumer_queue],
        producer_queue_name=producer_queue,
        exchange_name_producer=producer_exchange,
        exchange_type_producer=producer_exchange_type
    )
    
    # Setup clean shutdown with signal handlers
    signal.signal(signal.SIGINT, lambda s, f: worker._handle_shutdown())
    signal.signal(signal.SIGTERM, lambda s, f: worker._handle_shutdown())
    
    # Run the worker (blocking call)
    worker.run()

if __name__ == "__main__":
    logging.info("Starting filter_by_year worker service...")
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Worker stopped by user")
    except Exception as e:
        logging.error(f"Error in main: {e}")
