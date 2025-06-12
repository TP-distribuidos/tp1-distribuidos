import logging
import signal
from Worker import ConsumerWorker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)

def main():
    # Create worker
    worker = ConsumerWorker()
    
    # Setup clean shutdown with signal handlers
    signal.signal(signal.SIGINT, lambda s, f: worker._handle_shutdown())
    signal.signal(signal.SIGTERM, lambda s, f: worker._handle_shutdown())
    
    # Run the worker (blocking call)
    worker.run()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Consumer worker stopped by user")
    except Exception as e:
        logging.error(f"Error in main: {e}")
