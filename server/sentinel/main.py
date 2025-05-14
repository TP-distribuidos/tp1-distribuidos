import logging
import sys
import signal
from Sentinel import Sentinel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)

# Global instance for signal handler
sentinel_instance = None

# Signal handler function
def signal_handler(sig, frame):
    global sentinel_instance
    logging.info(f"Signal {sig} received. Initiating graceful shutdown...")
    if sentinel_instance:
        sentinel_instance.shutdown()
    # Explicit exit is not needed here as the main loop will exit gracefully

def main():
    global sentinel_instance
    try:
        sentinel_instance = Sentinel()
        
        # Register signal handlers
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
        
        # Run the sentinel
        sentinel_instance.run()
    except Exception as e:
        logging.error(f"Fatal error in sentinel: {e}", exc_info=True)
        sys.exit(1)
    finally:
        logging.info("Sentinel process exiting.")

if __name__ == "__main__":
    main()
