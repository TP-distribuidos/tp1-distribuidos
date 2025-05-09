import logging
import sys
import signal  # Added
from sentinel import Sentinel

# Global instance for signal handler
sentinel_instance = None  # Added

# Added signal handler function
def signal_handler(sig, frame):
    global sentinel_instance
    logging.info(f"Signal {sig} received. Initiating graceful shutdown...")
    if sentinel_instance:
        sentinel_instance.shutdown()
    # sys.exit(0) # Exit after shutdown is handled by the main loop completion

def main():
    global sentinel_instance  # Added
    try:
        sentinel_instance = Sentinel()  # Assign to global
        sentinel_instance.run()
    except Exception as e:
        logging.error(f"Fatal error in sentinel: {e}", exc_info=True)
        sys.exit(1)
    finally:
        logging.info("Sentinel process exiting.")


if __name__ == "__main__":
    # Register signal handlers
    signal.signal(signal.SIGTERM, signal_handler)  # Added
    signal.signal(signal.SIGINT, signal_handler)   # Added
    
    # Removed try-except KeyboardInterrupt from here
    main()