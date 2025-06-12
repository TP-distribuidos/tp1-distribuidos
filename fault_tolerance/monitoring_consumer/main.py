import logging
import signal
from Worker import MonitoringConsumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)

def main():
    # Create monitoring consumer
    consumer = MonitoringConsumer()
    
    # Setup clean shutdown
    signal.signal(signal.SIGINT, lambda s, f: consumer._handle_shutdown())
    signal.signal(signal.SIGTERM, lambda s, f: consumer._handle_shutdown())
    
    # Run the consumer (blocking call)
    consumer.run()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Monitoring consumer stopped by user")
    except Exception as e:
        logging.error(f"Error in main: {e}")
