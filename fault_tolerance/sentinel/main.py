import logging
from sentinel import Sentinel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)

def main():
    """
    Main entry point for the sentinel application
    """
    sentinel = Sentinel()
    sentinel.run()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Sentinel stopped by user")
    except Exception as e:
        logging.error(f"Fatal error in main: {e}")
        exit(1)