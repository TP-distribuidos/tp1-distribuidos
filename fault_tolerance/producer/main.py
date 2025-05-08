import asyncio
import logging
from Worker import ProducerWorker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)

async def main():
    worker = ProducerWorker()
    await worker.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Producer worker stopped by user")
    except Exception as e:
        logging.error(f"Error in main: {e}")