import asyncio
import logging
import signal
from Worker import ConsumerWorker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)

async def main():
    # Create worker
    worker = ConsumerWorker()
    
    # Setup clean shutdown
    loop = asyncio.get_event_loop()
    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
    for s in signals:
        loop.add_signal_handler(
            s, lambda s=s: asyncio.create_task(shutdown(s, worker, loop))
        )
    
    # Run the worker
    await worker.run()

async def shutdown(signal, worker, loop):
    """Clean shutdown of worker and event loop"""
    logging.info(f"Received exit signal {signal.name}...")
    logging.info("Shutting down...")
    
    # Stop the worker gracefully
    worker._running = False
    
    try:
        # Call worker cleanup explicitly
        await worker.cleanup()
    except Exception as e:
        logging.error(f"Error during worker cleanup: {e}")
    
    # Give tasks time to complete
    await asyncio.sleep(0.5)
    
    try:
        # Stop remaining tasks
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        if tasks:
            logging.info(f"Cancelling {len(tasks)} remaining tasks")
            for task in tasks:
                task.cancel()
            
            await asyncio.gather(*tasks, return_exceptions=True)
    except Exception as e:
        logging.error(f"Error during task cleanup: {e}")
    
    # Stop the event loop
    try:
        loop.stop()
    except Exception as e:
        logging.error(f"Error stopping event loop: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Consumer worker stopped by user")
    except Exception as e:
        logging.error(f"Error in main: {e}")