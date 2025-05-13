import asyncio
import logging
import os
import signal
import sys
import uuid

# Add parent directory to Python path to allow imports from sibling directories
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from rabbitmq.Rabbitmq_client import RabbitMQClient
from common.Serializer import Serializer
from common.SentinelBeacon import SentinelBeacon
from common.WriteAheadLog import WriteAheadLog, WALEntryStatus

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)

# Get environment variables
CONSUMER_QUEUE = os.getenv("CONSUMER_QUEUE", "test_queue")
SENTINEL_PORT = int(os.getenv("SENTINEL_PORT", 9002))
OUTPUT_FILE = os.getenv("OUTPUT_FILE", "/app/output/received_messages.txt")
WAL_DIR = os.getenv("WAL_DIR", "/app/wal")

def process_data(content):
    """Procesa los datos de un mensaje (capitaliza el texto)"""
    if not content:
        return ""
    
    # Capitalizar el texto como procesamiento simple
    return content.upper()

class ConsumerWorker:
    def __init__(self, consumer_queue=CONSUMER_QUEUE):
        self._running = True
        self.consumer_queue = consumer_queue
        self.rabbitmq = RabbitMQClient()
        
        # Create output directory if it doesn't exist
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        
        # Initialize Write Ahead Log
        self.wal = WriteAheadLog(log_dir=WAL_DIR, max_entries=10000, auto_cleanup=True)
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
        # Initialize sentinel beacon
        self.sentinel_beacon = SentinelBeacon(SENTINEL_PORT, "Consumer Worker")
        
        logging.info(f"Consumer Worker initialized to consume from queue '{consumer_queue}'")
    
    async def run(self):
        """Run the worker, connecting to RabbitMQ and consuming messages"""
        try:
            # Connect to RabbitMQ
            if not await self._setup_rabbitmq():
                logging.error("Failed to set up RabbitMQ connection. Exiting.")
                return False
            
            # Recuperar y procesar entradas pendientes del WAL
            logging.info("Checking for pending entries in WAL...")
            await self._recover_pending_entries()
            
            logging.info(f"Consumer Worker running and consuming from queue '{self.consumer_queue}'")
            
            # Keep the worker running until shutdown is triggered
            while self._running:
                await asyncio.sleep(1)
            
            return True
        finally:
            # Always clean up resources
            await self.cleanup()
            
    async def _recover_pending_entries(self):
        """Recupera y procesa entradas pendientes del WAL"""
        pending_entries = await self.wal.get_pending_entries()
        
        if pending_entries:
            logging.info(f"Found {len(pending_entries)} pending entries in WAL. Processing...")
            for entry in pending_entries:
                try:
                    # Procesar la entrada pendiente
                    await self._process_wal_entry(entry)
                except Exception as e:
                    logging.error(f"Error processing pending WAL entry {entry.id}: {e}")
    
    async def cleanup(self):
        """Clean up resources properly"""
        logging.info("Cleaning up resources...")
        if hasattr(self, 'rabbitmq'):
            try:
                await self.rabbitmq.close()
                logging.info("RabbitMQ connection closed")
            except Exception as e:
                logging.error(f"Error closing RabbitMQ connection: {e}")
                
        # Limpieza del WAL (entradas completadas)
        try:
            # Limpieza de entradas completadas más antiguas de 24 horas
            await self.wal.cleanup_completed(24)
        except Exception as e:
            logging.error(f"Error cleaning up WAL: {e}")
    
    async def _setup_rabbitmq(self, retry_count=1):
        """Set up RabbitMQ connection and consumer"""
        # Connect to RabbitMQ
        connected = await self.rabbitmq.connect()
        if not connected:
            logging.error(f"Failed to connect to RabbitMQ, retrying in {retry_count} seconds...")
            wait_time = min(30, 2 ** retry_count)
            await asyncio.sleep(wait_time)
            return await self._setup_rabbitmq(retry_count + 1)
        
        # Declare the consumer queue
        queue = await self.rabbitmq.declare_queue(self.consumer_queue, durable=True)
        if not queue:
            logging.error(f"Failed to declare queue '{self.consumer_queue}'")
            return False
        
        # Set up consumer
        success = await self.rabbitmq.consume(
            queue_name=self.consumer_queue,
            callback=self._process_message,
            no_ack=False
        )
        if not success:
            logging.error(f"Failed to set up consumer for queue '{self.consumer_queue}'")
            return False
            
        return True
    
    async def _process_message(self, message):
        """Process a message from the queue"""
        message_id = str(uuid.uuid4())  # Generar un ID único para esta operación
        
        try:
            # Paso 1: Deserializar el mensaje
            deserialized_message = Serializer.deserialize(message.body)
            
            # Log the received message
            batch = deserialized_message.get('batch')
            content = deserialized_message.get('content')
            logging.info(f"Received message {message_id} - Batch: {batch}")
            
            # Paso 2: Registrar en el WAL como PENDING
            message_data = {
                'batch': batch,
                'content': content
            }
            wal_entry = await self.wal.append(message_id, message_data)
            logging.info(f"Registered message in WAL with ID: {message_id}")
            
            # Paso 3: Actualizar estado a PROCESSING
            await self.wal.update_status(message_id, WALEntryStatus.PROCESSING)
            logging.info(f"Updated WAL entry {message_id} to PROCESSING")
            
            # Paso 4: Procesar los datos (capitalizar)
            processed_data = process_data(content)
            logging.info(f"Processed message data: {len(processed_data)} chars")
            
            # Paso 5: Escribir a disco
            formatted_message = f"--- Message {batch} ---\n"
            formatted_message += f"{processed_data}\n\n"
            
            # Escribir a archivo de forma asíncrona
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._write_to_file_sync, formatted_message)
            logging.info(f"Wrote processed message {message_id} to file")
            
            # Paso 6: Aquí iría el envío a la siguiente cola si fuera necesario
            # await self.rabbitmq.publish_to_queue("next_queue", processed_data)
            
            # Paso 7: Actualizar estado a COMPLETED y confirmar mensaje
            await self.wal.update_status(message_id, WALEntryStatus.COMPLETED)
            await message.ack()
            logging.info(f"Completed processing message {message_id}")
            
        except Exception as e:
            logging.error(f"Error processing message {message_id}: {e}")
            # Si ya hemos registrado en el WAL, marcar como fallido
            if 'message_id' in locals():
                try:
                    await self.wal.update_status(message_id, WALEntryStatus.FAILED)
                except Exception as wal_error:
                    logging.error(f"Error updating WAL status: {wal_error}")
            
            # Rechazar el mensaje y ponerlo de nuevo en la cola
            await message.reject(requeue=True)
    
    
    
    def _write_to_file_sync(self, data):
        """Synchronous file write operation"""
        with open(OUTPUT_FILE, 'a') as f:
            f.write(data)
    
    def _handle_shutdown(self, *_):
        """Handle shutdown signals"""
        logging.info("Shutting down consumer worker...")
        self._running = False
        
        # Shut down the sentinel beacon
        if hasattr(self, 'sentinel_beacon'):
            self.sentinel_beacon.shutdown()
    
    async def _process_wal_entry(self, entry):
        """Procesa una entrada del WAL"""
        try:
            # Marcar como en procesamiento
            await self.wal.update_status(entry.id, WALEntryStatus.PROCESSING)
            logging.info(f"Processing WAL entry {entry.id} - Batch: {entry.batch}")
            
            # Extraer datos y procesar
            message_content = entry.content.get('content', '')
            processed_data = process_data(message_content)
            
            # Escribir a disco
            formatted_message = f"--- Message {entry.batch} (recovered) ---\n"
            formatted_message += f"{processed_data}\n\n"
            
            # Escribir a archivo de forma asíncrona
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._write_to_file_sync, formatted_message)
            
            # Marcar como completado
            await self.wal.update_status(entry.id, WALEntryStatus.COMPLETED)
            logging.info(f"Completed processing WAL entry {entry.id}")
            
        except Exception as e:
            # Marcar como fallido
            await self.wal.update_status(entry.id, WALEntryStatus.FAILED)
            logging.error(f"Failed to process WAL entry {entry.id}: {e}")
            raise