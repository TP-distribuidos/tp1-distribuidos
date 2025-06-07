import logging
import pika
from typing import Optional, Any, Dict, Callable

class RabbitMQClient:
    def __init__(self, host="rabbitmq", port=5672, username="guest", password="guest"):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self._connection: Optional[pika.BlockingConnection] = None
        self._channel: Optional[pika.channel.Channel] = None
        self._consumers = {}

        logging.info(f"RabbitMQ client initialized for {host}:{port}")
    
    def connect(self) -> bool:
        """Establish connection to RabbitMQ server"""
        try:
            if self._connection and self._connection.is_open:
                return True
                
            credentials = pika.PlainCredentials(self.username, self.password)
            parameters = pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                credentials=credentials,
                heartbeat=60
            )
            
            self._connection = pika.BlockingConnection(parameters)
            self._channel = self._connection.channel()
            logging.info(f"Connected to RabbitMQ at {self.host}:{self.port}")
            return True
        except Exception as e:
            logging.error(f"Failed to connect to RabbitMQ: {e}")
            return False
    
    def close(self):
        """Close the RabbitMQ connection"""
        try:
            if self._channel and self._channel.is_open:
                self._channel.close()
                self._channel = None
                
            if self._connection and self._connection.is_open:
                self._connection.close()
                self._connection = None
                
            logging.info("RabbitMQ connection closed")
        except Exception as e:
            logging.error(f"Error closing RabbitMQ connection: {e}")
    
    def declare_exchange(self, name: str, exchange_type='direct', durable=True) -> bool:
        """Declare an exchange"""
        try:
            if not self._channel:
                if not self.connect():
                    return False
                    
            self._channel.exchange_declare(
                exchange=name, 
                exchange_type=exchange_type,
                durable=durable
            )
            logging.info(f"Exchange '{name}' declared")
            return True
        except Exception as e:
            logging.error(f"Failed to declare exchange '{name}': {e}")
            return False
    
    def declare_queue(self, name: str, durable=True) -> bool:
        """Declare a queue"""
        try:
            if not self._channel:
                if not self.connect():
                    return False
                    
            self._channel.queue_declare(
                queue=name,
                durable=durable
            )
            logging.info(f"Queue '{name}' declared")
            return True
        except Exception as e:
            logging.error(f"Failed to declare queue '{name}': {e}")
            return False
    
    def bind_queue(self, queue_name: str, exchange_name: str, routing_key: str) -> bool:
        """Bind queue to exchange with routing key"""
        try:
            if not self._channel:
                if not self.connect():
                    return False
                
            # Ensure exchange and queue exist
            if not self.declare_exchange(exchange_name):
                return False
                
            if not self.declare_queue(queue_name):
                return False
                
            self._channel.queue_bind(
                queue=queue_name,
                exchange=exchange_name,
                routing_key=routing_key
            )
            
            logging.info(f"Queue '{queue_name}' bound to exchange '{exchange_name}' with key '{routing_key}'")
            return True
        except Exception as e:
            logging.error(f"Failed to bind queue '{queue_name}' to exchange '{exchange_name}': {e}")
            return False
    
    def publish(self, exchange_name: str, routing_key: str, message: str, persistent=True) -> bool:
        """Publish message to exchange with routing key"""
        try:
            if not self._channel or not self._connection.is_open:
                if not self.connect():
                    return False
            
            # Ensure exchange exists
            if not self.declare_exchange(exchange_name):
                return False
                
            properties = pika.BasicProperties(
                delivery_mode=2 if persistent else 1  # 2 = persistent, 1 = non-persistent
            )
            
            message_body = message.encode('utf-8') if isinstance(message, str) else message
            
            self._channel.basic_publish(
                exchange=exchange_name,
                routing_key=routing_key,
                body=message_body,
                properties=properties
            )
            
            return True
        except Exception as e:
            logging.error(f"Failed to publish to exchange '{exchange_name}': {e}")
            return False
    
    def consume(self, queue_name: str, callback: Callable, no_ack=False, prefetch_count=1) -> bool:
        """Set up consumer for a queue"""
        try:
            if not self._channel:
                if not self.connect():
                    return False
            
            # Set QoS - FIXED: prefetch_size must be 0 (RabbitMQ doesn't support non-zero values)
            self._channel.basic_qos(prefetch_count=prefetch_count)
                    
            # Ensure queue exists
            if not self.declare_queue(queue_name):
                return False
                
            # Set up consumer with callback
            self._channel.basic_consume(
                queue=queue_name,
                on_message_callback=callback,
                auto_ack=no_ack
            )
            
            self._consumers[queue_name] = True
            logging.info(f"Consumer set up for queue '{queue_name}' with prefetch_count={prefetch_count}")
            return True
        except Exception as e:
            logging.error(f"Failed to set up consumer for queue '{queue_name}': {e}")
            return False
            
    def start_consuming(self):
        """Start consuming messages (blocking call)"""
        try:
            if not self._channel:
                if not self.connect():
                    return False
                    
            logging.info("Starting to consume messages...")
            self._channel.start_consuming()
            return True
        except KeyboardInterrupt:
            logging.info("Keyboard interrupt received, stopping consumer")
            self.stop_consuming()
            return False
        except Exception as e:
            logging.error(f"Error in consuming messages: {e}")
            return False
            
    def stop_consuming(self):
        """Stop consuming messages"""
        try:
            if self._channel and self._channel.is_open:
                self._channel.stop_consuming()
                self._consumers.clear()
                logging.info("Stopped consuming messages")
            return True
        except Exception as e:
            logging.error(f"Error stopping consumer: {e}")
            return False

    def publish_to_queue(self, queue_name: str, message: str, persistent=True) -> bool:
        """Publish message directly to queue using the default exchange"""
        try:
            if not self._channel or not self._connection.is_open:
                if not self.connect():
                    return False
                    
            # Ensure queue exists
            if not self.declare_queue(queue_name):
                return False
                
            properties = pika.BasicProperties(
                delivery_mode=2 if persistent else 1  # 2 = persistent, 1 = non-persistent
            )
            
            message_body = message.encode('utf-8') if isinstance(message, str) else message
            
            # Default exchange is empty string
            self._channel.basic_publish(
                exchange='',
                routing_key=queue_name,  # In default exchange, routing_key = queue_name
                body=message_body,
                properties=properties
            )
            
            return True
        except Exception as e:
            logging.error(f"Failed to publish to queue '{queue_name}': {e}")
            return False
