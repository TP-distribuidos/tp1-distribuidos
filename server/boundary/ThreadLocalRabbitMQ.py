import threading
from rabbitmq.Rabbitmq_client import RabbitMQClient

class ThreadLocalRabbitMQ:
    """Thread-safe RabbitMQ client that maintains separate connections per thread"""
    def __init__(self, host="rabbitmq", port=5672, username="guest", password="guest"):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self._local = threading.local()
    
    @property
    def client(self):
        """Get or create a thread-local RabbitMQ client"""
        if not hasattr(self._local, 'rabbitmq'):
            # Create a new connection for this thread
            self._local.rabbitmq = RabbitMQClient(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password
            )
            self._local.rabbitmq.connect()
        return self._local.rabbitmq
    
    def close_all(self):
        """Close the current thread's connection if it exists"""
        if hasattr(self._local, 'rabbitmq'):
            try:
                self._local.rabbitmq.close()
            except:
                pass  # Ignore errors during shutdown
