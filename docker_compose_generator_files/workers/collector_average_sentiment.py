from docker_compose_generator_files.constants import NETWORK

def generate_collector_average_sentiment_worker(network=NETWORK):
    """
    Generate the collector_average_sentiment_worker service configuration for Docker Compose.
    
    Args:
        network (str): Name of the Docker network to use
        
    Returns:
        dict: Dictionary with collector_average_sentiment_worker service configuration
    """
    # Base port for sentinel monitoring
    worker_port = 9084
    
    return {
        "collector_average_sentiment_worker": {
            "build": {
                "context": "./server",
                "dockerfile": "worker/collector_average_sentiment_worker/Dockerfile"
            },
            "depends_on": ["rabbitmq"],
            "ports": [
                f"{worker_port}:{worker_port}"
            ],
            "env_file": ["./server/worker/collector_average_sentiment_worker/.env"],
            "environment": [
                "ROUTER_CONSUME_QUEUE=collector_average_sentiment_worker",
                "RESPONSE_QUEUE=response_queue",
                f"SENTINEL_PORT={worker_port}"
            ],
            "volumes": [
                "./server/worker/collector_average_sentiment_worker:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ],
            "networks": [network]
        }
    }

def get_worker_host_and_port():
    """
    Get the hostname and port for the collector_average_sentiment_worker.
    
    Returns:
        tuple: (hostname, port)
    """
    return "collector_average_sentiment_worker", 9084
