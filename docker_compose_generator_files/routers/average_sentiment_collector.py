from docker_compose_generator_files.constants import NETWORK

def generate_average_sentiment_collector_router(num_avg_sentiment_workers=2, network=NETWORK):
    """
    Generate the average_sentiment_collector_router service configuration for Docker Compose.
    
    Args:
        num_avg_sentiment_workers (int): Number of average_sentiment workers feeding into this collector
                               (used to set NUMBER_OF_PRODUCER_WORKERS)
        network (str): Name of the Docker network to use
        
    Returns:
        dict: Dictionary with average_sentiment_collector_router service configuration
    """
    # Base port for sentinel monitoring
    base_port = 9991
    
    return {
        "average_sentiment_collector_router": {
            "build": {
                "context": "./server",
                "dockerfile": "router/Dockerfile"
            },
            "env_file": ["./server/router/.env"],
            "ports": [
                f"{base_port}:{base_port}"
            ],
            "environment": [
                f"NUMBER_OF_PRODUCER_WORKERS={num_avg_sentiment_workers}",
                "INPUT_QUEUE=average_sentiment_collector_router",
                "OUTPUT_QUEUES=collector_average_sentiment_worker",
                "BALANCER_TYPE=round_robin",
                f"SENTINEL_PORT={base_port}"
            ],
            "depends_on": {
                "rabbitmq": {
                    "condition": "service_healthy"
                }
            },
            "volumes": [
                "./server/router:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common",
                "./server/persistence/average_sentiment_collector_router:/app/persistence"
            ],
            "networks": [network]
        }
    }

def get_router_host_and_port():
    """
    Get the hostname and port for the average_sentiment_collector_router.
    
    Returns:
        tuple: (hostname, port)
    """
    return "average_sentiment_collector_router", 9991
