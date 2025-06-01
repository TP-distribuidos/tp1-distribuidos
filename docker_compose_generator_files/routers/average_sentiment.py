from docker_compose_generator_files.constants import NETWORK

def generate_average_sentiment_router(num_sentiment_workers=2, num_avg_sentiment_workers=2, network=NETWORK):
    """
    Generate the average_sentiment_router service configuration for Docker Compose.
    
    Args:
        num_sentiment_workers (int): Number of sentiment analysis workers feeding into this router
                               (used to set NUMBER_OF_PRODUCER_WORKERS)
        num_avg_sentiment_workers (int): Number of average sentiment worker destinations
        network (str): Name of the Docker network to use
        
    Returns:
        dict: Dictionary with average_sentiment_router service configuration
    """
    # Create the output queues configuration for sharding - use the configurable number of workers
    output_queues = '[['
    for i in range(1, num_avg_sentiment_workers + 1):
        if i > 1:
            output_queues += '],['
        output_queues += f'"average_sentiment_worker_{i}"'
    output_queues += ']]'
    
    # Base port for sentinel monitoring
    base_port = 9981
    
    return {
        "average_sentiment_router": {
            "build": {
                "context": "./server",
                "dockerfile": "router/Dockerfile"
            },
            "env_file": ["./server/router/.env"],
            "ports": [
                f"{base_port}:{base_port}"
            ],
            "environment": [
                f"NUMBER_OF_PRODUCER_WORKERS={num_sentiment_workers}",
                "INPUT_QUEUE=average_sentiment_router",
                f"OUTPUT_QUEUES={output_queues}",
                "BALANCER_TYPE=shard_by_ascii",
                f"SENTINEL_PORT={base_port}"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/router:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common",
                "./server/persistence/average_sentiment_router:/app/persistence"
            ],
            "networks": [network]
        }
    }

def get_router_host_and_port():
    """
    Get the hostname and port for the average_sentiment_router.
    
    Returns:
        tuple: (hostname, port)
    """
    return "average_sentiment_router", 9981
