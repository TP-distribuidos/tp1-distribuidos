from docker_compose_generator_files.constants import NETWORK

def generate_movies_q5_router(num_sentiment_workers=2, network=NETWORK):
    """
    Generate the movies_q5_router service configuration for Docker Compose.
    
    Args:
        num_sentiment_workers (int): Number of sentiment analysis workers to route to
        network (str): Name of the Docker network to use
        
    Returns:
        dict: Dictionary with movies_q5_router service configuration
    """
    # Dynamically generate the list of worker queues
    output_queues = ','.join([f"sentiment_analysis_worker_{i}" for i in range(1, num_sentiment_workers + 1)])
    
    # Base port for sentinel monitoring
    base_port = 9951
    
    return {
        "movies_q5_router": {
            "build": {
                "context": "./server",
                "dockerfile": "router/Dockerfile"
            },
            "env_file": ["./server/router/.env"],
            "ports": [
                f"{base_port}:{base_port}"
            ],
            "environment": [
                "NUMBER_OF_PRODUCER_WORKERS=1",
                "INPUT_QUEUE=boundary_movies_Q5_router",
                f"OUTPUT_QUEUES={output_queues}",
                "BALANCER_TYPE=round_robin",
                f"SENTINEL_PORT={base_port}"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/router:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common",
                "./server/wal/movies_q5_router:/app/wal"
            ],
            "networks": [network]
        }
    }

def get_router_host_and_port():
    """
    Get the hostname and port for the movies_q5_router.
    
    Returns:
        tuple: (hostname, port)
    """
    return "movies_q5_router", 9951
