from docker_compose_generator_files.constants import NETWORK

def generate_max_min_router(num_avg_rating_workers, num_max_min_workers=2, network=NETWORK):
    """
    Generate the max_min_router service configuration for Docker Compose.
    
    Args:
        num_avg_rating_workers (int): Total number of average_movies_by_rating workers
                                    (used to set NUMBER_OF_PRODUCER_WORKERS)
        num_max_min_workers (int): Number of max_min workers to distribute to
        network (str): Name of the Docker network to use
        
    Returns:
        dict: Dictionary with max_min_router service configuration
    """
    # Create the output queues configuration - each max_min worker is in its own shard
    output_queues = '['
    for i in range(1, num_max_min_workers+1):
        if i > 1:
            output_queues += ','
        output_queues += f'["max_min_worker_{i}"]'
    output_queues += ']'
    
    # Base port for sentinel monitoring
    base_port = 9601
    
    return {
        "max_min_router": {
            "build": {
                "context": "./server",
                "dockerfile": "router/Dockerfile"
            },
            "env_file": ["./server/router/.env"],
            "ports": [
                f"{base_port}:{base_port}"
            ],
            "environment": [
                f"NUMBER_OF_PRODUCER_WORKERS={num_avg_rating_workers}",
                "INPUT_QUEUE=max_min_router",
                f"OUTPUT_QUEUES={output_queues}",
                "BALANCER_TYPE=shard_by_ascii",
                f"SENTINEL_PORT={base_port}"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/router:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common",
                "./server/wal/max_min_router:/app/wal"
            ],
            "networks": [network]
        }
    }

def get_router_host_and_port():
    """
    Get the hostname and port for the max_min_router.
    
    Returns:
        tuple: (hostname, port)
    """
    return "max_min_router", 9601