from docker_compose_generator_files.constants import NETWORK

def generate_top_router(num_count_workers=4, num_top_workers=3, network=NETWORK):
    """
    Generate the top_router service configuration for Docker Compose.
    
    Args:
        num_count_workers (int): Total number of count workers feeding into this router
                              (used to set NUMBER_OF_PRODUCER_WORKERS)
        num_top_workers (int): Number of top workers to distribute to
        network (str): Name of the Docker network to use
        
    Returns:
        dict: Dictionary with top_router service configuration
    """
    # Create the output queues configuration - typically each top worker is in its own shard
    # for proper actor distribution
    output_queues = '['
    for i in range(1, num_top_workers+1):
        if i > 1:
            output_queues += ','
        output_queues += f'["top_worker_{i}"]'
    output_queues += ']'
    
    # Base port for sentinel monitoring
    base_port = 9801
    
    return {
        "top_router": {
            "build": {
                "context": "./server",
                "dockerfile": "router/Dockerfile"
            },
            "env_file": ["./server/router/.env"],
            "ports": [
                f"{base_port}:{base_port}"
            ],
            "environment": [
                f"NUMBER_OF_PRODUCER_WORKERS={num_count_workers}",
                "INPUT_QUEUE=top_router",
                f"OUTPUT_QUEUES={output_queues}",
                "BALANCER_TYPE=shard_by_ascii",
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
                "./server/persistence/top_router:/app/persistence"
            ],
            "networks": [network]
        }
    }

def get_router_host_and_port():
    """
    Get the hostname and port for the top_router.
    
    Returns:
        tuple: (hostname, port)
    """
    return "top_router", 9801
