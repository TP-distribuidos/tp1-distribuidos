from docker_compose_generator_files.constants import NETWORK

def generate_top_10_actors_collector_router(num_top_workers=3, network=NETWORK):
    """
    Generate the top_10_actors_collector_router service configuration for Docker Compose.
    
    Args:
        num_top_workers (int): Number of top workers feeding into this collector
                           (used to set NUMBER_OF_PRODUCER_WORKERS)
        network (str): Name of the Docker network to use
        
    Returns:
        dict: Dictionary with top_10_actors_collector_router service configuration
    """
    # Base port for sentinel monitoring
    base_port = 9901
    
    return {
        "top_10_actors_collector_router": {
            "build": {
                "context": "./server",
                "dockerfile": "router/Dockerfile"
            },
            "env_file": ["./server/router/.env"],
            "ports": [
                f"{base_port}:{base_port}"
            ],
            "environment": [
                f"NUMBER_OF_PRODUCER_WORKERS={num_top_workers}",
                "INPUT_QUEUE=top_10_actors_collector_router",
                "OUTPUT_QUEUES=collector_top_10_actors_worker",
                "BALANCER_TYPE=round_robin",
                f"SENTINEL_PORT={base_port}"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/router:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common",
                "./server/persistence/top_10_actors_collector_router:/app/persistence"
            ],
            "networks": [network]
        }
    }

def get_router_host_and_port():
    """
    Get the hostname and port for the top_10_actors_collector_router.
    
    Returns:
        tuple: (hostname, port)
    """
    return "top_10_actors_collector_router", 9901
