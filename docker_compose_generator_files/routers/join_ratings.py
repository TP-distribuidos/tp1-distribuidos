from docker_compose_generator_files.workers.join_ratings import generate_worker_queue_names
from docker_compose_generator_files.constants import NETWORK

def generate_join_ratings_router(num_join_ratings_workers=2, network=NETWORK):
    """
    Generate the join_ratings_router service configuration for Docker Compose.
    
    Args:
        num_join_ratings_workers (int): Number of join_ratings workers
        network (str): Name of the Docker network to use
        
    Returns:
        dict: Dictionary with join_ratings_router service configuration
    """
    # Get the ratings queue names for the join_ratings workers
    queue_names = generate_worker_queue_names(num_join_ratings_workers)
    ratings_queues = queue_names["ratings"]
    
    # Create the output queues string for join_ratings_router
    output_queues = ",".join(ratings_queues)
    
    # Base port for sentinel monitoring
    base_port = 9401
    
    return {
        "join_ratings_router": {
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
                "INPUT_QUEUE=boundary_ratings_router",
                f"OUTPUT_QUEUES={output_queues}",
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
                "./server/persistence/join_ratings_router:/app/persistence"
            ],
            "networks": [network]
        }
    }

def get_router_host_and_port():
    """
    Get the hostname and port for the join_ratings_router.
    
    Returns:
        tuple: (hostname, port)
    """
    return "join_ratings_router", 9401
