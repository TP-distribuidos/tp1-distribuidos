from docker_compose_generator_files.workers.join_credits import generate_worker_queue_names as generate_join_credits_queues
from docker_compose_generator_files.workers.join_ratings import generate_worker_queue_names as generate_join_ratings_queues
from docker_compose_generator_files.constants import NETWORK

def generate_join_movies_router(num_country_workers=2, num_join_credits_workers=2, num_join_ratings_workers=2, network=NETWORK):
    """
    Generate the join_movies_router service configuration for Docker Compose.
    
    Args:
        num_country_workers (int): Number of filter_by_country workers
        num_join_credits_workers (int): Number of join_credits workers
        num_join_ratings_workers (int): Number of join_ratings workers
        network (str): Name of the Docker network to use
        
    Returns:
        dict: Dictionary with join_movies_router service configuration
    """
    # Get the queue names for join_credits workers
    join_credits_queues = generate_join_credits_queues(num_join_credits_workers)
    movies_queues_credits = join_credits_queues["movies"]
    
    # Get the queue names for join_ratings workers
    join_ratings_queues = generate_join_ratings_queues(num_join_ratings_workers)
    movies_queues_ratings = join_ratings_queues["movies"]
    
    # Combine all output queues
    all_output_queues = movies_queues_ratings + movies_queues_credits
    output_queues = ",".join(all_output_queues)
    
    # Base port for sentinel monitoring
    base_port = 9351
    
    return {
        "join_movies_router": {
            "build": {
                "context": "./server",
                "dockerfile": "router/Dockerfile"
            },
            "env_file": ["./server/router/.env"],
            "ports": [
                f"{base_port}:{base_port}"
            ],
            "environment": [
                f"NUMBER_OF_PRODUCER_WORKERS={num_country_workers}",
                "INPUT_QUEUE=join_movies_router",
                f"OUTPUT_QUEUES={output_queues}",
                "EXCHANGE_TYPE=fanout",
                "EXCHANGE_NAME=join_router_exchange",
                f"SENTINEL_PORT={base_port}"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/router:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ],
            "networks": [network]
        }
    }

def get_router_host_and_port():
    """
    Get the hostname and port for the join_movies_router.
    
    Returns:
        tuple: (hostname, port)
    """
    return "join_movies_router", 9351
