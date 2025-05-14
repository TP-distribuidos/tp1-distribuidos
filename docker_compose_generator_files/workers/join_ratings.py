from docker_compose_generator_files.constants import NETWORK

def generate_worker_queue_names(num_workers=2):
    """
    Generate queue names for join_ratings workers.
    
    Args:
        num_workers (int): Number of join_ratings workers
        
    Returns:
        dict: Dictionary containing lists of queue names for the workers
    """
    movies_queues = [f"join_ratings_worker_{i}_movies" for i in range(1, num_workers + 1)]
    ratings_queues = [f"join_ratings_worker_{i}_ratings" for i in range(1, num_workers + 1)]
    
    return {
        "movies": movies_queues,
        "ratings": ratings_queues
    }

def generate_join_ratings_workers(num_workers=2, network=NETWORK):
    """
    Generate join_ratings worker services configuration for Docker Compose.
    
    Args:
        num_workers (int): Number of join_ratings workers to create
        network (str): Name of the Docker network to use
        
    Returns:
        dict: Dictionary with join_ratings worker service configurations
    """
    services = {}
    
    # Base port for sentinel monitoring
    base_port = 9401
    
    for i in range(1, num_workers + 1):
        # Calculate unique port for each worker
        worker_port = base_port + (i - 1) * 10
        
        services[f"join_ratings_worker_{i}"] = {
            "build": {
                "context": "./server",
                "dockerfile": "worker/join_ratings/Dockerfile"
            },
            "ports": [
                f"{worker_port}:{worker_port}"
            ],
            "env_file": ["./server/worker/join_ratings/.env"],
            "environment": [
                f"ROUTER_CONSUME_QUEUE_MOVIES=join_ratings_worker_{i}_movies",
                f"ROUTER_CONSUME_QUEUE_RATINGS=join_ratings_worker_{i}_ratings",
                "ROUTER_PRODUCER_QUEUE=average_movies_by_rating_router",
                f"SENTINEL_PORT={worker_port}"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/worker/join_ratings:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ],
            "networks": [network]
        }
        
    return services

def get_worker_hosts_and_ports(num_workers=2):
    """
    Get the hostnames and ports for the join_ratings workers.
    
    Args:
        num_workers (int): Number of join_ratings workers
        
    Returns:
        tuple: (list of hostnames, list of ports)
    """
    base_port = 9401
    hosts = [f"join_ratings_worker_{i}" for i in range(1, num_workers + 1)]
    ports = [base_port + (i - 1) * 10 for i in range(1, num_workers + 1)]
    
    return hosts, ports
