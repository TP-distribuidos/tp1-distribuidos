from docker_compose_generator_files.constants import NETWORK

def generate_worker_queue_names(num_workers=2):
    """
    Generate queue names for join_credits workers.
    
    Args:
        num_workers (int): Number of join_credits workers
        
    Returns:
        dict: Dictionary containing lists of queue names for the workers
    """
    movies_queues = [f"join_credits_worker_{i}_movies" for i in range(1, num_workers + 1)]
    credits_queues = [f"join_credits_worker_{i}_credits" for i in range(1, num_workers + 1)]
    
    return {
        "movies": movies_queues,
        "credits": credits_queues
    }

def generate_join_credits_workers(num_workers=2, network=NETWORK):
    """
    Generate join_credits worker services configuration for Docker Compose.
    
    Args:
        num_workers (int): Number of join_credits workers to create
        network (str): Name of the Docker network to use
        
    Returns:
        dict: Dictionary with join_credits worker service configurations
    """
    services = {}
    
    # Base port for sentinel monitoring
    base_port = 9508
    
    for i in range(1, num_workers + 1):
        # Calculate unique port for each worker
        worker_port = base_port + (i - 1) * 10
        worker_name = f"join_credits_worker_{i}"
        
        services[worker_name] = {
            "build": {
                "context": "./server",
                "dockerfile": "worker/join_credits/Dockerfile"
            },
            "ports": [
                f"{worker_port}:{worker_port}"
            ],
            "env_file": ["./server/worker/join_credits/.env"],
            "environment": [
                f"ROUTER_CONSUME_QUEUE_MOVIES={worker_name}_movies",
                f"ROUTER_CONSUME_QUEUE_CREDITS={worker_name}_credits",
                "ROUTER_PRODUCER_QUEUE=count_router",
                f"SENTINEL_PORT={worker_port}",
                f"NODE_ID={worker_name}_node"
            ],
            "depends_on": {
                "rabbitmq": {
                    "condition": "service_healthy"
                }
            },
            "volumes": [
                "./server/worker/join_credits:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common",
                f"./server/persistence/join_credits_worker_{i}:/app/persistence"
            ],
            "networks": [network]
        }
        
    return services

def get_worker_hosts_and_ports(num_workers=2):
    """
    Get the hostnames and ports for the join_credits workers.
    
    Args:
        num_workers (int): Number of join_credits workers
        
    Returns:
        tuple: (list of hostnames, list of ports)
    """
    base_port = 9508
    hosts = [f"join_credits_worker_{i}" for i in range(1, num_workers + 1)]
    ports = [base_port + (i - 1) * 10 for i in range(1, num_workers + 1)]
    
    return hosts, ports
