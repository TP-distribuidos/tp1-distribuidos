from docker_compose_generator_files.constants import NETWORK

def generate_worker_queue_names(num_workers=2):
    """
    Generate queue names for filter_by_country workers.
    
    Args:
        num_workers (int): Number of filter_by_country workers
        
    Returns:
        list: List of queue names for the workers
    """
    return [f"filter_by_country_worker_{i}" for i in range(1, num_workers + 1)]

def generate_filter_by_country_workers(num_workers=2, network=NETWORK):
    """
    Generate filter_by_country worker services configuration for Docker Compose.
    
    Args:
        num_workers (int): Number of filter_by_country workers to create
        network (str): Name of the Docker network to use
        
    Returns:
        dict: Dictionary with filter_by_country worker service configurations
    """
    services = {}
    
    # Base port for sentinel monitoring
    base_port = 9031
    
    for i in range(1, num_workers + 1):
        # Calculate unique port for each worker
        worker_port = base_port + (i - 1) * 10
        
        services[f"filter_by_country_worker_{i}"] = {
            "build": {
                "context": "./server",
                "dockerfile": "worker/filter_by_country/Dockerfile"
            },
            "depends_on": ["rabbitmq"],
            "ports": [
                f"{worker_port}:{worker_port}"
            ],
            "env_file": ["./server/worker/filter_by_country/.env"],
            "environment": [
                f"ROUTER_CONSUME_QUEUE=filter_by_country_worker_{i}",
                "ROUTER_PRODUCER_QUEUE=join_movies_router",
                f"SENTINEL_PORT={worker_port}"
            ],
            "volumes": [
                "./server/worker/filter_by_country:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ],
            "networks": [network]
        }
        
    return services

def get_worker_hosts_and_ports(num_workers=2):
    """
    Get the hostnames and ports for the filter_by_country workers.
    
    Args:
        num_workers (int): Number of filter_by_country workers
        
    Returns:
        tuple: (list of hostnames, list of ports)
    """
    base_port = 9031
    hosts = [f"filter_by_country_worker_{i}" for i in range(1, num_workers + 1)]
    ports = [base_port + (i - 1) * 10 for i in range(1, num_workers + 1)]
    
    return hosts, ports
