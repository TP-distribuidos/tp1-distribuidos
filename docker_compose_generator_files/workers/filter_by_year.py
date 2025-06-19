from docker_compose_generator_files.constants import NETWORK

def generate_worker_queue_names(num_workers=2):
    """
    Generate queue names for filter_by_year workers.
    
    Args:
        num_workers (int): Number of filter_by_year workers
        
    Returns:
        list: List of queue names for the workers
    """
    return [f"filter_by_year_worker_{i}" for i in range(1, num_workers + 1)]

def generate_filter_by_year_workers(num_workers=2, network=NETWORK):
    """
    Generate filter_by_year worker services configuration for Docker Compose.
    
    Args:
        num_workers (int): Number of filter_by_year workers to create
        network (str): Name of the Docker network to use
        
    Returns:
        dict: Dictionary with filter_by_year worker service configurations
    """
    services = {}
    
    base_port = 9007
    
    for i in range(1, num_workers + 1):
        # Calculate unique port for each worker
        worker_port = base_port + (i - 1) * 10
        worker_name = f"filter_by_year_worker_{i}"
        
        services[worker_name] = {
            "build": {
                "context": "./server",
                "dockerfile": "worker/filter_by_year/Dockerfile"
            },
            "depends_on": {
                "rabbitmq": {
                    "condition": "service_healthy"
                }
            },
            "ports": [
                f"{worker_port}:{worker_port}"
            ],
            "env_file": ["./server/worker/filter_by_year/.env"],
            "environment": [
                f"ROUTER_CONSUME_QUEUE={worker_name}",
                "ROUTER_PRODUCER_QUEUE=country_router",
                f"SENTINEL_PORT={worker_port}",
                f"NODE_ID={worker_name}_node"
            ],
            "volumes": [
                "./server/worker/filter_by_year:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common",
                f"./server/persistence/filter_by_year_worker_{i}:/app/persistence"
            ],
            "networks": [network]
        }
        
    return services

def get_worker_hosts_and_ports(num_workers=2):
    """
    Get the hostnames and ports for the filter_by_year workers.
    
    Args:
        num_workers (int): Number of filter_by_year workers
        
    Returns:
        tuple: (list of hostnames, list of ports)
    """
    base_port = 9007
    hosts = [f"filter_by_year_worker_{i}" for i in range(1, num_workers + 1)]
    ports = [base_port + (i - 1) * 10 for i in range(1, num_workers + 1)]
    
    return hosts, ports
