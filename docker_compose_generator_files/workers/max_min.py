from docker_compose_generator_files.constants import NETWORK

def generate_max_min_workers(num_workers=2, network=NETWORK):
    """
    Generate max_min worker services configuration for Docker Compose.
    
    Args:
        num_workers (int): Number of max_min workers to create
        network (str): Name of the Docker network to use
        
    Returns:
        dict: Dictionary with max_min worker service configurations
    """
    services = {}
    
    # Base port for sentinel monitoring
    base_port = 9900
    
    for i in range(1, num_workers + 1):
        # Calculate unique port for each worker
        worker_port = base_port + (i - 1) * 10
        worker_name = f"max_min_worker_{i}"
        
        services[worker_name] = {
            "build": {
                "context": "./server",
                "dockerfile": "worker/max_min/Dockerfile"
            },
            "ports": [
                f"{worker_port}:{worker_port}"
            ],
            "env_file": ["./server/worker/max_min/.env"],
            "environment": [
                f"ROUTER_CONSUME_QUEUE={worker_name}",
                "ROUTER_PRODUCER_QUEUE=max_min_collector_router",
                f"SENTINEL_PORT={worker_port}",
                f"NODE_ID={worker_name}_node"
            ],
            "depends_on": {
                "rabbitmq": {
                    "condition": "service_healthy"
                }
            },
            "volumes": [
                "./server/worker/max_min:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common",
                f"./server/persistence/max_min_worker_{i}:/app/persistence"
            ],
            "networks": [network]
        }
        
    return services

def get_worker_hosts_and_ports(num_workers=2):
    """
    Get the hostnames and ports for the max_min workers.
    
    Args:
        num_workers (int): Number of max_min workers
        
    Returns:
        tuple: (list of hostnames, list of ports)
    """
    base_port = 9900
    hosts = [f"max_min_worker_{i}" for i in range(1, num_workers + 1)]
    ports = [base_port + (i - 1) * 10 for i in range(1, num_workers + 1)]
    
    return hosts, ports
