from docker_compose_generator_files.constants import NETWORK

def generate_top_workers(num_workers=3, network=NETWORK):
    """
    Generate top worker services configuration for Docker Compose.
    
    Args:
        num_workers (int): Number of top workers to create
        network (str): Name of the Docker network to use
        
    Returns:
        dict: Dictionary with top worker service configurations
    """
    services = {}
    
    # Base port for sentinel monitoring
    base_port = 9100
    
    for i in range(1, num_workers + 1):
        # Calculate unique port for each worker
        worker_port = base_port + (i - 1) * 10
        
        services[f"top_worker_{i}"] = {
            "build": {
                "context": "./server",
                "dockerfile": "worker/top/Dockerfile"
            },
            "env_file": ["./server/worker/top/.env"],
            "ports": [
                f"{worker_port}:{worker_port}"
            ],
            "environment": [
                f"ROUTER_CONSUME_QUEUE=top_worker_{i}",
                "ROUTER_PRODUCER_QUEUE=top_10_actors_collector_router",
                f"SENTINEL_PORT={worker_port}"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/worker/top:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ],
            "networks": [network]
        }
        
    return services

def get_worker_hosts_and_ports(num_workers=3):
    """
    Get the hostnames and ports for the top workers.
    
    Args:
        num_workers (int): Number of top workers
        
    Returns:
        tuple: (list of hostnames, list of ports)
    """
    base_port = 9100
    hosts = [f"top_worker_{i}" for i in range(1, num_workers + 1)]
    ports = [base_port + (i - 1) * 10 for i in range(1, num_workers + 1)]
    
    return hosts, ports
