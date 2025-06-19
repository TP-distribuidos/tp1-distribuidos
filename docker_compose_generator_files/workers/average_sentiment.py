from docker_compose_generator_files.constants import NETWORK

def generate_worker_queue_names(num_workers=2):
    """
    Generate queue names for average_sentiment workers.
    
    Args:
        num_workers (int): Number of average_sentiment workers
        
    Returns:
        list: List of queue names for the workers
    """
    return [f"average_sentiment_worker_{i}" for i in range(1, num_workers + 1)]

def generate_average_sentiment_workers(num_workers=2, network=NETWORK):
    """
    Generate average_sentiment worker services configuration for Docker Compose.
    
    Args:
        num_workers (int): Number of average sentiment workers to create
        network (str): Name of the Docker network to use
        
    Returns:
        dict: Dictionary with average_sentiment worker service configurations
    """
    services = {}
    
    # Base port for sentinel monitoring
    base_port = 9073
    
    for i in range(1, num_workers + 1):
        # Calculate unique port for each worker
        worker_port = base_port + (i - 1) * 10
        worker_name = f"average_sentiment_worker_{i}"
        
        services[worker_name] = {
            "build": {
                "context": "./server",
                "dockerfile": "worker/average_sentiment/Dockerfile"
            },
            "depends_on": {
                "rabbitmq": {
                    "condition": "service_healthy"
                }
            },
            "ports": [
                f"{worker_port}:{worker_port}"
            ],
            "env_file": ["./server/worker/average_sentiment/.env"],
            "environment": [
                f"ROUTER_CONSUME_QUEUE={worker_name}",
                "ROUTER_PRODUCER_QUEUE=average_sentiment_collector_router",
                f"SENTINEL_PORT={worker_port}",
                f"NODE_ID={worker_name}_node"
            ],
            "volumes": [
                "./server/worker/average_sentiment:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common",
                f"./server/persistence/average_sentiment_worker_{i}:/app/persistence"
            ],
            "networks": [network]
        }
    
    return services

def get_worker_hosts_and_ports(num_workers=2):
    """
    Get the hostnames and ports for the average_sentiment workers.
    
    Args:
        num_workers (int): Number of average_sentiment workers
        
    Returns:
        tuple: (list of hostnames, list of ports)
    """
    base_port = 9073
    hosts = [f"average_sentiment_worker_{i}" for i in range(1, num_workers + 1)]
    ports = [base_port + (i - 1) * 10 for i in range(1, num_workers + 1)]
    
    return hosts, ports
