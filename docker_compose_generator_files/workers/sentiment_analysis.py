from docker_compose_generator_files.constants import NETWORK

def generate_sentiment_analysis_workers(num_workers=2, network=NETWORK):
    """
    Generate sentiment_analysis worker services configuration for Docker Compose.
    
    Args:
        num_workers (int): Number of sentiment analysis workers to create
        network (str): Name of the Docker network to use
        
    Returns:
        dict: Dictionary with sentiment_analysis worker service configurations
    """
    services = {}
    
    # Base port for sentinel monitoring
    base_port = 9800
    
    for i in range(1, num_workers + 1):
        # Calculate unique port for each worker
        worker_port = base_port + (i - 1) * 10
        worker_name = f"sentiment_analysis_worker_{i}"
        
        services[worker_name] = {
            "build": {
                "context": "./server",
                "dockerfile": "worker/sentiment_analysis/Dockerfile"
            },
            "ports": [
                f"{worker_port}:{worker_port}"
            ],
            "env_file": ["./server/worker/sentiment_analysis/.env"],
            "environment": [
                f"ROUTER_CONSUME_QUEUE={worker_name}",
                "ROUTER_PRODUCER_QUEUE=average_sentiment_router",
                f"SENTINEL_PORT={worker_port}",
                f"NODE_ID={worker_name}_node"
            ],
            "depends_on": {
                "rabbitmq": {
                    "condition": "service_healthy"
                }
            },
            "volumes": [
                "./server/worker/sentiment_analysis:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common",
                f"./server/persistence/sentiment_analysis_worker_{i}:/app/persistence"
            ],
            "networks": [network],
            "deploy": {
                "resources": {
                    "limits": {
                        "memory": "4G",
                        "cpus": "1.5"
                    },
                    "reservations": {
                        "memory": "2G",
                        "cpus": "0.5"
                    }
                }
            },
            "shm_size": "1g"  # Shared memory for NLP operations
        }
    
    return services

def get_worker_hosts_and_ports(num_workers=2):
    """
    Get the hostnames and ports for the sentiment_analysis workers.
    
    Args:
        num_workers (int): Number of sentiment analysis workers
        
    Returns:
        tuple: (list of hostnames, list of ports)
    """
    base_port = 9800
    hosts = [f"sentiment_analysis_worker_{i}" for i in range(1, num_workers + 1)]
    ports = [base_port + (i - 1) * 10 for i in range(1, num_workers + 1)]
    
    return hosts, ports
