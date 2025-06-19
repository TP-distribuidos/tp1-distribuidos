from docker_compose_generator_files.constants import NETWORK

def generate_collector_max_min_worker(network=NETWORK):
    """
    Generate the collector_max_min_worker service configuration for Docker Compose.
    
    Args:
        network (str): Name of the Docker network to use
        
    Returns:
        dict: Dictionary with collector_max_min_worker service configuration
    """
    # Set port for sentinel monitoring
    worker_port = 9250
    worker_name = "collector_max_min_worker"
    
    return {
        worker_name: {
            "build": {
                "context": "./server",
                "dockerfile": "worker/collector_max_min/Dockerfile"
            },
            "ports": [
                f"{worker_port}:{worker_port}"
            ],
            "env_file": ["./server/worker/collector_max_min/.env"],
            "environment": [
                f"ROUTER_CONSUME_QUEUE={worker_name}",
                "RESPONSE_QUEUE=response_queue",
                f"SENTINEL_PORT={worker_port}",
                f"NODE_ID={worker_name}_node"
            ],
            "depends_on": {
                "rabbitmq": {
                    "condition": "service_healthy"
                }
            },
            "volumes": [
                "./server/worker/collector_max_min:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common",
                "./server/persistence/collector_max_min_worker:/app/persistence"
            ],
            "networks": [network]
        }
    }

def get_worker_host_and_port():
    """
    Get the hostname and port for the collector_max_min_worker.
    
    Returns:
        tuple: (hostname, port)
    """
    return "collector_max_min_worker", 9250
