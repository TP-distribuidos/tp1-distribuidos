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
    
    return {
        "collector_max_min_worker": {
            "build": {
                "context": "./server",
                "dockerfile": "worker/collector_max_min/Dockerfile"
            },
            "ports": [
                f"{worker_port}:{worker_port}"
            ],
            "env_file": ["./server/worker/collector_max_min/.env"],
            "environment": [
                "ROUTER_CONSUME_QUEUE=collector_max_min_worker",
                "RESPONSE_QUEUE=response_queue",
                f"SENTINEL_PORT={worker_port}"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/worker/collector_max_min:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common",
                "./server/wal/collector_max_min_worker:/app/wal"
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
