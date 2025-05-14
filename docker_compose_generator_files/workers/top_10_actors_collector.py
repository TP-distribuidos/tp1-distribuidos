from docker_compose_generator_files.constants import NETWORK

def generate_collector_top_10_actors_worker(network=NETWORK):
    """
    Generate the collector_top_10_actors_worker service configuration for Docker Compose.
    
    Args:
        network (str): Name of the Docker network to use
        
    Returns:
        dict: Dictionary with collector_top_10_actors_worker service configuration
    """
    # Set port for sentinel monitoring
    worker_port = 9151
    
    return {
        "collector_top_10_actors_worker": {
            "build": {
                "context": "./server",
                "dockerfile": "worker/collector_top_10_actors/Dockerfile"
            },
            "ports": [
                f"{worker_port}:{worker_port}"
            ],
            "env_file": ["./server/worker/collector_top_10_actors/.env"],
            "environment": [
                "ROUTER_CONSUME_QUEUE=collector_top_10_actors_worker",
                "RESPONSE_QUEUE=response_queue",
                f"SENTINEL_PORT={worker_port}"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/worker/collector_top_10_actors:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ],
            "networks": [network]
        }
    }

def get_worker_host_and_port():
    """
    Get the hostname and port for the collector_top_10_actors_worker.
    
    Returns:
        tuple: (hostname, port)
    """
    return "collector_top_10_actors_worker", 9151
