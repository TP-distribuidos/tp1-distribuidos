from docker_compose_generator_files.workers.filter_by_country import generate_worker_queue_names
from docker_compose_generator_files.constants import NETWORK

def generate_country_router(num_year_workers=2, num_country_workers=2, network=NETWORK):
    """
    Generate the country_router service configuration for Docker Compose.
    
    Args:
        num_year_workers (int): Number of filter_by_year workers 
                               (used to set NUMBER_OF_PRODUCER_WORKERS)
        num_country_workers (int): Number of filter_by_country workers
                                  (used to set OUTPUT_QUEUES)
        network (str): Name of the Docker network to use
        
    Returns:
        dict: Dictionary with country_router service configuration
    """
    # Get queue names from the filter_by_country module
    worker_queues = generate_worker_queue_names(num_country_workers)
    
    # Create the output queues string for country_router
    country_worker_queues = ",".join(worker_queues)
    
    # Base port for sentinel monitoring
    base_port = 9301
    
    return {
        "country_router": {
            "build": {
                "context": "./server",
                "dockerfile": "router/Dockerfile"
            },
            "env_file": ["./server/router/.env"],
            "ports": [
                f"{base_port}:{base_port}"
            ],
            "environment": [
                f"NUMBER_OF_PRODUCER_WORKERS={num_year_workers}",
                "INPUT_QUEUE=country_router",
                f"OUTPUT_QUEUES={country_worker_queues}",
                "BALANCER_TYPE=round_robin",
                f"SENTINEL_PORT={base_port}"
            ],
            "depends_on": {
                "rabbitmq": {
                    "condition": "service_healthy"
                }
            },
            "volumes": [
                "./server/router:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common",
                "./server/persistence/country_router:/app/persistence"
            ],
            "networks": [network]
        }
    }

def get_router_host_and_port():
    """
    Get the hostname and port for the country_router.
    
    Returns:
        tuple: (hostname, port)
    """
    return "country_router", 9301
