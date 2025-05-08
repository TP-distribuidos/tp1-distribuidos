#!/usr/bin/env python3

from docker_compose_generator_files.workers.filter_by_country import generate_worker_queue_names

def generate_country_router(num_year_workers=2, num_country_workers=2):
    """
    Generate the country_router service configuration for Docker Compose.
    
    Args:
        num_year_workers (int): Number of filter_by_year workers 
                               (used to set NUMBER_OF_PRODUCER_WORKERS)
        num_country_workers (int): Number of filter_by_country workers
                                  (used to set OUTPUT_QUEUES)
        
    Returns:
        dict: Dictionary with country_router service configuration
    """
    # Get queue names from the filter_by_country module
    worker_queues = generate_worker_queue_names(num_country_workers)
    
    # Create the output queues string for country_router
    country_worker_queues = ",".join(worker_queues)
    
    return {
        "country_router": {
            "build": {
                "context": "./server",
                "dockerfile": "router/Dockerfile"
            },
            "env_file": ["./server/router/.env"],
            "environment": [
                f"NUMBER_OF_PRODUCER_WORKERS={num_year_workers}",
                "INPUT_QUEUE=country_router",
                f"OUTPUT_QUEUES={country_worker_queues}",
                "BALANCER_TYPE=round_robin"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/router:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ]
        }
    }
