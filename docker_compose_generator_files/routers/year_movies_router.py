#!/usr/bin/env python3

from docker_compose_generator_files.workers.filter_by_year import generate_worker_queue_names

def generate_year_movies_router(num_workers=2):
    """
    Generate the year_movies_router service configuration for Docker Compose.
    
    Args:
        num_workers (int): Number of filter_by_year workers
        
    Returns:
        dict: Dictionary with year_movies_router service configuration
    """
    # Get queue names from the filter_by_year module
    worker_queues = generate_worker_queue_names(num_workers)
    
    # Create the output queues string for year_movies_router
    year_worker_queues = ",".join(worker_queues)
    
    return {
        "year_movies_router": {
            "build": {
                "context": "./server",
                "dockerfile": "router/Dockerfile"
            },
            "env_file": ["./server/router/.env"],
            "environment": [
                "NUMBER_OF_PRODUCER_WORKERS=1",
                "INPUT_QUEUE=boundary_movies_router",
                f"OUTPUT_QUEUES={year_worker_queues}",
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
