#!/usr/bin/env python3

from docker_compose_generator_files.workers.join_ratings import generate_worker_queue_names

def generate_join_ratings_router(num_join_ratings_workers=2):
    """
    Generate the join_ratings_router service configuration for Docker Compose.
    
    Args:
        num_join_ratings_workers (int): Number of join_ratings workers
        
    Returns:
        dict: Dictionary with join_ratings_router service configuration
    """
    # Get the ratings queue names for the join_ratings workers
    queue_names = generate_worker_queue_names(num_join_ratings_workers)
    ratings_queues = queue_names["ratings"]
    
    # Create the output queues string for join_ratings_router
    output_queues = ",".join(ratings_queues)
    
    return {
        "join_ratings_router": {
            "build": {
                "context": "./server",
                "dockerfile": "router/Dockerfile"
            },
            "env_file": ["./server/router/.env"],
            "environment": [
                "NUMBER_OF_PRODUCER_WORKERS=1",
                "INPUT_QUEUE=boundary_ratings_router",
                f"OUTPUT_QUEUES={output_queues}",
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
