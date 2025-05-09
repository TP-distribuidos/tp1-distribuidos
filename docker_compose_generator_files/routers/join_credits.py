#!/usr/bin/env python3

from docker_compose_generator_files.workers.join_credits import generate_worker_queue_names
from docker_compose_generator_files.constants import NETWORK

def generate_join_credits_router(num_join_credits_workers=2):
    """
    Generate the join_credits_router service configuration for Docker Compose.
    
    Args:
        num_join_credits_workers (int): Number of join_credits workers
        
    Returns:
        dict: Dictionary with join_credits_router service configuration
    """
    # Get the credits queue names for the join_credits workers
    queue_names = generate_worker_queue_names(num_join_credits_workers)
    credits_queues = queue_names["credits"]
    
    # Create the output queues string for join_credits_router
    output_queues = ",".join(credits_queues)
    
    return {
        "join_credits_router": {
            "build": {
                "context": "./server",
                "dockerfile": "router/Dockerfile"
            },
            "env_file": ["./server/router/.env"],
            "environment": [
                "NUMBER_OF_PRODUCER_WORKERS=1",
                "INPUT_QUEUE=boundary_credits_router",
                f"OUTPUT_QUEUES={output_queues}",
                "BALANCER_TYPE=round_robin"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/router:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ],
            "networks": [NETWORK]
        }
    }
