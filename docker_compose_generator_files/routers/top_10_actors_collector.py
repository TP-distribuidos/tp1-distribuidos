#!/usr/bin/env python3

from docker_compose_generator_files.constants import NETWORK

def generate_top_10_actors_collector_router(num_top_workers=3):
    """
    Generate the top_10_actors_collector_router service configuration for Docker Compose.
    
    Args:
        num_top_workers (int): Number of top workers feeding into this collector
                           (used to set NUMBER_OF_PRODUCER_WORKERS)
        
    Returns:
        dict: Dictionary with top_10_actors_collector_router service configuration
    """
    return {
        "top_10_actors_collector_router": {
            "build": {
                "context": "./server",
                "dockerfile": "router/Dockerfile"
            },
            "env_file": ["./server/router/.env"],
            "environment": [
                f"NUMBER_OF_PRODUCER_WORKERS={num_top_workers}",
                "INPUT_QUEUE=top_10_actors_collector_router",
                "OUTPUT_QUEUES=collector_top_10_actors_worker",
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
