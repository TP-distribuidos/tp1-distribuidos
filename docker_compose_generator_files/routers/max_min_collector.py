#!/usr/bin/env python3

from docker_compose_generator_files.constants import NETWORK

def generate_max_min_collector_router(num_max_min_workers=2):
    """
    Generate the max_min_collector_router service configuration for Docker Compose.
    
    Args:
        num_max_min_workers (int): Number of max_min workers feeding into this collector
                           (used to set NUMBER_OF_PRODUCER_WORKERS)
        
    Returns:
        dict: Dictionary with max_min_collector_router service configuration
    """
    return {
        "max_min_collector_router": {
            "build": {
                "context": "./server",
                "dockerfile": "router/Dockerfile"
            },
            "env_file": ["./server/router/.env"],
            "environment": [
                f"NUMBER_OF_PRODUCER_WORKERS={num_max_min_workers}",
                "INPUT_QUEUE=max_min_collector_router",
                "OUTPUT_QUEUES=collector_max_min_worker",
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
