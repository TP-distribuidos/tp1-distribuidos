#!/usr/bin/env python3

from docker_compose_generator_files.constants import NETWORK

def generate_max_min_workers(num_workers=2):
    """
    Generate max_min worker services configuration for Docker Compose.
    
    Args:
        num_workers (int): Number of max_min workers to create
        
    Returns:
        dict: Dictionary with max_min worker service configurations
    """
    services = {}
    
    for i in range(1, num_workers + 1):
        services[f"max_min_worker_{i}"] = {
            "build": {
                "context": "./server",
                "dockerfile": "worker/max_min/Dockerfile"
            },
            "env_file": ["./server/worker/max_min/.env"],
            "environment": [
                f"ROUTER_CONSUME_QUEUE=max_min_worker_{i}",
                "ROUTER_PRODUCER_QUEUE=max_min_collector_router"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/worker/max_min:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ],
            "networks": [NETWORK]
        }
        
    return services
