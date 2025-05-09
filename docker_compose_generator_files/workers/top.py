#!/usr/bin/env python3

from docker_compose_generator_files.constants import NETWORK

def generate_top_workers(num_workers=3):
    """
    Generate top worker services configuration for Docker Compose.
    
    Args:
        num_workers (int): Number of top workers to create
        
    Returns:
        dict: Dictionary with top worker service configurations
    """
    services = {}
    
    for i in range(1, num_workers + 1):
        services[f"top_worker_{i}"] = {
            "build": {
                "context": "./server",
                "dockerfile": "worker/top/Dockerfile"
            },
            "env_file": ["./server/worker/top/.env"],
            "environment": [
                f"ROUTER_CONSUME_QUEUE=top_worker_{i}",
                "ROUTER_PRODUCER_QUEUE=top_10_actors_collector_router"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/worker/top:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ],
            "networks": [NETWORK]
        }
        
    return services