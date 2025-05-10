#!/usr/bin/env python3

from docker_compose_generator_files.constants import NETWORK

def generate_top_router(num_count_workers=4, num_top_workers=3):
    """
    Generate the top_router service configuration for Docker Compose.
    
    Args:
        num_count_workers (int): Total number of count workers feeding into this router
                              (used to set NUMBER_OF_PRODUCER_WORKERS)
        num_top_workers (int): Number of top workers to distribute to
        
    Returns:
        dict: Dictionary with top_router service configuration
    """
    # Create the output queues configuration - typically each top worker is in its own shard
    # for proper actor distribution
    output_queues = '['
    for i in range(1, num_top_workers+1):
        if i > 1:
            output_queues += ','
        output_queues += f'["top_worker_{i}"]'
    output_queues += ']'
    
    return {
        "top_router": {
            "build": {
                "context": "./server",
                "dockerfile": "router/Dockerfile"
            },
            "env_file": ["./server/router/.env"],
            "environment": [
                f"NUMBER_OF_PRODUCER_WORKERS={num_count_workers}",
                "INPUT_QUEUE=top_router",
                f"OUTPUT_QUEUES={output_queues}",
                "BALANCER_TYPE=shard_by_ascii"
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
