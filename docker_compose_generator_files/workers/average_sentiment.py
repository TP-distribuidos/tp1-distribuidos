#!/usr/bin/env python3

from docker_compose_generator_files.constants import NETWORK

def generate_average_sentiment_workers(num_workers=2, network=NETWORK):
    """
    Generate average_sentiment worker services configuration for Docker Compose.
    
    Args:
        num_workers (int): Number of average sentiment workers to create
        network (str): Name of the Docker network to use
        
    Returns:
        dict: Dictionary with average_sentiment worker service configurations
    """
    services = {}
    
    for i in range(1, num_workers + 1):
        services[f"average_sentiment_worker_{i}"] = {
            "build": {
                "context": "./server",
                "dockerfile": "worker/average_sentiment/Dockerfile"
            },
            "env_file": ["./server/worker/average_sentiment/.env"],
            "environment": [
                f"ROUTER_CONSUME_QUEUE=average_sentiment_worker_{i}",
                "ROUTER_PRODUCER_QUEUE=average_sentiment_collector_router"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/worker/average_sentiment:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ],
            "networks": [network]
        }
    
    return services
