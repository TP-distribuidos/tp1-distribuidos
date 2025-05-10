#!/usr/bin/env python3

from docker_compose_generator_files.constants import NETWORK

def generate_average_sentiment_collector_router(num_avg_sentiment_workers=2, network=NETWORK):
    """
    Generate the average_sentiment_collector_router service configuration for Docker Compose.
    
    Args:
        num_avg_sentiment_workers (int): Number of average_sentiment workers feeding into this collector
                               (used to set NUMBER_OF_PRODUCER_WORKERS)
        network (str): Name of the Docker network to use
        
    Returns:
        dict: Dictionary with average_sentiment_collector_router service configuration
    """
    return {
        "average_sentiment_collector_router": {
            "build": {
                "context": "./server",
                "dockerfile": "router/Dockerfile"
            },
            "env_file": ["./server/router/.env"],
            "environment": [
                f"NUMBER_OF_PRODUCER_WORKERS={num_avg_sentiment_workers}",
                "INPUT_QUEUE=average_sentiment_collector_router",
                "OUTPUT_QUEUES=collector_average_sentiment_worker",
                "BALANCER_TYPE=round_robin"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/router:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ],
            "networks": [network]
        }
    }