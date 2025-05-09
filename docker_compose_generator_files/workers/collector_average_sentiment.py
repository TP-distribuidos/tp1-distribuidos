#!/usr/bin/env python3

from docker_compose_generator_files.constants import NETWORK

def generate_collector_average_sentiment_worker(network=NETWORK):
    """
    Generate the collector_average_sentiment_worker service configuration for Docker Compose.
    
    Args:
        network (str): Name of the Docker network to use
        
    Returns:
        dict: Dictionary with collector_average_sentiment_worker service configuration
    """
    return {
        "collector_average_sentiment_worker": {
            "build": {
                "context": "./server",
                "dockerfile": "worker/collector_average_sentiment_worker/Dockerfile"
            },
            "env_file": ["./server/worker/collector_average_sentiment_worker/.env"],
            "environment": [
                "ROUTER_CONSUME_QUEUE=collector_average_sentiment_worker",
                "RESPONSE_QUEUE=response_queue"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/worker/collector_average_sentiment_worker:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ],
            "networks": [network]
        }
    }
