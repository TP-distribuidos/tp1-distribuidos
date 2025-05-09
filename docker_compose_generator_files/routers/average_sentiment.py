#!/usr/bin/env python3

from docker_compose_generator_files.constants import NETWORK

def generate_average_sentiment_router(num_sentiment_workers=2, num_avg_sentiment_workers=2, network=NETWORK):
    """
    Generate the average_sentiment_router service configuration for Docker Compose.
    
    Args:
        num_sentiment_workers (int): Number of sentiment analysis workers feeding into this router
                               (used to set NUMBER_OF_PRODUCER_WORKERS)
        num_avg_sentiment_workers (int): Number of average sentiment worker destinations
        network (str): Name of the Docker network to use
        
    Returns:
        dict: Dictionary with average_sentiment_router service configuration
    """
    # Create the output queues configuration for sharding - use the configurable number of workers
    output_queues = '[['
    for i in range(1, num_avg_sentiment_workers + 1):
        if i > 1:
            output_queues += '],['
        output_queues += f'"average_sentiment_worker_{i}"'
    output_queues += ']]'
    
    return {
        "average_sentiment_router": {
            "build": {
                "context": "./server",
                "dockerfile": "router/Dockerfile"
            },
            "env_file": ["./server/router/.env"],
            "environment": [
                f"NUMBER_OF_PRODUCER_WORKERS={num_sentiment_workers}",
                "INPUT_QUEUE=average_sentiment_router",
                f"OUTPUT_QUEUES={output_queues}",
                "BALANCER_TYPE=shard_by_ascii"
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
