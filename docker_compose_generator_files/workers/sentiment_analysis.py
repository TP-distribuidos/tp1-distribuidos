#!/usr/bin/env python3

from docker_compose_generator_files.constants import NETWORK

def generate_sentiment_analysis_workers(num_workers=2, network=NETWORK):
    """
    Generate sentiment_analysis worker services configuration for Docker Compose.
    
    Args:
        num_workers (int): Number of sentiment analysis workers to create
        network (str): Name of the Docker network to use
        
    Returns:
        dict: Dictionary with sentiment_analysis worker service configurations
    """
    services = {}
    
    for i in range(1, num_workers + 1):
        services[f"sentiment_analysis_worker_{i}"] = {
            "build": {
                "context": "./server",
                "dockerfile": "worker/sentiment_analysis/Dockerfile"
            },
            "env_file": ["./server/worker/sentiment_analysis/.env"],
            "environment": [
                f"ROUTER_CONSUME_QUEUE=sentiment_analysis_worker_{i}",
                "ROUTER_PRODUCER_QUEUE=average_sentiment_router"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/worker/sentiment_analysis:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ],
            "networks": [network],
            "deploy": {
                "resources": {
                    "limits": {
                        "memory": "2G"
                    }
                }
            }
        }
    
    return services
