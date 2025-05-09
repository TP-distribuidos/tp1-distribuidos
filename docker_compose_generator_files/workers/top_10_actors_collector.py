#!/usr/bin/env python3

def generate_collector_top_10_actors_worker():
    """
    Generate the collector_top_10_actors_worker service configuration for Docker Compose.
    
    Returns:
        dict: Dictionary with collector_top_10_actors_worker service configuration
    """
    return {
        "collector_top_10_actors_worker": {
            "build": {
                "context": "./server",
                "dockerfile": "worker/collector_top_10_actors/Dockerfile"
            },
            "env_file": ["./server/worker/collector_top_10_actors/.env"],
            "environment": [
                "ROUTER_CONSUME_QUEUE=collector_top_10_actors_worker",
                "RESPONSE_QUEUE=response_queue"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/worker/collector_top_10_actors:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ]
        }
    }
