#!/usr/bin/env python3

def generate_collector_max_min_worker():
    """
    Generate the collector_max_min_worker service configuration for Docker Compose.
    
    Returns:
        dict: Dictionary with collector_max_min_worker service configuration
    """
    return {
        "collector_max_min_worker": {
            "build": {
                "context": "./server",
                "dockerfile": "worker/collector_max_min/Dockerfile"
            },
            "env_file": ["./server/worker/collector_max_min/.env"],
            "environment": [
                "ROUTER_CONSUME_QUEUE=collector_max_min_worker",
                "RESPONSE_QUEUE=response_queue"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/worker/collector_max_min:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ]
        }
    }
