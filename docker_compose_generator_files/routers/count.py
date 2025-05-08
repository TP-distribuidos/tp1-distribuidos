#!/usr/bin/env python3

def generate_count_router(num_join_credits_workers=2):
    """
    Generate the count_router service configuration for Docker Compose.
    
    Args:
        num_join_credits_workers (int): Number of join_credits workers
                                    (used to set NUMBER_OF_PRODUCER_WORKERS)
        
    Returns:
        dict: Dictionary with count_router service configuration
    """
    return {
        "count_router": {
            "build": {
                "context": "./server",
                "dockerfile": "router/Dockerfile"
            },
            "env_file": ["./server/router/.env"],
            "environment": [
                f"NUMBER_OF_PRODUCER_WORKERS={num_join_credits_workers}", 
                "INPUT_QUEUE=count_router",
                "OUTPUT_QUEUES=[[\"count_worker_1\", \"count_worker_2\"],[\"count_worker_3\", \"count_worker_4\"]]",
                "BALANCER_TYPE=shard_by_ascii"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/router:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ]
        }
    }
