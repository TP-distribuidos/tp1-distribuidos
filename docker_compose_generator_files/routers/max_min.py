#!/usr/bin/env python3

def generate_max_min_router(num_avg_rating_workers):
    """
    Generate the max_min_router service configuration for Docker Compose.
    
    Args:
        num_avg_rating_workers (int): Total number of average_movies_by_rating workers
                                    (used to set NUMBER_OF_PRODUCER_WORKERS)
        
    Returns:
        dict: Dictionary with max_min_router service configuration
    """
    return {
        "max_min_router": {
            "build": {
                "context": "./server",
                "dockerfile": "router/Dockerfile"
            },
            "env_file": ["./server/router/.env"],
            "environment": [
                f"NUMBER_OF_PRODUCER_WORKERS={num_avg_rating_workers}",
                "INPUT_QUEUE=max_min_router",
                "OUTPUT_QUEUES=[[\"max_min_worker_1\"],[\"max_min_worker_2\"]]",
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
