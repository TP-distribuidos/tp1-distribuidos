#!/usr/bin/env python3

def generate_join_movies_router(num_country_workers=2):
    """
    Generate the join_movies_router service configuration for Docker Compose.
    
    Args:
        num_country_workers (int): Number of filter_by_country workers 
                               (used to set NUMBER_OF_PRODUCER_WORKERS)
        
    Returns:
        dict: Dictionary with join_movies_router service configuration
    """
    return {
        "join_movies_router": {
            "build": {
                "context": "./server",
                "dockerfile": "router/Dockerfile"
            },
            "env_file": ["./server/router/.env"],
            "environment": [
                f"NUMBER_OF_PRODUCER_WORKERS={num_country_workers}",  # Dynamic based on num_country_workers
                "INPUT_QUEUE=join_movies_router",
                "OUTPUT_QUEUES=join_ratings_worker_1_movies,join_ratings_worker_2_movies,join_credits_worker_1_movies,join_credits_worker_2_movies",
                "EXCHANGE_TYPE=fanout",
                "EXCHANGE_NAME=join_router_exchange"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/router:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ]
        }
    }
