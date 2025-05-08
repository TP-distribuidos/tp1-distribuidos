#!/usr/bin/env python3

def generate_average_movies_by_rating_router(num_join_ratings_workers=2):
    """
    Generate the average_movies_by_rating_router service configuration for Docker Compose.
    
    Args:
        num_join_ratings_workers (int): Number of join_ratings workers
                                    (used to set NUMBER_OF_PRODUCER_WORKERS)
        
    Returns:
        dict: Dictionary with average_movies_by_rating_router service configuration
    """
    # For this router we maintain the fixed output queues but make the NUMBER_OF_PRODUCER_WORKERS
    # dependent on the number of join_ratings workers feeding into it
    
    return {
        "average_movies_by_rating_router": {
            "build": {
                "context": "./server",
                "dockerfile": "router/Dockerfile"
            },
            "env_file": ["./server/router/.env"],
            "environment": [
                f"NUMBER_OF_PRODUCER_WORKERS={num_join_ratings_workers}",
                "INPUT_QUEUE=average_movies_by_rating_router",
                "OUTPUT_QUEUES=[[\"average_movies_by_rating_worker_1\"],[\"average_movies_by_rating_worker_2\",\"average_movies_by_rating_worker_3\"]]",
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
