#!/usr/bin/env python3

from docker_compose_generator_files.workers.average_movies_by_rating import generate_output_queues_config
from docker_compose_generator_files.constants import NETWORK

def generate_average_movies_by_rating_router(num_join_ratings_workers=2, avg_rating_shards=2, avg_rating_replicas=2):
    """
    Generate the average_movies_by_rating_router service configuration for Docker Compose.
    
    Args:
        num_join_ratings_workers (int): Number of upstream join_ratings workers
                                    (used to set NUMBER_OF_PRODUCER_WORKERS)
        avg_rating_shards (int): Number of shards for downstream workers
        avg_rating_replicas (int): Target number of replicas per shard
        
    Returns:
        dict: Dictionary with average_movies_by_rating_router service configuration
    """
    # Generate the output queues configuration string with proper sharding
    output_queues = generate_output_queues_config(avg_rating_shards, avg_rating_replicas)
    
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
                f"OUTPUT_QUEUES={output_queues}",
                "BALANCER_TYPE=shard_by_ascii"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/router:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ],
            "networks": [NETWORK]
        }
    }
