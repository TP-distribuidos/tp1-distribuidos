#!/usr/bin/env python3

from docker_compose_generator_files.workers.count import generate_output_queues_config
from docker_compose_generator_files.constants import NETWORK

def generate_count_router(num_join_credits_workers=2, count_shards=2, count_workers_per_shard=2):
    """
    Generate the count_router service configuration for Docker Compose.
    
    Args:
        num_join_credits_workers (int): Number of upstream join_credits workers
                                    (used to set NUMBER_OF_PRODUCER_WORKERS)
        count_shards (int): Number of shards for downstream count workers
        count_workers_per_shard (int): Number of count workers per shard
        
    Returns:
        dict: Dictionary with count_router service configuration
    """
    # Generate the output queues configuration string with proper sharding
    output_queues = generate_output_queues_config(count_shards, count_workers_per_shard)
    
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
