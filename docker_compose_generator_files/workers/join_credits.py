#!/usr/bin/env python3

def generate_worker_queue_names(num_workers=2):
    """
    Generate queue names for join_credits workers.
    
    Args:
        num_workers (int): Number of join_credits workers
        
    Returns:
        dict: Dictionary containing lists of queue names for the workers
    """
    movies_queues = [f"join_credits_worker_{i}_movies" for i in range(1, num_workers + 1)]
    credits_queues = [f"join_credits_worker_{i}_credits" for i in range(1, num_workers + 1)]
    
    return {
        "movies": movies_queues,
        "credits": credits_queues
    }

def generate_join_credits_workers(num_workers=2):
    """
    Generate join_credits worker services configuration for Docker Compose.
    
    Args:
        num_workers (int): Number of join_credits workers to create
        
    Returns:
        dict: Dictionary with join_credits worker service configurations
    """
    services = {}
    
    for i in range(1, num_workers + 1):
        services[f"join_credits_worker_{i}"] = {
            "build": {
                "context": "./server",
                "dockerfile": "worker/join_credits/Dockerfile"
            },
            "env_file": ["./server/worker/join_credits/.env"],
            "environment": [
                f"ROUTER_CONSUME_QUEUE_MOVIES=join_credits_worker_{i}_movies",
                f"ROUTER_CONSUME_QUEUE_CREDITS=join_credits_worker_{i}_credits",
                "ROUTER_PRODUCER_QUEUE=count_router"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/worker/join_credits:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ]
        }
        
    return services
