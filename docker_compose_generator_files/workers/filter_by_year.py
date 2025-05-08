#!/usr/bin/env python3

def generate_worker_queue_names(num_workers=2):
    """
    Generate queue names for filter_by_year workers.
    
    Args:
        num_workers (int): Number of filter_by_year workers
        
    Returns:
        list: List of queue names for the workers
    """
    return [f"filter_by_year_worker_{i}" for i in range(1, num_workers + 1)]

def generate_filter_by_year_workers(num_workers=2):
    """
    Generate filter_by_year worker services configuration for Docker Compose.
    
    Args:
        num_workers (int): Number of filter_by_year workers to create
        
    Returns:
        dict: Dictionary with filter_by_year worker service configurations
    """
    services = {}
    
    for i in range(1, num_workers + 1):
        services[f"filter_by_year_worker_{i}"] = {
            "build": {
                "context": "./server",
                "dockerfile": "worker/filter_by_year/Dockerfile"
            },
            "env_file": ["./server/worker/filter_by_year/.env"],
            "environment": [
                f"ROUTER_CONSUME_QUEUE=filter_by_year_worker_{i}",
                "ROUTER_PRODUCER_QUEUE=country_router"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/worker/filter_by_year:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ]
        }
        
    return services
