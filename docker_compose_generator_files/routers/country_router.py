#!/usr/bin/env python3

def generate_country_router(num_year_workers=2):
    """
    Generate the country_router service configuration for Docker Compose.
    
    Args:
        num_year_workers (int): Number of filter_by_year workers 
                               (used to set NUMBER_OF_PRODUCER_WORKERS)
        
    Returns:
        dict: Dictionary with country_router service configuration
    """
    return {
        "country_router": {
            "build": {
                "context": "./server",
                "dockerfile": "router/Dockerfile"
            },
            "env_file": ["./server/router/.env"],
            "environment": [
                f"NUMBER_OF_PRODUCER_WORKERS={num_year_workers}",
                "INPUT_QUEUE=country_router",
                "OUTPUT_QUEUES=filter_by_country_worker_1,filter_by_country_worker_2",
                "BALANCER_TYPE=round_robin"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/router:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ]
        }
    }
