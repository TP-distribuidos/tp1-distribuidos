#!/usr/bin/env python3
from docker_compose_generator_files.constants import NETWORK

def generate_boundary_service():
    """
    Generate the boundary service configuration for Docker Compose.
    
    Returns:
        dict: Dictionary with boundary service configuration
    """
    return {
        "boundary": {
            "build": {
                "context": "./server",
                "dockerfile": "boundary/Dockerfile"
            },
            "env_file": ["./server/boundary/.env"],
            "environment": [
                "MOVIES_ROUTER_QUEUE=boundary_movies_router",
                "MOVIES_ROUTER_Q5_QUEUE=boundary_movies_Q5_router",
                "CREDITS_ROUTER_QUEUE=boundary_credits_router",
                "RATINGS_ROUTER_QUEUE=boundary_ratings_router"
                "NODE_ID=boundary_node"
            ],
            "depends_on": {
                "rabbitmq": {
                    "condition": "service_healthy"
                }
            },
            "ports": ["5000:5000"],
            "volumes": [
                "./server/boundary:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ],
            "networks": [NETWORK]
        }
    }