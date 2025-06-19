#!/usr/bin/env python3
from docker_compose_generator_files.constants import NETWORK
sentinel_port = 9850

def get_boundary_host_and_port():
    """
    Get the host and port for the boundary service.
    
    Returns:
        tuple: (host, port) for the boundary service
    """
    return "boundary", sentinel_port

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
                "RATINGS_ROUTER_QUEUE=boundary_ratings_router",
                "NODE_ID=boundary_node",
                f"SENTINEL_PORT={sentinel_port}"
            ],
            "depends_on": {
                "rabbitmq": {
                    "condition": "service_healthy"
                }
            },
            "ports": ["5000:5000", f"{sentinel_port}:{sentinel_port}"],
            "volumes": [
                "./server/boundary:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common",
                "./server/persistence/boundary:/app/persistence"
            ],
            "networks": [NETWORK]
        }
    }