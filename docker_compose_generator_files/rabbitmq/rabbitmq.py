#!/usr/bin/env python3

def generate_rabbitmq_service():
    """
    Generate the RabbitMQ service configuration for Docker Compose.
    
    Returns:
        dict: Dictionary with RabbitMQ service configuration
    """
    return {
        "rabbitmq": {
            "image": "rabbitmq:3-management",
            "ports": ["5672:5672", "15672:15672"]
        }
    }
