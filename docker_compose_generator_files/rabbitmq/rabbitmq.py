#!/usr/bin/env python3

from docker_compose_generator_files.constants import NETWORK

def generate_rabbitmq_service():
    """
    Generate the RabbitMQ service configuration for Docker Compose.
    
    Returns:
        dict: Dictionary with RabbitMQ service configuration
    """
    return {
        "rabbitmq": {
            "image": "rabbitmq:3-management",
            "ports": ["5672:5672", "15672:15672"],
            "environment": [
                "RABBITMQ_LOG_LEVEL=none",
                "RABBITMQ_LOGS=/dev/null",  # Send logs to /dev/null instead of stdout
                "RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS=-rabbit log_levels [{connection,none},{channel,none},{authentication,none},{federation,none},{queue,none},{default,none},{access_control,none},{amqp,none},{amqp_client,none}]"
            ],
            "networks": [NETWORK],
            
            "logging": {
                "driver": "none"  # This completely discards all container logs
            },
            
            "healthcheck": {
                # A more comprehensive health check that verifies RabbitMQ is ready for connections
                "test": ["CMD", "bash", "-c", "rabbitmq-diagnostics -q check_port_connectivity && rabbitmq-diagnostics -q check_running"],
                "interval": "10s",
                "timeout": "10s",
                "retries": 5,
                "start_period": "20s"  
            }
        }
    }