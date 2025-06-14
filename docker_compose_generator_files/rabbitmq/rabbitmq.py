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
                # use the flag --erlang-cookie if setting the erlang 
                # cookie was necessary (comment by red-riding-hood)
                # test: rabbitmq-diagnostics -q ping --erlang-cookie "mycookie"
                "test": "rabbitmq-diagnostics -q ping",
                "interval": "30s",
                "timeout": "10s",
                "retries": 5,
                "start_period": "10s"
            }
        }
    }