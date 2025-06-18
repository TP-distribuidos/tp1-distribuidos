#!/usr/bin/env python3

from docker_compose_generator_files.constants import NETWORK, NUMBER_OF_CLIENTS_AUTOMATIC

def generate_client_services(num_clients=4, auto_clients=NUMBER_OF_CLIENTS_AUTOMATIC):
    """
    Generate client services configuration for Docker Compose.
    
    Args:
        num_clients (int): Number of client nodes to create
        auto_clients (int): Number of client nodes to start automatically
        
    Returns:
        dict: Dictionary with client service configurations
    """
    services = {}
    
    # Base IP for clients (using 172.25.0.x range)
    client_ip_base = "172.25.0."

    for i in range(1, num_clients + 1):
        client_config = {
            "env_file": ["./client/.env"],
            "build": "./client",
            "environment": [f"CLIENT_ID={i}"],
            "depends_on": ["boundary"],
            "volumes": ["./client:/app"],
            "networks": {
                NETWORK: {
                    "ipv4_address": f"{client_ip_base}{100 + i}"  # Starting from 172.25.0.101, 102, etc.
                }
            },
        }
        
        # Add profiles for clients beyond the auto clients
        if i > auto_clients:
            client_config["profiles"] = ["manual"]
            
        services[f"client{i}"] = client_config
        
    return services
