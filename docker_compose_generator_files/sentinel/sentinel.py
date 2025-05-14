from docker_compose_generator_files.constants import NETWORK

def generate_sentinel_service(service_name, worker_hosts, worker_ports, num_replicas=2, network=NETWORK):
    """
    Generate a sentinel service configuration for Docker Compose.
    
    Args:
        service_name (str): Name of the service being monitored (e.g. "filter_by_year")
        worker_hosts (list): List of worker host names
        worker_ports (list): List of worker ports
        num_replicas (int): Number of sentinel replicas to deploy
        network (str): Name of the Docker network to use
        
    Returns:
        dict: Dictionary with sentinel service configuration
    """
    # Convert lists to comma-separated strings
    worker_hosts_str = ",".join(worker_hosts)
    worker_ports_str = ",".join([str(port) for port in worker_ports])
    
    sentinel_name = f"sentinel_{service_name}"
    
    return {
        sentinel_name: {
            "build": {
                "context": "./server",
                "dockerfile": "sentinel/Dockerfile"
            },
            "environment": [
                f"WORKER_HOSTS={worker_hosts_str}",
                f"WORKER_PORTS={worker_ports_str}",
                "CHECK_INTERVAL=5",
                f"SERVICE_NAME={sentinel_name}",
                "PEER_PORT=9010",
                "RESTART_ATTEMPTS=2",
                "RESTART_COOLDOWN=5",
                "COMPOSE_PROJECT_NAME=tp1-distribuidos"
            ],
            "depends_on": worker_hosts,
            "volumes": [
                "./server/sentinel:/app",
                "./server/common:/app/common",
                "/var/run/docker.sock:/var/run/docker.sock"
            ],
            "deploy": {
                "replicas": num_replicas
            },
            "networks": [network]
        }
    }
