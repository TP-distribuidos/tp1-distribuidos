from docker_compose_generator_files.workers.count import generate_output_queues_config
from docker_compose_generator_files.constants import NETWORK

def generate_count_router(num_join_credits_workers=2, count_shards=2, count_workers_per_shard=2, network=NETWORK):
    """
    Generate the count_router service configuration for Docker Compose.
    
    Args:
        num_join_credits_workers (int): Number of upstream join_credits workers
                                    (used to set NUMBER_OF_PRODUCER_WORKERS)
        count_shards (int): Number of shards for downstream count workers
        count_workers_per_shard (int): Number of count workers per shard
        network (str): Name of the Docker network to use
        
    Returns:
        dict: Dictionary with count_router service configuration
    """
    # Generate the output queues configuration string with proper sharding
    output_queues = generate_output_queues_config(count_shards, count_workers_per_shard)
    
    # Base port for sentinel monitoring
    base_port = 9701
    
    return {
        "count_router": {
            "build": {
                "context": "./server",
                "dockerfile": "router/Dockerfile"
            },
            "env_file": ["./server/router/.env"],
            "ports": [
                f"{base_port}:{base_port}"
            ],
            "environment": [
                f"NUMBER_OF_PRODUCER_WORKERS={num_join_credits_workers}", 
                "INPUT_QUEUE=count_router",
                f"OUTPUT_QUEUES={output_queues}",
                "BALANCER_TYPE=shard_by_ascii",
                f"SENTINEL_PORT={base_port}"
            ],
            "depends_on": {
                "rabbitmq": {
                    "condition": "service_healthy"
                }
            },
            "volumes": [
                "./server/router:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common",
                "./server/persistence/count_router:/app/persistence"
            ],
            "networks": [network]
        }
    }

def get_router_host_and_port():
    """
    Get the hostname and port for the count_router.
    
    Returns:
        tuple: (hostname, port)
    """
    return "count_router", 9701
