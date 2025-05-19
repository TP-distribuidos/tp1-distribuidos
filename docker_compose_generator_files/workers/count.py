from docker_compose_generator_files.constants import NETWORK

def calculate_workers_per_shard(num_shards=2, num_workers_per_shard=2):
    """
    Calculate the distribution of count workers per shard.
    
    Args:
        num_shards (int): Number of shards
        num_workers_per_shard (int): Target number of workers per shard
        
    Returns:
        list: List containing the number of workers for each shard
    """
    return [num_workers_per_shard] * num_shards

def generate_worker_queue_names(num_shards=2, num_workers_per_shard=2):
    """
    Generate queue names for count workers.
    
    Args:
        num_shards (int): Number of shards
        num_workers_per_shard (int): Target number of workers per shard
        
    Returns:
        list: List of queue names for all workers
    """
    workers_per_shard = calculate_workers_per_shard(num_shards, num_workers_per_shard)
    
    queues = []
    worker_id = 1
    
    for shard_id, worker_count in enumerate(workers_per_shard, 1):
        for _ in range(worker_count):
            queues.append(f"count_worker_{worker_id}")
            worker_id += 1
            
    return queues

def generate_output_queues_config(num_shards=2, num_workers_per_shard=2):
    """
    Generate the output queues configuration for the count_router.
    
    Args:
        num_shards (int): Number of shards
        num_workers_per_shard (int): Target number of workers per shard
        
    Returns:
        str: JSON-like string representation of sharded output queues
    """
    workers_per_shard = calculate_workers_per_shard(num_shards, num_workers_per_shard)
    
    shards_config = []
    worker_id = 1
    
    for worker_count in workers_per_shard:
        shard = []
        for _ in range(worker_count):
            shard.append(f"count_worker_{worker_id}")
            worker_id += 1
        
        # Format the shard as a JSON array string
        shard_str = '[' + ','.join([f'"{q}"' for q in shard]) + ']'
        shards_config.append(shard_str)
    
    # Combine all shards into the final config string
    output_queues = '[' + ','.join(shards_config) + ']'
    return output_queues

def generate_count_workers(num_shards=2, num_workers_per_shard=2, network=NETWORK):
    """
    Generate count worker services configuration for Docker Compose.
    
    Args:
        num_shards (int): Number of shards
        num_workers_per_shard (int): Target number of workers per shard
        network (str): Name of the Docker network to use
        
    Returns:
        dict: Dictionary with count worker service configurations
    """
    services = {}
    
    # Get all worker queue names
    queues = generate_worker_queue_names(num_shards, num_workers_per_shard)
    
    # Base port for sentinel monitoring
    base_port = 9065
    
    # Create a worker for each queue
    for i, queue in enumerate(queues):
        # Calculate unique port for each worker
        worker_port = base_port + i * 10
        
        services[queue] = {
            "build": {
                "context": "./server",
                "dockerfile": "worker/count/Dockerfile"
            },
            "depends_on": ["rabbitmq"],
            "ports": [
                f"{worker_port}:{worker_port}"
            ],
            "env_file": ["./server/worker/count/.env"],
            "environment": [
                f"ROUTER_CONSUME_QUEUE={queue}",
                "ROUTER_PRODUCER_QUEUE=top_router",
                f"SENTINEL_PORT={worker_port}"
            ],
            "volumes": [
                "./server/worker/count:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common",
                f"./server/wal/count_worker_{i}:/app/wal"
            ],
            "networks": [network]
        }
        
    return services

def get_total_workers(num_shards=2, num_workers_per_shard=2):
    """
    Calculate the total number of count workers.
    
    Args:
        num_shards (int): Number of shards
        num_workers_per_shard (int): Target number of workers per shard
        
    Returns:
        int: Total number of count workers
    """
    workers_per_shard = calculate_workers_per_shard(num_shards, num_workers_per_shard)
    return sum(workers_per_shard)

def get_worker_hosts_and_ports(num_shards=2, num_workers_per_shard=2):
    """
    Get the hostnames and ports for the count workers.
    
    Args:
        num_shards (int): Number of shards
        num_workers_per_shard (int): Number of workers per shard
        
    Returns:
        tuple: (list of hostnames, list of ports)
    """
    base_port = 9065
    queues = generate_worker_queue_names(num_shards, num_workers_per_shard)
    hosts = queues
    ports = [base_port + i * 10 for i in range(len(queues))]
    
    return hosts, ports
