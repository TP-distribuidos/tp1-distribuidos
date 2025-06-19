from docker_compose_generator_files.constants import NETWORK

def calculate_workers_per_shard(num_shards=2, num_replicas=2):
    """
    Calculate the distribution of workers per shard.
    
    With even numbers of shards and replicas, distribution is equal.
    With odd numbers, the last shard gets fewer workers.
    
    Args:
        num_shards (int): Number of shards
        num_replicas (int): Target number of replicas per shard
        
    Returns:
        list: List containing the number of workers for each shard
    """
    workers_per_shard = []
    
    # Calculate total workers and default workers per shard
    total_workers = num_shards * num_replicas
    
    # If both numbers are even, equal distribution
    if num_shards % 2 == 0 and num_replicas % 2 == 0:
        workers_per_shard = [num_replicas] * num_shards
    else:
        # Basic distribution: all shards get num_replicas workers
        workers_per_shard = [num_replicas] * num_shards
        
        # If odd configuration, adjust the last shard
        if total_workers % num_shards != 0:
            # Last shard gets fewer workers
            workers_per_shard[-1] = num_replicas - (total_workers % num_shards != 0)
    
    return workers_per_shard

def generate_worker_queue_names(num_shards=2, num_replicas=2):
    """
    Generate queue names for average_movies_by_rating workers.
    
    Args:
        num_shards (int): Number of shards
        num_replicas (int): Target number of replicas per shard
        
    Returns:
        list: List of queue names for all workers
    """
    workers_per_shard = calculate_workers_per_shard(num_shards, num_replicas)
    
    queues = []
    worker_id = 1
    
    for shard_id, worker_count in enumerate(workers_per_shard, 1):
        for _ in range(worker_count):
            queues.append(f"average_movies_by_rating_worker_{worker_id}")
            worker_id += 1
            
    return queues

def generate_output_queues_config(num_shards=2, num_replicas=2):
    """
    Generate the output queues configuration for the average_movies_by_rating_router.
    
    Args:
        num_shards (int): Number of shards
        num_replicas (int): Target number of replicas per shard
        
    Returns:
        str: JSON-like string representation of sharded output queues
    """
    workers_per_shard = calculate_workers_per_shard(num_shards, num_replicas)
    
    shards_config = []
    worker_id = 1
    
    for worker_count in workers_per_shard:
        shard = []
        for _ in range(worker_count):
            shard.append(f"average_movies_by_rating_worker_{worker_id}")
            worker_id += 1
        
        # Format the shard as a JSON array string - removed the backslashes
        shard_str = '[' + ','.join([f'"{q}"' for q in shard]) + ']'
        shards_config.append(shard_str)
    
    # Combine all shards into the final config string
    output_queues = '[' + ','.join(shards_config) + ']'
    return output_queues

def generate_average_movies_by_rating_workers(num_shards=2, num_replicas=2, network=NETWORK):
    """
    Generate average_movies_by_rating worker services configuration for Docker Compose.
    
    Args:
        num_shards (int): Number of shards
        num_replicas (int): Target number of replicas per shard
        network (str): Name of the Docker network to use
        
    Returns:
        dict: Dictionary with average_movies_by_rating worker service configurations
    """
    services = {}
    
    # Get all worker queue names
    queues = generate_worker_queue_names(num_shards, num_replicas)
    
    # Base port for sentinel monitoring
    base_port = 9052
    
    # Create a worker for each queue
    for i, queue in enumerate(queues):
        worker_id = queue.split('_')[-1]  # Extract worker ID number
        
        # Calculate unique port for each worker
        worker_port = base_port + i * 10
        
        services[queue] = {
            "build": {
                "context": "./server",
                "dockerfile": "worker/average_movies_by_rating/Dockerfile"
            },
            "depends_on": {
                "rabbitmq": {
                    "condition": "service_healthy"
                }
            },
            "ports": [
                f"{worker_port}:{worker_port}"
            ],
            "env_file": ["./server/worker/average_movies_by_rating/.env"],
            "environment": [
                f"ROUTER_CONSUME_QUEUE={queue}",
                "ROUTER_PRODUCER_QUEUE=max_min_router",
                f"SENTINEL_PORT={worker_port}",
                f"NODE_ID={queue}_node"
            ],
            "volumes": [
                "./server/worker/average_movies_by_rating:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common",
                f"./server/persistence/average_movies_by_rating_worker_{i+1}:/app/persistence"
            ],
            "networks": [network]
        }
        
    return services

def get_total_workers(num_shards=2, num_replicas=2):
    """
    Calculate the total number of average_movies_by_rating workers.
    
    Args:
        num_shards (int): Number of shards
        num_replicas (int): Target number of replicas per shard
        
    Returns:
        int: Total number of workers
    """
    workers_per_shard = calculate_workers_per_shard(num_shards, num_replicas)
    return sum(workers_per_shard)

def get_worker_hosts_and_ports(num_shards=2, num_replicas=2):
    """
    Get the hostnames and ports for the average_movies_by_rating workers.
    
    Args:
        num_shards (int): Number of shards
        num_replicas (int): Number of replicas per shard
        
    Returns:
        tuple: (list of hostnames, list of ports)
    """
    base_port = 9052
    queues = generate_worker_queue_names(num_shards, num_replicas)
    hosts = queues
    ports = [base_port + i * 10 for i in range(len(queues))]
    
    return hosts, ports
