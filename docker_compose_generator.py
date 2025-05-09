#!/usr/bin/env python3

import yaml
import sys

from docker_compose_generator_files.client.client import generate_client_services
from docker_compose_generator_files.workers.filter_by_year import generate_filter_by_year_workers
from docker_compose_generator_files.workers.filter_by_country import generate_filter_by_country_workers
from docker_compose_generator_files.workers.join_credits import generate_join_credits_workers
from docker_compose_generator_files.workers.join_ratings import generate_join_ratings_workers
from docker_compose_generator_files.workers.average_movies_by_rating import generate_average_movies_by_rating_workers, get_total_workers
from docker_compose_generator_files.workers.count import generate_count_workers, get_total_workers as get_total_count_workers
from docker_compose_generator_files.workers.top import generate_top_workers
from docker_compose_generator_files.workers.max_min import generate_max_min_workers
from docker_compose_generator_files.workers.max_min_collector import generate_collector_max_min_worker
from docker_compose_generator_files.workers.top_10_actors_collector import generate_collector_top_10_actors_worker
from docker_compose_generator_files.routers.year_movies import generate_year_movies_router
from docker_compose_generator_files.routers.country import generate_country_router
from docker_compose_generator_files.routers.join_movies import generate_join_movies_router
from docker_compose_generator_files.routers.join_credits import generate_join_credits_router
from docker_compose_generator_files.routers.join_ratings import generate_join_ratings_router
from docker_compose_generator_files.routers.average_movies_by_rating import generate_average_movies_by_rating_router
from docker_compose_generator_files.routers.max_min import generate_max_min_router
from docker_compose_generator_files.routers.max_min_collector import generate_max_min_collector_router
from docker_compose_generator_files.routers.count import generate_count_router
from docker_compose_generator_files.routers.top import generate_top_router
from docker_compose_generator_files.routers.top_10_actors_collector import generate_top_10_actors_collector_router
from docker_compose_generator_files.rabbitmq.rabbitmq import generate_rabbitmq_service
from docker_compose_generator_files.boundary.boundary import generate_boundary_service

OUTPUT_FILE = 'docker-compose-test.yaml'

def generate_docker_compose(output_file='docker-compose-test.yaml', num_clients=4, num_year_workers=2, 
                           num_country_workers=2, num_join_credits_workers=2, num_join_ratings_workers=2,
                           avg_rating_shards=2, avg_rating_replicas=2, count_shards=2, 
                           count_workers_per_shard=2, num_top_workers=3, num_max_min_workers=2):
    # Start with an empty services dictionary
    services = {}

    # Add client services
    NUMBER_OF_CLIENTS_AUTOMATIC = 3
    client_services = generate_client_services(num_clients, NUMBER_OF_CLIENTS_AUTOMATIC)
    services.update(client_services)
    
    # Add filter_by_year workers
    year_workers = generate_filter_by_year_workers(num_year_workers)
    services.update(year_workers)
    
    # Add filter_by_country workers
    country_workers = generate_filter_by_country_workers(num_country_workers)
    services.update(country_workers)
    
    # Add join_credits workers
    join_credits_workers = generate_join_credits_workers(num_join_credits_workers)
    services.update(join_credits_workers)
    
    # Add join_ratings workers
    join_ratings_workers = generate_join_ratings_workers(num_join_ratings_workers)
    services.update(join_ratings_workers)
    
    # Add average_movies_by_rating workers
    avg_rating_workers = generate_average_movies_by_rating_workers(avg_rating_shards, avg_rating_replicas)
    services.update(avg_rating_workers)
    
    # Add count workers
    count_workers = generate_count_workers(count_shards, count_workers_per_shard)
    services.update(count_workers)
    
    # Add top workers
    top_workers = generate_top_workers(num_top_workers)
    services.update(top_workers)
    
    # Add max_min workers
    max_min_workers = generate_max_min_workers(num_max_min_workers)
    services.update(max_min_workers)
    
    # Add collector_max_min_worker
    collector_max_min = generate_collector_max_min_worker()
    services.update(collector_max_min)
    
    # Add collector_top_10_actors_worker
    collector_top_10_actors = generate_collector_top_10_actors_worker()
    services.update(collector_top_10_actors)
    
    # Get the total number of average_movies_by_rating workers
    total_avg_rating_workers = get_total_workers(avg_rating_shards, avg_rating_replicas)
    
    # Get the total number of count workers
    total_count_workers = get_total_count_workers(count_shards, count_workers_per_shard)
    
    # Add year_movies_router
    year_router = generate_year_movies_router(num_year_workers) 
    services.update(year_router)
    
    # Add country_router with both worker counts
    country_router = generate_country_router(num_year_workers, num_country_workers)
    services.update(country_router)
    
    # Add join_movies_router with all worker counts
    join_movies = generate_join_movies_router(num_country_workers, num_join_credits_workers, num_join_ratings_workers)
    services.update(join_movies)
    
    # Add join_credits_router with join_credits workers count
    join_credits = generate_join_credits_router(num_join_credits_workers)
    services.update(join_credits)
    
    # Add join_ratings_router with join_ratings workers count
    join_ratings = generate_join_ratings_router(num_join_ratings_workers)
    services.update(join_ratings)
    
    # Add average_movies_by_rating_router with join_ratings workers count and sharding config
    avg_rating_router = generate_average_movies_by_rating_router(num_join_ratings_workers, avg_rating_shards, avg_rating_replicas)
    services.update(avg_rating_router)
    
    # Add max_min_router with the total average_movies_by_rating workers count and number of max_min workers
    max_min_router = generate_max_min_router(total_avg_rating_workers, num_max_min_workers)
    services.update(max_min_router)
    
    # Add max_min_collector_router with num_max_min_workers
    max_min_collector = generate_max_min_collector_router(num_max_min_workers)
    services.update(max_min_collector)
    
    # Add count_router with join_credits workers count and sharding config
    count_router = generate_count_router(num_join_credits_workers, count_shards, count_workers_per_shard)
    services.update(count_router)
    
    # Add top_router with total count workers
    top_router = generate_top_router(total_count_workers, num_top_workers)
    services.update(top_router)
    
    # Add top_10_actors_collector_router with num_top_workers
    top_collector = generate_top_10_actors_collector_router(num_top_workers)
    services.update(top_collector)

    # Add RabbitMQ service  
    rabbitmq = generate_rabbitmq_service()
    services.update(rabbitmq)
    
    # Add Boundary service
    boundary = generate_boundary_service()
    services.update(boundary)

    # Q5 SECTION
    services["movies_q5_router"] = {
        "build": {
            "context": "./server",
            "dockerfile": "router/Dockerfile"
        },
        "env_file": ["./server/router/.env"],
        "environment": [
            "NUMBER_OF_PRODUCER_WORKERS=1",
            "INPUT_QUEUE=boundary_movies_Q5_router",
            "OUTPUT_QUEUES=sentiment_analysis_worker_1,sentiment_analysis_worker_2",
            "BALANCER_TYPE=round_robin"
        ],
        "depends_on": ["rabbitmq"],
        "volumes": [
            "./server/router:/app",
            "./server/rabbitmq:/app/rabbitmq",
            "./server/common:/app/common"
        ]
    }

    # SENTIMENT ANALYSIS WORKERS
    for i in range(1, 3):
        services[f"sentiment_analysis_worker_{i}"] = {
            "build": {
                "context": "./server",
                "dockerfile": "worker/sentiment_analysis/Dockerfile"
            },
            "env_file": ["./server/worker/sentiment_analysis/.env"],
            "environment": [
                f"ROUTER_CONSUME_QUEUE=sentiment_analysis_worker_{i}",
                "ROUTER_PRODUCER_QUEUE=average_sentiment_router"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/worker/sentiment_analysis:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ],
            "deploy": {
                "resources": {
                    "limits": {
                        "memory": "2G"
                    }
                }
            }
        }

    # AVERAGE SENTIMENT ROUTER
    services["average_sentiment_router"] = {
        "build": {
            "context": "./server",
            "dockerfile": "router/Dockerfile"
        },
        "env_file": ["./server/router/.env"],
        "environment": [
            "NUMBER_OF_PRODUCER_WORKERS=2",
            "INPUT_QUEUE=average_sentiment_router",
            "OUTPUT_QUEUES=[[\"average_sentiment_worker_1\"],[\"average_sentiment_worker_2\"]]",
            "BALANCER_TYPE=shard_by_ascii"
        ],
        "depends_on": ["rabbitmq"],
        "volumes": [
            "./server/router:/app",
            "./server/rabbitmq:/app/rabbitmq",
            "./server/common:/app/common"
        ]
    }

    # AVERAGE SENTIMENT WORKERS
    for i in range(1, 3):
        services[f"average_sentiment_worker_{i}"] = {
            "build": {
                "context": "./server",
                "dockerfile": "worker/average_sentiment/Dockerfile"
            },
            "env_file": ["./server/worker/average_sentiment/.env"],
            "environment": [
                f"ROUTER_CONSUME_QUEUE=average_sentiment_worker_{i}",
                "ROUTER_PRODUCER_QUEUE=average_sentiment_collector_router"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/worker/average_sentiment:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ]
        }

    # AVERAGE SENTIMENT COLLECTOR ROUTER
    services["average_sentiment_collector_router"] = {
        "build": {
            "context": "./server",
            "dockerfile": "router/Dockerfile"
        },
        "env_file": ["./server/router/.env"],
        "environment": [
            "NUMBER_OF_PRODUCER_WORKERS=2",
            "INPUT_QUEUE=average_sentiment_collector_router",
            "OUTPUT_QUEUES=collector_average_sentiment_worker",
            "BALANCER_TYPE=round_robin"
        ],
        "depends_on": ["rabbitmq"],
        "volumes": [
            "./server/router:/app",
            "./server/rabbitmq:/app/rabbitmq",
            "./server/common:/app/common"
        ]
    }

    # COLLECTOR AVERAGE SENTIMENT WORKER
    services["collector_average_sentiment_worker"] = {
        "build": {
            "context": "./server",
            "dockerfile": "worker/collector_average_sentiment_worker/Dockerfile"
        },
        "env_file": ["./server/worker/collector_average_sentiment_worker/.env"],
        "environment": [
            "ROUTER_CONSUME_QUEUE=collector_average_sentiment_worker",
            "RESPONSE_QUEUE=response_queue"
        ],
        "depends_on": ["rabbitmq"],
        "volumes": [
            "./server/worker/collector_average_sentiment_worker:/app",
            "./server/rabbitmq:/app/rabbitmq",
            "./server/common:/app/common"
        ]
    }

    # Compile final docker-compose dictionary
    docker_compose = {"services": services}
    
    # Write to the specified output file
    with open(output_file, 'w') as file:
        # Convert Python dictionary to YAML and write to file
        yaml.dump(docker_compose, file, default_flow_style=False)
        
    print(f"Docker Compose file generated successfully at {output_file}")
    print(f"Configuration: {num_clients} clients, {num_year_workers} year workers, " +
          f"{num_country_workers} country workers, {num_join_credits_workers} join_credits workers, " +
          f"{num_join_ratings_workers} join_ratings workers, {avg_rating_shards} avg_rating shards, " +
          f"{avg_rating_replicas} avg_rating replicas per shard, {count_shards} count shards, " +
          f"{count_workers_per_shard} count workers per shard, {num_top_workers} top workers, " +
          f"and {num_max_min_workers} max_min workers")

if __name__ == "__main__":
    # Process command line arguments
    output_file = OUTPUT_FILE
    num_clients = 4
    num_year_workers = 2
    num_country_workers = 2
    num_join_credits_workers = 2
    num_join_ratings_workers = 2
    avg_rating_shards = 2
    avg_rating_replicas = 2
    count_shards = 2
    count_workers_per_shard = 2
    num_top_workers = 3
    num_max_min_workers = 2
    
    # Get output filename from first argument if provided
     # Get output filename from first argument if provided
    if len(sys.argv) > 1:
        output_file = sys.argv[1]
    
    # Get number of clients from second argument if provided
    if len(sys.argv) > 2:
        try:
            num_clients = int(sys.argv[2])
            if num_clients < 1:
                raise ValueError("Number of clients must be positive")
        except ValueError:
            print("Error: Number of clients must be a positive integer.")
            sys.exit(1)
    
    # Get number of year workers from third argument if provided
    if len(sys.argv) > 3:
        try:
            num_year_workers = int(sys.argv[3])
            if num_year_workers < 1:
                raise ValueError("Number of year workers must be positive")
        except ValueError:
            print("Error: Number of year workers must be a positive integer.")
            sys.exit(1)
    
    # Get number of country workers from fourth argument if provided
    if len(sys.argv) > 4:
        try:
            num_country_workers = int(sys.argv[4])
            if num_country_workers < 1:
                raise ValueError("Number of country workers must be positive")
        except ValueError:
            print("Error: Number of country workers must be a positive integer.")
            sys.exit(1)
            
    # Get number of join credits workers from fifth argument if provided
    if len(sys.argv) > 5:
        try:
            num_join_credits_workers = int(sys.argv[5])
            if num_join_credits_workers < 1:
                raise ValueError("Number of join credits workers must be positive")
        except ValueError:
            print("Error: Number of join credits workers must be a positive integer.")
            sys.exit(1)
            
    # Get number of join ratings workers from sixth argument if provided
    if len(sys.argv) > 6:
        try:
            num_join_ratings_workers = int(sys.argv[6])
            if num_join_ratings_workers < 1:
                raise ValueError("Number of join ratings workers must be positive")
        except ValueError:
            print("Error: Number of join ratings workers must be a positive integer.")
            sys.exit(1)
            
    # Get number of average_movies_by_rating shards from seventh argument if provided
    if len(sys.argv) > 7:
        try:
            avg_rating_shards = int(sys.argv[7])
            if avg_rating_shards < 1:
                raise ValueError("Number of average_movies_by_rating shards must be positive")
        except ValueError:
            print("Error: Number of average_movies_by_rating shards must be a positive integer.")
            sys.exit(1)
            
    # Get number of average_movies_by_rating replicas per shard from eighth argument if provided
    if len(sys.argv) > 8:
        try:
            avg_rating_replicas = int(sys.argv[8])
            if avg_rating_replicas < 1:
                raise ValueError("Number of average_movies_by_rating replicas per shard must be positive")
        except ValueError:
            print("Error: Number of average_movies_by_rating replicas per shard must be a positive integer.")
            sys.exit(1)

    # Get number of count shards from ninth argument if provided
    if len(sys.argv) > 9:
        try:
            count_shards = int(sys.argv[9])
            if count_shards < 1:
                raise ValueError("Number of count shards must be positive")
        except ValueError:
            print("Error: Number of count shards must be a positive integer.")
            sys.exit(1)
            
    # Get number of count workers per shard from tenth argument if provided
    if len(sys.argv) > 10:
        try:
            count_workers_per_shard = int(sys.argv[10])
            if count_workers_per_shard < 1:
                raise ValueError("Number of count workers per shard must be positive")
        except ValueError:
            print("Error: Number of count workers per shard must be a positive integer.")
            sys.exit(1)
            
    # Get number of top workers from eleventh argument if provided
    if len(sys.argv) > 11:
        try:
            num_top_workers = int(sys.argv[11])
            if num_top_workers < 1:
                raise ValueError("Number of top workers must be positive")
        except ValueError:
            print("Error: Number of top workers must be a positive integer.")
            sys.exit(1)
    
    if len(sys.argv) > 12:
        try:
            num_max_min_workers = int(sys.argv[12])
            if num_max_min_workers < 1:
                raise ValueError("Number of max_min workers must be positive")
        except ValueError:
            print("Error: Number of max_min workers must be a positive integer.")
            sys.exit(1)
    
    # Generate the Docker Compose file
    generate_docker_compose(output_file, num_clients, num_year_workers, 
                           num_country_workers, num_join_credits_workers,
                           num_join_ratings_workers, avg_rating_shards,
                           avg_rating_replicas, count_shards, 
                           count_workers_per_shard, num_top_workers,
                           num_max_min_workers)
