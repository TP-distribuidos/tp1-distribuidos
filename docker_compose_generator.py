#!/usr/bin/env python3

import yaml
import os
import sys

from docker_compose_generator_files.client import generate_client_services
from docker_compose_generator_files.workers.filter_by_year import generate_filter_by_year_workers
from docker_compose_generator_files.routers.year_movies_router import generate_year_movies_router
from docker_compose_generator_files.routers.country_router import generate_country_router

    


def generate_docker_compose(output_file='docker-compose-test.yaml', num_clients=4, num_year_workers=2):
    # Start with an empty services dictionary
    services = {}

   # Add client services
   # TODO: Make the NUMBER_OF_CLIENTS_AUTOMATIC configurable
    NUMBER_OF_CLIENTS_AUTOMATIC = 3
    client_services = generate_client_services(num_clients, NUMBER_OF_CLIENTS_AUTOMATIC)
    services.update(client_services)
    
    # Add filter_by_year workers
    year_workers = generate_filter_by_year_workers(num_year_workers)
    services.update(year_workers)
    
    # Add year_movies_router
    year_router = generate_year_movies_router(num_year_workers) 
    services.update(year_router)
    
    # Add country_router
    country_router = generate_country_router(num_year_workers)
    services.update(country_router)
      
    # RabbitMQ service
    services["rabbitmq"] = {
        "image": "rabbitmq:3-management",
        "ports": ["5672:5672", "15672:15672"]
    }

    # Boundary service
    services["boundary"] = {
        "build": {
            "context": "./server",
            "dockerfile": "boundary/Dockerfile"
        },
        "env_file": ["./server/boundary/.env"],
        "environment": [
            "MOVIES_ROUTER_QUEUE=boundary_movies_router",
            "MOVIES_ROUTER_Q5_QUEUE=boundary_movies_Q5_router",
            "CREDITS_ROUTER_QUEUE=boundary_credits_router",
            "RATINGS_ROUTER_QUEUE=boundary_ratings_router"
        ],
        "depends_on": ["rabbitmq"],
        "ports": ["5000:5000"],
        "volumes": [
            "./server/boundary:/app",
            "./server/rabbitmq:/app/rabbitmq",
            "./server/common:/app/common"
        ]
    }

    services["join_credits_router"] = {
        "build": {
            "context": "./server",
            "dockerfile": "router/Dockerfile"
        },
        "env_file": ["./server/router/.env"],
        "environment": [
            "NUMBER_OF_PRODUCER_WORKERS=1",
            "INPUT_QUEUE=boundary_credits_router",
            "OUTPUT_QUEUES=join_credits_worker_1_credits,join_credits_worker_2_credits",
            "BALANCER_TYPE=round_robin"
        ],
        "depends_on": ["rabbitmq"],
        "volumes": [
            "./server/router:/app",
            "./server/rabbitmq:/app/rabbitmq",
            "./server/common:/app/common"
        ]
    }
    
    # JOIN CREDITS WORKERS
    for i in range(1, 3):
        services[f"join_credits_worker_{i}"] = {
            "build": {
                "context": "./server",
                "dockerfile": "worker/join_credits/Dockerfile"
            },
            "env_file": ["./server/worker/join_credits/.env"],
            "environment": [
                f"ROUTER_CONSUME_QUEUE_MOVIES=join_credits_worker_{i}_movies",
                f"ROUTER_CONSUME_QUEUE_CREDITS=join_credits_worker_{i}_credits",
                "ROUTER_PRODUCER_QUEUE=count_router"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/worker/join_credits:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ]
        }

    # FILTER BY COUNTRY WORKERS
    for i in range(1, 3):
        services[f"filter_by_country_worker_{i}"] = {
            "build": {
                "context": "./server",
                "dockerfile": "worker/filter_by_country/Dockerfile"
            },
            "env_file": ["./server/worker/filter_by_country/.env"],
            "environment": [
                f"ROUTER_CONSUME_QUEUE=filter_by_country_worker_{i}",
                "ROUTER_PRODUCER_QUEUE=join_movies_router"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/worker/filter_by_country:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ]
        }

    # Q3 SECTION
    services["join_ratings_router"] = {
        "build": {
            "context": "./server",
            "dockerfile": "router/Dockerfile"
        },
        "env_file": ["./server/router/.env"],
        "environment": [
            "NUMBER_OF_PRODUCER_WORKERS=1",
            "INPUT_QUEUE=boundary_ratings_router",
            "OUTPUT_QUEUES=join_ratings_worker_1_ratings,join_ratings_worker_2_ratings",
            "BALANCER_TYPE=round_robin"
        ],
        "depends_on": ["rabbitmq"],
        "volumes": [
            "./server/router:/app",
            "./server/rabbitmq:/app/rabbitmq",
            "./server/common:/app/common"
        ]
    }

    # JOIN RATINGS WORKERS
    for i in range(1, 3):
        services[f"join_ratings_worker_{i}"] = {
            "build": {
                "context": "./server",
                "dockerfile": "worker/join_ratings/Dockerfile"
            },
            "env_file": ["./server/worker/join_ratings/.env"],
            "environment": [
                f"ROUTER_CONSUME_QUEUE_MOVIES=join_ratings_worker_{i}_movies",
                f"ROUTER_CONSUME_QUEUE_RATINGS=join_ratings_worker_{i}_ratings",
                "ROUTER_PRODUCER_QUEUE=average_movies_by_rating_router"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/worker/join_ratings:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ]
        }

    # AVERAGE MOVIES BY RATING ROUTER
    services["average_movies_by_rating_router"] = {
        "build": {
            "context": "./server",
            "dockerfile": "router/Dockerfile"
        },
        "env_file": ["./server/router/.env"],
        "environment": [
            "NUMBER_OF_PRODUCER_WORKERS=2",
            "INPUT_QUEUE=average_movies_by_rating_router",
            "OUTPUT_QUEUES=[[\"average_movies_by_rating_worker_1\"],[\"average_movies_by_rating_worker_2\",\"average_movies_by_rating_worker_3\"]]",
            "BALANCER_TYPE=shard_by_ascii"
        ],
        "depends_on": ["rabbitmq"],
        "volumes": [
            "./server/router:/app",
            "./server/rabbitmq:/app/rabbitmq",
            "./server/common:/app/common"
        ]
    }

    # AVERAGE MOVIES BY RATING WORKERS
    for i in range(1, 4):
        services[f"average_movies_by_rating_worker_{i}"] = {
            "build": {
                "context": "./server",
                "dockerfile": "worker/average_movies_by_rating/Dockerfile"
            },
            "env_file": ["./server/worker/average_movies_by_rating/.env"],
            "environment": [
                f"ROUTER_CONSUME_QUEUE=average_movies_by_rating_worker_{i}",
                "ROUTER_PRODUCER_QUEUE=max_min_router"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/worker/average_movies_by_rating:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ]
        }

    # MAX MIN ROUTER
    services["max_min_router"] = {
        "build": {
            "context": "./server",
            "dockerfile": "router/Dockerfile"
        },
        "env_file": ["./server/router/.env"],
        "environment": [
            "NUMBER_OF_PRODUCER_WORKERS=3",
            "INPUT_QUEUE=max_min_router",
            "OUTPUT_QUEUES=[[\"max_min_worker_1\"],[\"max_min_worker_2\"]]",
            "BALANCER_TYPE=shard_by_ascii"
        ],
        "depends_on": ["rabbitmq"],
        "volumes": [
            "./server/router:/app",
            "./server/rabbitmq:/app/rabbitmq",
            "./server/common:/app/common"
        ]
    }

    # MAX MIN WORKERS
    for i in range(1, 3):
        services[f"max_min_worker_{i}"] = {
            "build": {
                "context": "./server",
                "dockerfile": "worker/max_min/Dockerfile"
            },
            "env_file": ["./server/worker/max_min/.env"],
            "environment": [
                f"ROUTER_CONSUME_QUEUE=max_min_worker_{i}",
                "ROUTER_PRODUCER_QUEUE=max_min_collector_router"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/worker/max_min:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ]
        }

    # MAX MIN COLLECTOR ROUTER
    services["max_min_collector_router"] = {
        "build": {
            "context": "./server",
            "dockerfile": "router/Dockerfile"
        },
        "env_file": ["./server/router/.env"],
        "environment": [
            "NUMBER_OF_PRODUCER_WORKERS=2",
            "INPUT_QUEUE=max_min_collector_router",
            "OUTPUT_QUEUES=collector_max_min_worker",
            "BALANCER_TYPE=round_robin"
        ],
        "depends_on": ["rabbitmq"],
        "volumes": [
            "./server/router:/app",
            "./server/rabbitmq:/app/rabbitmq",
            "./server/common:/app/common"
        ]
    }

    # COLLECTOR MAX MIN WORKER
    services["collector_max_min_worker"] = {
        "build": {
            "context": "./server",
            "dockerfile": "worker/collector_max_min/Dockerfile"
        },
        "env_file": ["./server/worker/collector_max_min/.env"],
        "environment": [
            "ROUTER_CONSUME_QUEUE=collector_max_min_worker",
            "RESPONSE_QUEUE=response_queue"
        ],
        "depends_on": ["rabbitmq"],
        "volumes": [
            "./server/worker/collector_max_min:/app",
            "./server/rabbitmq:/app/rabbitmq",
            "./server/common:/app/common"
        ]
    }

    # Q4 SECTION
    services["join_movies_router"] = {
        "build": {
            "context": "./server",
            "dockerfile": "router/Dockerfile"
        },
        "env_file": ["./server/router/.env"],
        "environment": [
            "NUMBER_OF_PRODUCER_WORKERS=2",
            "INPUT_QUEUE=join_movies_router",
            "OUTPUT_QUEUES=join_ratings_worker_1_movies,join_ratings_worker_2_movies,join_credits_worker_1_movies,join_credits_worker_2_movies",
            "EXCHANGE_TYPE=fanout",
            "EXCHANGE_NAME=join_router_exchange"
        ],
        "depends_on": ["rabbitmq"],
        "volumes": [
            "./server/router:/app",
            "./server/rabbitmq:/app/rabbitmq",
            "./server/common:/app/common"
        ]
    }

    # COUNT ROUTER
    services["count_router"] = {
        "build": {
            "context": "./server",
            "dockerfile": "router/Dockerfile"
        },
        "env_file": ["./server/router/.env"],
        "environment": [
            "NUMBER_OF_PRODUCER_WORKERS=2",
            "INPUT_QUEUE=count_router",
            "OUTPUT_QUEUES=[[\"count_worker_1\", \"count_worker_2\"],[\"count_worker_3\", \"count_worker_4\"]]",
            "BALANCER_TYPE=shard_by_ascii"
        ],
        "depends_on": ["rabbitmq"],
        "volumes": [
            "./server/router:/app",
            "./server/rabbitmq:/app/rabbitmq",
            "./server/common:/app/common"
        ]
    }

    # COUNT WORKERS
    for i in range(1, 5):
        services[f"count_worker_{i}"] = {
            "build": {
                "context": "./server",
                "dockerfile": "worker/count/Dockerfile"
            },
            "env_file": ["./server/worker/count/.env"],
            "environment": [
                f"ROUTER_CONSUME_QUEUE=count_worker_{i}",
                "ROUTER_PRODUCER_QUEUE=top_router"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/worker/count:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ]
        }

    # TOP ROUTER
    services["top_router"] = {
        "build": {
            "context": "./server",
            "dockerfile": "router/Dockerfile"
        },
        "env_file": ["./server/router/.env"],
        "environment": [
            "NUMBER_OF_PRODUCER_WORKERS=4",
            "INPUT_QUEUE=top_router",
            "OUTPUT_QUEUES=[[\"top_worker_1\"],[\"top_worker_2\"],[\"top_worker_3\"]]",
            "BALANCER_TYPE=shard_by_ascii"
        ],
        "depends_on": ["rabbitmq"],
        "volumes": [
            "./server/router:/app",
            "./server/rabbitmq:/app/rabbitmq",
            "./server/common:/app/common"
        ]
    }

    # TOP WORKERS
    for i in range(1, 4):
        services[f"top_worker_{i}"] = {
            "build": {
                "context": "./server",
                "dockerfile": "worker/top/Dockerfile"
            },
            "env_file": ["./server/worker/top/.env"],
            "environment": [
                f"ROUTER_CONSUME_QUEUE=top_worker_{i}",
                "ROUTER_PRODUCER_QUEUE=top_10_actors_collector_router"
            ],
            "depends_on": ["rabbitmq"],
            "volumes": [
                "./server/worker/top:/app",
                "./server/rabbitmq:/app/rabbitmq",
                "./server/common:/app/common"
            ]
        }

    # TOP 10 ACTORS COLLECTOR ROUTER
    services["top_10_actors_collector_router"] = {
        "build": {
            "context": "./server",
            "dockerfile": "router/Dockerfile"
        },
        "env_file": ["./server/router/.env"],
        "environment": [
            "NUMBER_OF_PRODUCER_WORKERS=3",
            "INPUT_QUEUE=top_10_actors_collector_router",
            "OUTPUT_QUEUES=collector_top_10_actors_worker",
            "BALANCER_TYPE=round_robin"
        ],
        "depends_on": ["rabbitmq"],
        "volumes": [
            "./server/router:/app",
            "./server/rabbitmq:/app/rabbitmq",
            "./server/common:/app/common"
        ]
    }

    # COLLECTOR TOP 10 ACTORS WORKER
    services["collector_top_10_actors_worker"] = {
        "build": {
            "context": "./server",
            "dockerfile": "worker/collector_top_10_actors/Dockerfile"
        },
        "env_file": ["./server/worker/collector_top_10_actors/.env"],
        "environment": [
            "ROUTER_CONSUME_QUEUE=collector_top_10_actors_worker",
            "RESPONSE_QUEUE=response_queue"
        ],
        "depends_on": ["rabbitmq"],
        "volumes": [
            "./server/worker/collector_top_10_actors:/app",
            "./server/rabbitmq:/app/rabbitmq",
            "./server/common:/app/common"
        ]
    }

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
        
    print(f"Docker Compose file generated successfully at {output_file} with {num_clients} client nodes and {num_year_workers} filter_by_year workers")

if __name__ == "__main__":
    # Process command line arguments
    output_file = 'docker-compose-test.yaml'
    num_clients = 4
    num_year_workers = 2
    
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
    
    # Generate the Docker Compose file
    generate_docker_compose(output_file, num_clients, num_year_workers)
