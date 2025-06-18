import yaml
import sys


from docker_compose_generator_files.client.client import generate_client_services
from docker_compose_generator_files.workers.filter_by_year import generate_filter_by_year_workers, get_worker_hosts_and_ports as get_year_worker_hosts_and_ports
from docker_compose_generator_files.workers.filter_by_country import generate_filter_by_country_workers, get_worker_hosts_and_ports as get_country_worker_hosts_and_ports
from docker_compose_generator_files.workers.join_credits import generate_join_credits_workers, get_worker_hosts_and_ports as get_join_credits_worker_hosts_and_ports
from docker_compose_generator_files.workers.join_ratings import generate_join_ratings_workers, get_worker_hosts_and_ports as get_join_ratings_worker_hosts_and_ports
from docker_compose_generator_files.workers.average_movies_by_rating import generate_average_movies_by_rating_workers, get_total_workers, get_worker_hosts_and_ports as get_avg_rating_worker_hosts_and_ports
from docker_compose_generator_files.workers.count import generate_count_workers, get_total_workers as get_total_count_workers, get_worker_hosts_and_ports as get_count_worker_hosts_and_ports
from docker_compose_generator_files.workers.top import generate_top_workers, get_worker_hosts_and_ports as get_top_worker_hosts_and_ports
from docker_compose_generator_files.workers.max_min import generate_max_min_workers, get_worker_hosts_and_ports as get_max_min_worker_hosts_and_ports
from docker_compose_generator_files.workers.max_min_collector import generate_collector_max_min_worker, get_worker_host_and_port as get_collector_max_min_worker_host_and_port
from docker_compose_generator_files.workers.top_10_actors_collector import generate_collector_top_10_actors_worker, get_worker_host_and_port as get_collector_top_10_actors_worker_host_and_port
from docker_compose_generator_files.workers.sentiment_analysis import generate_sentiment_analysis_workers, get_worker_hosts_and_ports as get_sentiment_analysis_worker_hosts_and_ports
from docker_compose_generator_files.workers.average_sentiment import generate_average_sentiment_workers, get_worker_hosts_and_ports as get_avg_sentiment_worker_hosts_and_ports
from docker_compose_generator_files.workers.collector_average_sentiment import generate_collector_average_sentiment_worker, get_worker_host_and_port as get_collector_avg_sentiment_worker_host_and_port

from docker_compose_generator_files.routers.year_movies import generate_year_movies_router, get_router_host_and_port as get_year_movies_router_host_and_port
from docker_compose_generator_files.routers.country import generate_country_router, get_router_host_and_port as get_country_router_host_and_port
from docker_compose_generator_files.routers.join_movies import generate_join_movies_router, get_router_host_and_port as get_join_movies_router_host_and_port
from docker_compose_generator_files.routers.join_credits import generate_join_credits_router, get_router_host_and_port as get_credits_router_host_and_port
from docker_compose_generator_files.routers.join_ratings import generate_join_ratings_router, get_router_host_and_port as get_ratings_router_host_and_port
from docker_compose_generator_files.routers.average_movies_by_rating import generate_average_movies_by_rating_router, get_router_host_and_port as get_avg_rating_router_host_and_port
from docker_compose_generator_files.routers.max_min import generate_max_min_router, get_router_host_and_port as get_max_min_router_host_and_port
from docker_compose_generator_files.routers.max_min_collector import generate_max_min_collector_router, get_router_host_and_port as get_max_min_collector_router_host_and_port
from docker_compose_generator_files.routers.count import generate_count_router, get_router_host_and_port as get_count_router_host_and_port
from docker_compose_generator_files.routers.top import generate_top_router, get_router_host_and_port as get_top_router_host_and_port
from docker_compose_generator_files.routers.top_10_actors_collector import generate_top_10_actors_collector_router, get_router_host_and_port as get_top_10_actors_collector_router_host_and_port
from docker_compose_generator_files.routers.generate_movies_q5 import generate_movies_q5_router, get_router_host_and_port as get_movies_q5_router_host_and_port
from docker_compose_generator_files.routers.average_sentiment import generate_average_sentiment_router, get_router_host_and_port as get_avg_sentiment_router_host_and_port
from docker_compose_generator_files.routers.average_sentiment_collector import generate_average_sentiment_collector_router, get_router_host_and_port as get_avg_sentiment_collector_router_host_and_port
from docker_compose_generator_files.boundary.boundary import get_boundary_host_and_port

from docker_compose_generator_files.rabbitmq.rabbitmq import generate_rabbitmq_service
from docker_compose_generator_files.boundary.boundary import generate_boundary_service
from docker_compose_generator_files.sentinel.sentinel import generate_sentinel_service

from docker_compose_generator_files.constants import NETWORK, OUTPUT_FILE, NUMBER_OF_CLIENTS_AUTOMATIC



def generate_docker_compose(output_file='docker-compose-test.yaml', num_clients=4, num_year_workers=2, 
                           num_country_workers=2, num_join_credits_workers=2, num_join_ratings_workers=2,
                           avg_rating_shards=2, avg_rating_replicas=2, count_shards=2, 
                           count_workers_per_shard=2, num_top_workers=2, num_max_min_workers=2,
                           num_sentiment_workers=4, num_avg_sentiment_workers=2,
                           network=NETWORK, include_q5=False, sentinel_replicas=2):
    # Start with an empty services dictionary
    services = {}

   # Add client services
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
    
    # Conditionally add Q5 workers based on include_q5 flag
    if include_q5:
        # Add sentiment_analysis workers (Q5)
        sentiment_analysis_workers = generate_sentiment_analysis_workers(num_sentiment_workers)
        services.update(sentiment_analysis_workers)
        
        # Add average_sentiment workers (Q5)
        average_sentiment_workers = generate_average_sentiment_workers(num_avg_sentiment_workers)
        services.update(average_sentiment_workers)
        
        # Add collector_average_sentiment_worker (Q5)
        collector_avg_sentiment = generate_collector_average_sentiment_worker()
        services.update(collector_avg_sentiment)
    
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
    
    # Conditionally add Q5 routers based on include_q5 flag
    if include_q5:
        # Add movies_q5_router (Q5)
        movies_q5 = generate_movies_q5_router(num_sentiment_workers)
        services.update(movies_q5)
        
        # Add average_sentiment_router with num_sentiment_workers (Q5)
        avg_sentiment_router = generate_average_sentiment_router(num_sentiment_workers, num_avg_sentiment_workers)
        services.update(avg_sentiment_router)
        
        # Add average_sentiment_collector_router with num_avg_sentiment_workers (Q5)
        avg_sentiment_collector = generate_average_sentiment_collector_router(num_avg_sentiment_workers)
        services.update(avg_sentiment_collector)

    # Add RabbitMQ service  
    rabbitmq = generate_rabbitmq_service()
    services.update(rabbitmq)
    
    boundary = generate_boundary_service()
    services.update(boundary)


    # Add sentinels for all critical routers

    # 0. Add sentinel for boundary service
    boundary_host, boundary_port = get_boundary_host_and_port()
    sentinel_boundary = generate_sentinel_service("boundary",
                                                [boundary_host], 
                                                [boundary_port], 
                                                sentinel_replicas, 
                                                network)
    services.update(sentinel_boundary)
    
    # 1. Add sentinel for filter_by_year workers (existing)
    year_worker_hosts, year_worker_ports = get_year_worker_hosts_and_ports(num_year_workers)
    sentinel_filter_by_year = generate_sentinel_service("filter_by_year", 
                                                      year_worker_hosts, 
                                                      year_worker_ports, 
                                                      sentinel_replicas, 
                                                      network)
    services.update(sentinel_filter_by_year)
    
    # 2. Add sentinel for join_credits_router
    credits_router_host, credits_router_port = get_credits_router_host_and_port()
    sentinel_join_credits = generate_sentinel_service("join_credits_router", 
                                                   [credits_router_host], 
                                                   [credits_router_port], 
                                                   sentinel_replicas, 
                                                   network)
    services.update(sentinel_join_credits)
    
    # 3. Add sentinel for year_movies_router
    year_movies_router_host, year_movies_router_port = get_year_movies_router_host_and_port()
    sentinel_year_movies = generate_sentinel_service("year_movies_router", 
                                                  [year_movies_router_host], 
                                                  [year_movies_router_port], 
                                                  sentinel_replicas, 
                                                  network)
    services.update(sentinel_year_movies)
    
    # 4. Add sentinel for country_router
    country_router_host, country_router_port = get_country_router_host_and_port()
    sentinel_country = generate_sentinel_service("country_router", 
                                               [country_router_host], 
                                               [country_router_port], 
                                               sentinel_replicas, 
                                               network)
    services.update(sentinel_country)
    
    # 5. Add sentinel for join_ratings_router
    ratings_router_host, ratings_router_port = get_ratings_router_host_and_port()
    sentinel_join_ratings = generate_sentinel_service("join_ratings_router", 
                                                   [ratings_router_host], 
                                                   [ratings_router_port], 
                                                   sentinel_replicas, 
                                                   network)
    services.update(sentinel_join_ratings)
    
    # 6. Add sentinel for average_movies_by_rating_router
    avg_rating_router_host, avg_rating_router_port = get_avg_rating_router_host_and_port()
    sentinel_avg_rating = generate_sentinel_service("avg_movies_by_rating_router", 
                                                 [avg_rating_router_host], 
                                                 [avg_rating_router_port], 
                                                 sentinel_replicas, 
                                                 network)
    services.update(sentinel_avg_rating)
    
    # 7. Add sentinel for max_min_router
    max_min_router_host, max_min_router_port = get_max_min_router_host_and_port()
    sentinel_max_min = generate_sentinel_service("max_min_router", 
                                              [max_min_router_host], 
                                              [max_min_router_port], 
                                              sentinel_replicas, 
                                              network)
    services.update(sentinel_max_min)
    
    # 8. Add sentinel for count_router
    count_router_host, count_router_port = get_count_router_host_and_port()
    sentinel_count = generate_sentinel_service("count_router", 
                                            [count_router_host], 
                                            [count_router_port], 
                                            sentinel_replicas, 
                                            network)
    services.update(sentinel_count)

    # 9. Add sentinel for join_movies_router
    join_movies_router_host, join_movies_router_port = get_join_movies_router_host_and_port()
    sentinel_join_movies = generate_sentinel_service("join_movies_router", 
                                                  [join_movies_router_host], 
                                                  [join_movies_router_port], 
                                                  sentinel_replicas, 
                                                  network)
    services.update(sentinel_join_movies)
    
    # 10. Add sentinel for top_router
    top_router_host, top_router_port = get_top_router_host_and_port()
    sentinel_top = generate_sentinel_service("top_router", 
                                          [top_router_host], 
                                          [top_router_port], 
                                          sentinel_replicas, 
                                          network)
    services.update(sentinel_top)
    
    # 11. Add sentinel for top_10_actors_collector_router
    top_collector_router_host, top_collector_router_port = get_top_10_actors_collector_router_host_and_port()
    sentinel_top_collector = generate_sentinel_service("top_10_actors_collector_router", 
                                                    [top_collector_router_host], 
                                                    [top_collector_router_port], 
                                                    sentinel_replicas, 
                                                    network)
    services.update(sentinel_top_collector)
    
    # 12. Add sentinel for max_min_collector_router
    max_min_collector_router_host, max_min_collector_router_port = get_max_min_collector_router_host_and_port()
    sentinel_max_min_collector = generate_sentinel_service("max_min_collector_router", 
                                                        [max_min_collector_router_host], 
                                                        [max_min_collector_router_port], 
                                                        sentinel_replicas, 
                                                        network)
    services.update(sentinel_max_min_collector)
    
    # Conditionally add Q5 sentinels based on include_q5 flag
    if include_q5:
        # 13. Add sentinel for movies_q5_router
        movies_q5_router_host, movies_q5_router_port = get_movies_q5_router_host_and_port()
        sentinel_movies_q5 = generate_sentinel_service("movies_q5_router", 
                                                    [movies_q5_router_host], 
                                                    [movies_q5_router_port], 
                                                    sentinel_replicas, 
                                                    network)
        services.update(sentinel_movies_q5)
        
        # 14. Add sentinel for average_sentiment_router
        avg_sentiment_router_host, avg_sentiment_router_port = get_avg_sentiment_router_host_and_port()
        sentinel_avg_sentiment = generate_sentinel_service("average_sentiment_router", 
                                                        [avg_sentiment_router_host], 
                                                        [avg_sentiment_router_port], 
                                                        sentinel_replicas, 
                                                        network)
        services.update(sentinel_avg_sentiment)
        
        # 15. Add sentinel for average_sentiment_collector_router
        avg_sentiment_collector_router_host, avg_sentiment_collector_router_port = get_avg_sentiment_collector_router_host_and_port()
        sentinel_avg_sentiment_collector = generate_sentinel_service("average_sentiment_collector_router", 
                                                                  [avg_sentiment_collector_router_host], 
                                                                  [avg_sentiment_collector_router_port], 
                                                                  sentinel_replicas, 
                                                                  network)
        services.update(sentinel_avg_sentiment_collector)

    # Add worker sentinels
    
    # 16. Add sentinel for filter_by_country workers
    country_worker_hosts, country_worker_ports = get_country_worker_hosts_and_ports(num_country_workers)
    sentinel_filter_by_country = generate_sentinel_service("filter_by_country", 
                                                        country_worker_hosts, 
                                                        country_worker_ports, 
                                                        sentinel_replicas, 
                                                        network)
    services.update(sentinel_filter_by_country)
    
    # 17. Add sentinel for average_movies_by_rating workers
    avg_rating_worker_hosts, avg_rating_worker_ports = get_avg_rating_worker_hosts_and_ports(avg_rating_shards, avg_rating_replicas)
    sentinel_avg_rating_workers = generate_sentinel_service("avg_movies_by_rating_workers", 
                                                          avg_rating_worker_hosts, 
                                                          avg_rating_worker_ports, 
                                                          sentinel_replicas, 
                                                          network)
    services.update(sentinel_avg_rating_workers)
    
    # 18. Add sentinel for count workers
    count_worker_hosts, count_worker_ports = get_count_worker_hosts_and_ports(count_shards, count_workers_per_shard)
    sentinel_count_workers = generate_sentinel_service("count_workers", 
                                                    count_worker_hosts, 
                                                    count_worker_ports, 
                                                    sentinel_replicas, 
                                                    network)
    services.update(sentinel_count_workers)
    
    # Conditionally add Q5 worker sentinels based on include_q5 flag
    if include_q5:
        # 19. Add sentinel for average_sentiment workers
        avg_sentiment_worker_hosts, avg_sentiment_worker_ports = get_avg_sentiment_worker_hosts_and_ports(num_avg_sentiment_workers)
        sentinel_avg_sentiment_workers = generate_sentinel_service("average_sentiment_workers", 
                                                                avg_sentiment_worker_hosts, 
                                                                avg_sentiment_worker_ports, 
                                                                sentinel_replicas, 
                                                                network)
        services.update(sentinel_avg_sentiment_workers)
        
        # 20. Add sentinel for collector_average_sentiment worker
        collector_avg_sentiment_host, collector_avg_sentiment_port = get_collector_avg_sentiment_worker_host_and_port()
        sentinel_collector_avg_sentiment = generate_sentinel_service("collector_avg_sentiment_worker", 
                                                                   [collector_avg_sentiment_host], 
                                                                   [collector_avg_sentiment_port], 
                                                                   sentinel_replicas, 
                                                                   network)
        services.update(sentinel_collector_avg_sentiment)

     # 21. Add sentinel for top workers
    top_worker_hosts, top_worker_ports = get_top_worker_hosts_and_ports(num_top_workers)
    sentinel_top_workers = generate_sentinel_service("top_workers", 
                                                  top_worker_hosts, 
                                                  top_worker_ports, 
                                                  sentinel_replicas, 
                                                  network)
    services.update(sentinel_top_workers)
    
    # 22. Add sentinel for max_min workers
    max_min_worker_hosts, max_min_worker_ports = get_max_min_worker_hosts_and_ports(num_max_min_workers)
    sentinel_max_min_workers = generate_sentinel_service("max_min_workers", 
                                                      max_min_worker_hosts, 
                                                      max_min_worker_ports, 
                                                      sentinel_replicas, 
                                                      network)
    services.update(sentinel_max_min_workers)
    
    # 23. Add sentinel for join_ratings workers
    join_ratings_worker_hosts, join_ratings_worker_ports = get_join_ratings_worker_hosts_and_ports(num_join_ratings_workers)
    sentinel_join_ratings_workers = generate_sentinel_service("join_ratings_workers", 
                                                           join_ratings_worker_hosts, 
                                                           join_ratings_worker_ports, 
                                                           sentinel_replicas, 
                                                           network)
    services.update(sentinel_join_ratings_workers)
    
    # 24. Add sentinel for join_credits workers
    join_credits_worker_hosts, join_credits_worker_ports = get_join_credits_worker_hosts_and_ports(num_join_credits_workers)
    sentinel_join_credits_workers = generate_sentinel_service("join_credits_workers", 
                                                           join_credits_worker_hosts, 
                                                           join_credits_worker_ports, 
                                                           sentinel_replicas, 
                                                           network)
    services.update(sentinel_join_credits_workers)
    
    # 25. Add sentinel for collector_top_10_actors_worker
    collector_top_10_actors_worker_host, collector_top_10_actors_worker_port = get_collector_top_10_actors_worker_host_and_port()
    sentinel_collector_top_10_actors = generate_sentinel_service("collector_top_10_actors_worker", 
                                                              [collector_top_10_actors_worker_host], 
                                                              [collector_top_10_actors_worker_port], 
                                                              sentinel_replicas, 
                                                              network)
    services.update(sentinel_collector_top_10_actors)
    
    # 26. Add sentinel for collector_max_min_worker
    collector_max_min_worker_host, collector_max_min_worker_port = get_collector_max_min_worker_host_and_port()
    sentinel_collector_max_min = generate_sentinel_service("collector_max_min_worker", 
                                                        [collector_max_min_worker_host], 
                                                        [collector_max_min_worker_port], 
                                                        sentinel_replicas, 
                                                        network)
    services.update(sentinel_collector_max_min)
    
    # Conditionally add sentiment analysis workers sentinel based on include_q5 flag
    if include_q5:
        # 27. Add sentinel for sentiment_analysis workers
        sentiment_analysis_worker_hosts, sentiment_analysis_worker_ports = get_sentiment_analysis_worker_hosts_and_ports(num_sentiment_workers)
        sentinel_sentiment_analysis_workers = generate_sentinel_service("sentiment_analysis_workers", 
                                                                     sentiment_analysis_worker_hosts, 
                                                                     sentiment_analysis_worker_ports, 
                                                                     sentinel_replicas, 
                                                                     network)
        services.update(sentinel_sentiment_analysis_workers)

    networks = {
        network: {
            "driver": "bridge",
            "ipam": {
                "config": [{
                    "subnet": "172.25.0.0/16"
                }]
            }
        }
    }

    # Compile final docker-compose dictionary
    docker_compose = {"services": services, "networks": networks}
    
    # Write to the specified output file
    with open(output_file, 'w') as file:
        # Convert Python dictionary to YAML and write to file
        yaml.dump(docker_compose, file, default_flow_style=False)
        
    q5_status = "included" if include_q5 else "excluded"
    print(f"Docker Compose file generated successfully at {output_file}")
    print(f"Configuration: {num_clients} clients, {num_year_workers} year workers, " +
          f"{num_country_workers} country workers, {num_join_credits_workers} join_credits workers, " +
          f"{num_join_ratings_workers} join_ratings workers, {avg_rating_shards} avg_rating shards, " +
          f"{avg_rating_replicas} avg_rating replicas per shard, {count_shards} count shards, " +
          f"{count_workers_per_shard} count workers per shard, {num_top_workers} top workers, " +
          f"{num_max_min_workers} max_min workers, Q5 components {q5_status}, " + 
          f"{sentinel_replicas} sentinel replicas per service, network: {network}")

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
    num_top_workers = 2
    num_max_min_workers = 2
    num_sentiment_workers = 4
    num_avg_sentiment_workers = 2
    network = NETWORK
    include_q5 = False
    sentinel_replicas = 2

    
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

    if len(sys.argv) > 13:
        try:
            num_sentiment_workers = int(sys.argv[13])
            if num_sentiment_workers < 1:
                raise ValueError("Number of sentiment analysis workers must be positive")
        except ValueError:
            print("Error: Number of sentiment analysis workers must be a positive integer.")
            sys.exit(1)
            
    if len(sys.argv) > 14:
        try:
            num_avg_sentiment_workers = int(sys.argv[14])
            if num_avg_sentiment_workers < 1:
                raise ValueError("Number of average sentiment workers must be positive")
        except ValueError:
            print("Error: Number of average sentiment workers must be a positive integer.")
            sys.exit(1)
        
    # TODO: Add the network argument in each node
    if len(sys.argv) > 15:
        network = sys.argv[15]
    
    if len(sys.argv) > 16:
        include_q5_arg = sys.argv[16].lower()
        include_q5 = include_q5_arg in ('true', 't', 'yes', 'y', '1')
    
    # Get sentinel_replicas from 17th argument if provided
    if len(sys.argv) > 17:
        try:
            sentinel_replicas = int(sys.argv[17])
            if sentinel_replicas < 1:
                raise ValueError("Number of sentinel replicas must be positive")
        except ValueError:
            print("Error: Number of sentinel replicas must be a positive integer.")
            sys.exit(1)
    
    # Generate the Docker Compose file
    network = NETWORK
    generate_docker_compose(output_file, num_clients, num_year_workers, 
                           num_country_workers, num_join_credits_workers,
                           num_join_ratings_workers, avg_rating_shards,
                           avg_rating_replicas, count_shards, 
                           count_workers_per_shard, num_top_workers,
                           num_max_min_workers, num_sentiment_workers,
                           num_avg_sentiment_workers, network, include_q5,
                           sentinel_replicas)
