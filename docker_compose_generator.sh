#!/bin/bash

# Default values
OUTPUT_FILE="docker-compose-test.yaml"
NUM_CLIENTS=5
NUM_YEAR_WORKERS=2
NUM_COUNTRY_WORKERS=2
NUM_JOIN_CREDITS_WORKERS=2
NUM_JOIN_RATINGS_WORKERS=2
AVG_RATING_SHARDS=2
AVG_RATING_REPLICAS=2
COUNT_SHARDS=2
COUNT_WORKERS_PER_SHARD=2
NUM_TOP_WORKERS=2
NUM_MAX_MIN_WORKERS=2
NUM_SENTIMENT_WORKERS=4
NUM_AVG_SENTIMENT_WORKERS=2
NETWORK="tp_distribuidos"
INCLUDE_Q5="false"
SENTINEL_REPLICAS=2

# Function to display usage information
show_help() {
    echo "Usage: $0 [OPTIONS]"
    echo "Generate Docker Compose configuration file."
    echo ""
    echo "Options:"
    echo "  -o, --output FILE     Specify output filename (default: $OUTPUT_FILE)"
    echo "  -c, --clients NUM     Specify number of client nodes to generate (default: $NUM_CLIENTS)"
    echo "  -y, --year-workers NUM    Specify number of filter_by_year worker nodes (default: $NUM_YEAR_WORKERS)"
    echo "  -n, --country-workers NUM Specify number of filter_by_country worker nodes (default: $NUM_COUNTRY_WORKERS)"
    echo "  -j, --join-credits-workers NUM Specify number of join_credits worker nodes (default: $NUM_JOIN_CREDITS_WORKERS)"
    echo "  -r, --join-ratings-workers NUM Specify number of join_ratings worker nodes (default: $NUM_JOIN_RATINGS_WORKERS)"
    echo "  -a, --avg-rating-shards NUM Specify number of average_movies_by_rating shards (default: $AVG_RATING_SHARDS)"
    echo "  -b, --avg-rating-replicas NUM Specify replicas per avg_rating shard (default: $AVG_RATING_REPLICAS)"
    echo "  -d, --count-shards NUM Specify number of count worker shards (default: $COUNT_SHARDS)"
    echo "  -e, --count-workers-per-shard NUM Specify workers per count shard (default: $COUNT_WORKERS_PER_SHARD)"
    echo "  -t, --top-workers NUM Specify number of top workers (default: $NUM_TOP_WORKERS)"
    echo "  -m, --max-min-workers NUM Specify number of max_min workers (default: $NUM_MAX_MIN_WORKERS)"
    echo "  -s, --sentiment-workers NUM Specify number of sentiment analysis workers (default: $NUM_SENTIMENT_WORKERS)"
    echo "  -v, --avg-sentiment-workers NUM Specify number of average sentiment workers (default: $NUM_AVG_SENTIMENT_WORKERS)"
    echo "  -k, --network NAME     Specify Docker network name (default: $NETWORK)"
    echo "  -q, --include-q5       Include Q5 sentiment analysis components (default: $INCLUDE_Q5)"
    echo "  -S, --sentinel-replicas NUM Specify number of sentinel replicas per service (default: $SENTINEL_REPLICAS)"
    echo "  -h, --help             Display this help message and exit"
    
    echo ""
    echo "Examples:"
    echo "  $0                    Generate using defaults"
    echo "  $0 -m 3               Generate with 3 max_min workers"
    echo "  $0 -a 3 -b 2 -m 3     Configure with custom worker counts"
}


# Function to show Docker Compose commands after successful generation
show_docker_commands() {
    local compose_file=$1
    
    echo ""
    echo "========== DOCKER COMPOSE COMMANDS =========="
    echo "To use the generated Docker Compose file:"
    echo ""
    echo "Start services:"
    echo "  docker compose -f ${compose_file} up"
    echo ""
    echo "Start services in detached mode:"
    echo "  docker compose -f ${compose_file} up -d"
    echo ""
    echo "Build and start services:"
    echo "  docker compose -f ${compose_file} up --build"
    echo ""
    echo "Stop and remove services:"
    echo "  docker compose -f ${compose_file} down"
    echo ""
    echo "Complete reset (stop containers, remove volumes, rebuild):"
    echo "  docker compose -f ${compose_file} down -v && docker compose -f ${compose_file} up --build"
    echo ""
    echo "View logs:"
    echo "  docker compose -f ${compose_file} logs -f"
    echo "==========================================="
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -o|--output)
            OUTPUT_FILE="$2"
            shift 2
            ;;
        -c|--clients)
            NUM_CLIENTS="$2"
            # Validate that the input is a positive integer
            if ! [[ "$NUM_CLIENTS" =~ ^[0-9]+$ ]] || [ "$NUM_CLIENTS" -lt 1 ]; then
                echo "Error: Number of clients must be a positive integer."
                exit 1
            fi
            shift 2
            ;;
        -S|--sentinel-replicas)
            SENTINEL_REPLICAS="$2"
            if ! [[ "$SENTINEL_REPLICAS" =~ ^[0-9]+$ ]] || [ "$SENTINEL_REPLICAS" -lt 1 ]; then
                echo "Error: Number of sentinel replicas must be a positive integer."
                exit 1
            fi
            shift 2
            ;;
        -j|--join-credits-workers)
            NUM_JOIN_CREDITS_WORKERS="$2"
            # Validate that the input is a positive integer
            if ! [[ "$NUM_JOIN_CREDITS_WORKERS" =~ ^[0-9]+$ ]] || [ "$NUM_JOIN_CREDITS_WORKERS" -lt 1 ]; then
                echo "Error: Number of join credits workers must be a positive integer."
                exit 1
            fi
            shift 2
            ;;
        -r|--join-ratings-workers)
            NUM_JOIN_RATINGS_WORKERS="$2"
            # Validate that the input is a positive integer
            if ! [[ "$NUM_JOIN_RATINGS_WORKERS" =~ ^[0-9]+$ ]] || [ "$NUM_JOIN_RATINGS_WORKERS" -lt 1 ]; then
                echo "Error: Number of join ratings workers must be a positive integer."
                exit 1
            fi
            shift 2
            ;;
        -a|--avg-rating-shards)
            AVG_RATING_SHARDS="$2"
            # Validate that the input is a positive integer
            if ! [[ "$AVG_RATING_SHARDS" =~ ^[0-9]+$ ]] || [ "$AVG_RATING_SHARDS" -lt 1 ]; then
                echo "Error: Number of average_movies_by_rating shards must be a positive integer."
                exit 1
            fi
            shift 2
            ;;
        -b|--avg-rating-replicas)
            AVG_RATING_REPLICAS="$2"
            # Validate that the input is a positive integer
            if ! [[ "$AVG_RATING_REPLICAS" =~ ^[0-9]+$ ]] || [ "$AVG_RATING_REPLICAS" -lt 1 ]; then
                echo "Error: Number of average_movies_by_rating replicas per shard must be a positive integer."
                exit 1
            fi
            shift 2
            ;;
        -d|--count-shards)
            COUNT_SHARDS="$2"
            # Validate that the input is a positive integer
            if ! [[ "$COUNT_SHARDS" =~ ^[0-9]+$ ]] || [ "$COUNT_SHARDS" -lt 1 ]; then
                echo "Error: Number of count shards must be a positive integer."
                exit 1
            fi
            shift 2
            ;;
        -e|--count-workers-per-shard)
            COUNT_WORKERS_PER_SHARD="$2"
            # Validate that the input is a positive integer
            if ! [[ "$COUNT_WORKERS_PER_SHARD" =~ ^[0-9]+$ ]] || [ "$COUNT_WORKERS_PER_SHARD" -lt 1 ]; then
                echo "Error: Number of count workers per shard must be a positive integer."
                exit 1
            fi
            shift 2
            ;;
        -t|--top-workers)
            NUM_TOP_WORKERS="$2"
            # Validate that the input is a positive integer
            if ! [[ "$NUM_TOP_WORKERS" =~ ^[0-9]+$ ]] || [ "$NUM_TOP_WORKERS" -lt 1 ]; then
                echo "Error: Number of top workers must be a positive integer."
                exit 1
            fi
            shift 2
            ;;
        -y|--year-workers)
            NUM_YEAR_WORKERS="$2"
            # Validate that the input is a positive integer
            if ! [[ "$NUM_YEAR_WORKERS" =~ ^[0-9]+$ ]] || [ "$NUM_YEAR_WORKERS" -lt 1 ]; then
                echo "Error: Number of year workers must be a positive integer."
                exit 1
            fi
            shift 2
            ;;
        -n|--country-workers)
            NUM_COUNTRY_WORKERS="$2"
            # Validate that the input is a positive integer
            if ! [[ "$NUM_COUNTRY_WORKERS" =~ ^[0-9]+$ ]] || [ "$NUM_COUNTRY_WORKERS" -lt 1 ]; then
                echo "Error: Number of country workers must be a positive integer."
                exit 1
            fi
            shift 2
            ;;
        -m|--max-min-workers)
            NUM_MAX_MIN_WORKERS="$2"
            # Validate that the input is a positive integer
            if ! [[ "$NUM_MAX_MIN_WORKERS" =~ ^[0-9]+$ ]] || [ "$NUM_MAX_MIN_WORKERS" -lt 1 ]; then
                echo "Error: Number of max_min workers must be a positive integer."
                exit 1
            fi
            shift 2
            ;;
        -s|--sentiment-workers)
            NUM_SENTIMENT_WORKERS="$2"
            if ! [[ "$NUM_SENTIMENT_WORKERS" =~ ^[0-9]+$ ]] || [ "$NUM_SENTIMENT_WORKERS" -lt 1 ]; then
                echo "Error: Number of sentiment workers must be a positive integer."
                exit 1
            fi
            shift 2
            ;;
        -v|--avg-sentiment-workers)
            NUM_AVG_SENTIMENT_WORKERS="$2"
            if ! [[ "$NUM_AVG_SENTIMENT_WORKERS" =~ ^[0-9]+$ ]] || [ "$NUM_AVG_SENTIMENT_WORKERS" -lt 1 ]; then
                echo "Error: Number of average sentiment workers must be a positive integer."
                exit 1
            fi
            shift 2
            ;;
        -k|--network)
            NETWORK="$2"
            shift 2
            ;;
        -q|--include-q5)
            INCLUDE_Q5="true"
            shift
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

echo "Will generate Docker Compose file: $OUTPUT_FILE with:"
echo "  $NUM_CLIENTS client nodes"
echo "  $NUM_YEAR_WORKERS filter_by_year workers"
echo "  $NUM_COUNTRY_WORKERS filter_by_country workers"
echo "  $NUM_JOIN_CREDITS_WORKERS join_credits workers" 
echo "  $NUM_JOIN_RATINGS_WORKERS join_ratings workers"
echo "  $AVG_RATING_SHARDS average_movies_by_rating shards"
echo "  $AVG_RATING_REPLICAS average_movies_by_rating replicas per shard"
echo "  $COUNT_SHARDS count shards"
echo "  $COUNT_WORKERS_PER_SHARD count workers per shard"
echo "  $NUM_TOP_WORKERS top workers"
echo "  $NUM_MAX_MIN_WORKERS max_min workers"
echo "  $NUM_MAX_MIN_WORKERS max_min workers"
echo "  $NUM_SENTIMENT_WORKERS sentiment analysis workers"
echo "  $NUM_AVG_SENTIMENT_WORKERS average sentiment workers"
echo "  Include Q5 components: $INCLUDE_Q5"
echo "  $SENTINEL_REPLICAS sentinel replicas per service"
echo "  Network: $NETWORK (DEFAULTING to tp_distribuidos anyways because this is a WIP)"




# Check if Python is installed
if command -v python3 &>/dev/null; then
    PYTHON="python3"
elif command -v python &>/dev/null; then
    PYTHON="python"
else
    echo "Error: Python is not installed. Please install Python to run this script."
    exit 1
fi

# Check if PyYAML is installed
$PYTHON -c "import yaml" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "PyYAML is not installed. Installing..."
    pip install PyYAML
fi

echo "Generating Docker Compose file..."
$PYTHON docker_compose_generator.py "$OUTPUT_FILE" "$NUM_CLIENTS" "$NUM_YEAR_WORKERS" "$NUM_COUNTRY_WORKERS" \
"$NUM_JOIN_CREDITS_WORKERS" "$NUM_JOIN_RATINGS_WORKERS" "$AVG_RATING_SHARDS" "$AVG_RATING_REPLICAS" \
"$COUNT_SHARDS" "$COUNT_WORKERS_PER_SHARD" "$NUM_TOP_WORKERS" "$NUM_MAX_MIN_WORKERS" \
"$NUM_SENTIMENT_WORKERS" "$NUM_AVG_SENTIMENT_WORKERS" "$NETWORK" "$INCLUDE_Q5" "$SENTINEL_REPLICAS"


# Check if generation was successful
if [ -f "$OUTPUT_FILE" ]; then
    echo "Docker Compose file generated successfully at: $OUTPUT_FILE"
    show_docker_commands "$OUTPUT_FILE"
else
    echo "Error: Failed to generate Docker Compose file."
    exit 1
fi
