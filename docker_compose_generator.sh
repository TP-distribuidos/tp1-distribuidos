#!/bin/bash

# Default values
OUTPUT_FILE="docker-compose-test.yaml"
NUM_CLIENTS=4
NUM_YEAR_WORKERS=2  # Default number of filter_by_year workers
NUM_COUNTRY_WORKERS=2  # Default number of filter_by_country workers

# Function to display usage information
show_help() {
    echo "Usage: $0 [OPTIONS]"
    echo "Generate Docker Compose configuration file."
    echo ""
    echo "Options:"
    echo "  -o, --output FILE     Specify output filename (default: docker-compose-test.yaml)"
    echo "  -c, --clients NUM     Specify number of client nodes to generate (default: 4)"
    echo "  -y, --year-workers NUM    Specify number of filter_by_year worker nodes (default: 2)"
    echo "  -n, --country-workers NUM Specify number of filter_by_country worker nodes (default: 2)"
    echo "  -h, --help            Display this help message and exit"
    echo ""
    echo "Examples:"
    echo "  $0                    Generate using defaults (4 clients, 2 year workers, 2 country workers)"
    echo "  $0 -o my-compose.yaml    Generate with custom filename"
    echo "  $0 -c 10              Generate with 10 client nodes"
    echo "  $0 -y 5 -n 4          Generate with 5 year workers and 4 country workers"
    echo "  $0 -c 6 -y 4 -n 3 -o prod.yaml Generate with custom settings"
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

echo "Will generate Docker Compose file: $OUTPUT_FILE with $NUM_CLIENTS client nodes, $NUM_YEAR_WORKERS filter_by_year workers, and $NUM_COUNTRY_WORKERS filter_by_country workers"

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
$PYTHON docker_compose_generator.py "$OUTPUT_FILE" "$NUM_CLIENTS" "$NUM_YEAR_WORKERS" "$NUM_COUNTRY_WORKERS"

# Check if generation was successful
if [ -f "$OUTPUT_FILE" ]; then
    echo "Docker Compose file generated successfully at: $OUTPUT_FILE"
    show_docker_commands "$OUTPUT_FILE"
else
    echo "Error: Failed to generate Docker Compose file."
    exit 1
fi
