#!/bin/bash

# Default output filename
OUTPUT_FILE="docker-compose-test.yaml"

# Function to display usage information
show_help() {
    echo "Usage: $0 [OPTIONS]"
    echo "Generate Docker Compose configuration file."
    echo ""
    echo "Options:"
    echo "  -o, --output FILE    Specify output filename (default: docker-compose-test.yaml)"
    echo "  -h, --help           Display this help message and exit"
    echo ""
    echo "Examples:"
    echo "  $0                   Generate using default filename"
    echo "  $0 -o my-compose.yaml   Generate with custom filename"
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

echo "Will generate Docker Compose file: $OUTPUT_FILE"

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
$PYTHON docker_compose_generator.py "$OUTPUT_FILE"

# Check if generation was successful
if [ -f "$OUTPUT_FILE" ]; then
    echo "Docker Compose file generated successfully at: $OUTPUT_FILE"
    show_docker_commands "$OUTPUT_FILE"
else
    echo "Error: Failed to generate Docker Compose file."
    exit 1
fi
