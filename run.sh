#!/bin/bash

# Colors for better readability
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Function to display help information
# Only updating the show_help function

show_help() {
    echo "Usage: ./run.sh [OPTIONS] [COMPOSE_FILE]"
    echo
    echo "Start Docker containers and optionally run the demo script."
    echo
    echo "Options:"
    echo "  --no-demo        Don't run the demo script"
    echo "  -h, --help       Display this help message and exit"
    echo
    echo "Examples:"
    echo "  ./run.sh                             # Run with default compose file and demo"
    echo "  ./run.sh --no-demo                   # Run without demo script"
    echo "  ./run.sh my-compose.yaml             # Run with custom compose file and demo"
    echo "  ./run.sh my-compose.yaml --no-demo   # Run with custom compose file without demo"
    echo
    echo "Notes:"
    echo "  - Demo output is logged to: demo_output.log"
    echo "  - You can view logs in real-time with: tail -f demo_output.log"
    echo "  - To stop just the demo script: ./stop_demo.sh"
    echo
    exit 0
}

# Default compose file
COMPOSE_FILE="docker-compose-test.yaml"
RUN_DEMO=true

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --no-demo)
            RUN_DEMO=false
            shift
            ;;
        -h|--help)
            show_help
            ;;
        *)
            # Assume any other argument is the compose file
            COMPOSE_FILE="$1"
            shift
            ;;
    esac
done

# Find compose file if it exists in the current directory
if [ -f "docker-compose.yaml" ] && [ "$COMPOSE_FILE" = "docker-compose-test.yaml" ]; then
    COMPOSE_FILE="docker-compose.yaml"
elif [ -f "docker-compose.yml" ] && [ "$COMPOSE_FILE" = "docker-compose-test.yaml" ]; then
    COMPOSE_FILE="docker-compose.yml"
fi

# Verify the file exists
if [ ! -f "$COMPOSE_FILE" ]; then
    echo -e "${YELLOW}Warning: File $COMPOSE_FILE not found. Using default: docker-compose-test.yaml${NC}"
    COMPOSE_FILE="docker-compose-test.yaml"
fi

# Inform user about what's happening
echo -e "${GREEN}Using compose file: $COMPOSE_FILE${NC}"
echo -e "${GREEN}Stopping any running containers...${NC}"

# Stop any running containers
docker compose -f "$COMPOSE_FILE" down

# Kill any existing demo script that might be running
if [ -f ".demo.pid" ]; then
    OLD_PID=$(cat .demo.pid)
    if ps -p $OLD_PID > /dev/null 2>&1; then
        echo -e "${YELLOW}Stopping previous demo script (PID: $OLD_PID)...${NC}"
        kill $OLD_PID 2>/dev/null || true
    fi
    rm -f .demo.pid
fi

# Clear the screen
clear

# Prepare for startup
echo -e "${GREEN}Starting services with docker compose...${NC}"

# Launch demo.sh if enabled
if [ "$RUN_DEMO" = true ]; then
    echo -e "${GREEN}Launching demo.sh in background...${NC}"
    bash demo.sh > demo_output.log 2>&1 &
    DEMO_PID=$!
    echo $DEMO_PID > .demo.pid
    echo -e "${GREEN}Demo script started with PID: $DEMO_PID${NC}"
    echo -e "${GREEN}To stop the demo script: kill $DEMO_PID${NC}"
    echo -e "${GREEN}Demo output is being logged to: demo_output.log${NC}"
    echo -e "${YELLOW}You can view the demo output in real-time with: tail -f demo_output.log${NC}"
else
    echo -e "${YELLOW}Demo script disabled with --no-demo flag${NC}"
fi

# Start Docker Compose in the foreground
docker compose -f "$COMPOSE_FILE" up --build

# When docker-compose is stopped (Ctrl+C), also stop the demo script if it was running
if [ "$RUN_DEMO" = true ] && [ -f ".demo.pid" ]; then
    DEMO_PID=$(cat .demo.pid)
    if ps -p $DEMO_PID > /dev/null 2>&1; then
        echo -e "${GREEN}Stopping demo script (PID: $DEMO_PID)...${NC}"
        kill $DEMO_PID 2>/dev/null || true
    fi
    rm -f .demo.pid
fi

echo -e "${GREEN}All processes have been stopped.${NC}"
