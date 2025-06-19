#!/bin/bash

GREEN='\033[0;32m'
YELLOW='\033[0;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Function to display help information
show_help() {
    echo "Usage: ./stop_demo.sh [OPTIONS]"
    echo
    echo "Stop the demo script running in the background."
    echo
    echo "Options:"
    echo "  -h, --help       Display this help message and exit"
    echo
    echo "Notes:"
    echo "  - Demo output is logged to: demo_output.log"
    echo "  - You can view logs in real-time with: tail -f demo_output.log"
    echo
    exit 0
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_help
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            echo "Use -h or --help to see available options"
            exit 1
            ;;
    esac
done

if [ -f ".demo.pid" ]; then
    DEMO_PID=$(cat .demo.pid)
    if ps -p $DEMO_PID > /dev/null 2>&1; then
        echo -e "${GREEN}Stopping demo script (PID: $DEMO_PID)...${NC}"
        kill $DEMO_PID
        echo -e "${GREEN}Demo script stopped.${NC}"
    else
        echo -e "${YELLOW}Demo script (PID: $DEMO_PID) is not running.${NC}"
    fi
    rm -f .demo.pid
else
    echo -e "${YELLOW}No demo script seems to be running (.demo.pid not found).${NC}"
fi
