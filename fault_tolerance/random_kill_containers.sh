#!/bin/bash

# Script to randomly kill non-sentinel, non-monitoring, non-rabbitmq Docker containers 
# at random intervals between 5-10 seconds
# USAGE: ./random_kill_containers.sh

# ANSI Color Codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}Starting random container killer...${NC}"
echo -e "${BLUE}This script will randomly kill up to 2 non-sentinel/non-monitoring/non-rabbitmq containers${NC}"
echo -e "${BLUE}at random intervals between 5-10 seconds.${NC}"
echo -e "${BLUE}Press Ctrl+C to stop.${NC}"
echo ""

# Infinite loop
while true; do
    echo -e "${BLUE}$(date) - Finding available containers to kill...${NC}"
    
    # Get list of valid container IDs and names
    # Exclude sentinel, monitoring, and rabbitmq containers
    CONTAINERS=$(docker ps --format "{{.ID}}|{{.Names}}" | grep -v "sentinel" | grep -v "monitoring" | grep -v "rabbitmq")
    
    # Count available containers
    CONTAINER_COUNT=$(echo "$CONTAINERS" | wc -l)
    
    if [ -z "$CONTAINERS" ] || [ "$CONTAINER_COUNT" -eq 0 ]; then
        echo -e "${RED}No valid containers found to kill. Waiting for next cycle.${NC}"
    else
        echo -e "${GREEN}Found $CONTAINER_COUNT valid containers.${NC}"
        
        # Determine how many to kill (up to 2, but not more than available)
        TO_KILL=2
        if [ "$CONTAINER_COUNT" -lt "$TO_KILL" ]; then
            TO_KILL=$CONTAINER_COUNT
        fi
        
        echo -e "${YELLOW}Will kill $TO_KILL containers this cycle.${NC}"
        
        # Randomly select and kill containers
        SELECTED_CONTAINERS=$(echo "$CONTAINERS" | shuf -n "$TO_KILL")
        
        echo "$SELECTED_CONTAINERS" | while IFS='|' read -r ID NAME; do
            echo -e "${YELLOW}Attempting to kill container $ID ($NAME) with SIGKILL...${NC}"
            
            if docker kill -s SIGKILL "$ID" > /dev/null; then
                echo -e "${GREEN}Successfully sent SIGKILL to container $ID ($NAME).${NC}"
            else
                echo -e "${RED}Failed to kill container $ID ($NAME).${NC}"
            fi
        done
    fi
    
    # Generate a random wait time between 5 and 10 seconds
    WAIT_TIME=$((RANDOM % 6 + 5))
    echo -e "${BLUE}Waiting $WAIT_TIME seconds until next kill cycle...${NC}"
    echo "------------------------------------------------------"
    sleep $WAIT_TIME
done
