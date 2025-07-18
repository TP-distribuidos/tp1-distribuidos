#!/bin/bash

# Script to periodically kill specified Docker containers
# USAGE: ./demo.sh -t <seconds_interval> -n <num_containers_per_interval> -p prefix1,prefix2,...
# EXAMPLE: ./demo.sh -t 10 -n 3 -p max_min_worker,join_ratings_worker

# Default values
INTERVAL=10 # Default interval in seconds
NUM_CONTAINERS=5  # Default number of containers to kill per interval
PREFIXES=("max_min_worker" "average_movies_by_rating_worker" "collector_max_min_worker" "join_ratings_worker" "collector_top_10_actors_worker" "top_worker" "join_credits_worker" "count_worker" "filter_by_country_worker" "filter_by_year_worker" "collector_average_sentiment_worker" "average_sentiment_worker" "boundary" "sentinel") #Default prefixed, do not separate with commas.
# PREFIXES=("collector_average_sentiment_worker" "average_sentiment_worker") 

# ANSI Color Codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
ORANGE='\033[38;5;208m' 
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to display usage information
function show_usage {
  echo "Usage: $0 -t <seconds_interval> -n <num_containers_per_interval> [-p prefix1,prefix2,...]"
  echo ""
  echo "Options:"
  echo "  -t, --time       Time interval in seconds between kills (default: 30)"
  echo "  -n, --number     Number of containers to kill in each interval (default: 1)"
  echo "  -p, --prefixes   Comma-separated list of container prefixes to target (default: max_min_worker)"
  echo "  -h, --help       Show this help message"
  exit 1
}

# Parse command-line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    -t|--time)
      INTERVAL="$2"
      shift 2
      ;;
    -n|--number)
      NUM_CONTAINERS="$2"
      shift 2
      ;;
    -p|--prefixes)
      IFS=',' read -r -a PREFIXES <<< "$2"
      shift 2
      ;;
    -h|--help)
      show_usage
      ;;
    *)
      echo -e "${RED}Error: Unknown option $1${NC}"
      show_usage
      ;;
  esac
done

# Validate inputs
if ! [[ "$INTERVAL" =~ ^[0-9]+$ ]]; then
  echo -e "${RED}Error: Interval must be a positive integer${NC}"
  exit 1
fi

if ! [[ "$NUM_CONTAINERS" =~ ^[0-9]+$ ]]; then
  echo -e "${RED}Error: Number of containers must be a positive integer${NC}"
  exit 1
fi

if [ ${#PREFIXES[@]} -eq 0 ]; then
  echo -e "${RED}Error: At least one prefix must be specified${NC}"
  exit 1
fi

# Function to get a list of running containers matching the prefixes
function get_matching_containers {
  local containers=""
  
  # First, get a list of all running containers related to tp1-distribuidos
  all_containers=$(docker ps --filter name=tp1-distribuidos --format "{{.Names}}" | sed 's/tp1-distribuidos-//g' | sed 's/^ *//;s/ *$//')
  
  # Filter containers based on prefixes
  for prefix in "${PREFIXES[@]}"; do
    matching=$(echo "$all_containers" | grep "^$prefix")
    if [ ! -z "$matching" ]; then
      if [ -z "$containers" ]; then
        containers="$matching"
      else
        containers="$containers
$matching"
      fi
    fi
  done
  
  echo "$containers"
}

# Function to check if a sentinel container is the last of its group
function is_last_sentinel_of_group {
  local container="$1"
  
  # Skip if not a sentinel container
  if [[ "$container" != sentinel_* ]]; then
    return 1 # Not a sentinel, so not the last one
  fi
  
  # Extract the sentinel group name (everything before the last dash)
  local group_name=$(echo "$container" | sed -E 's/(.+)-[0-9]+$/\1/')
  
  # Count how many containers of this group are running
  local count=$(docker ps --format "{{.Names}}" | grep "tp1-distribuidos-$group_name-" | wc -l | tr -d ' ')
  
  # If count is 1, this is the last one
  if [ "$count" -eq 1 ]; then
    return 0 # True, this is the last one
  else
    return 1 # False, not the last one
  fi
}

# Main execution loop
echo -e "${GREEN}Starting periodic container kill script${NC}"
echo -e "${GREEN}Interval: ${INTERVAL} seconds | Containers per interval: ${NUM_CONTAINERS} | Targeting prefixes: ${PREFIXES[*]}${NC}"
echo -e "${YELLOW}Press Ctrl+C to stop the script${NC}"
echo

while true; do
  # Get matching containers
  matching_containers=$(get_matching_containers)
  
  if [ -z "$matching_containers" ]; then
    echo -e "${YELLOW}No matching containers found. Waiting for next interval...${NC}"
  else
    # Count how many containers we found
    container_count=$(echo "$matching_containers" | wc -l | tr -d ' ')
    
    # Determine how many to kill (min of NUM_CONTAINERS and actual count)
    to_kill=$NUM_CONTAINERS
    if [ $container_count -lt $NUM_CONTAINERS ]; then
      to_kill=$container_count
    fi
    
    # Kill the containers
    echo -e "${GREEN}Killing $to_kill containers${NC}"
    
    # Take the first $to_kill containers
    selected_containers=$(echo "$matching_containers" | head -n $to_kill)
    
    killed_count=0
    while read -r container; do
      if [ ! -z "$container" ]; then
        # Check if this is the last sentinel of its group
        if is_last_sentinel_of_group "$container"; then
          echo -e "${BLUE}Skipping ${ORANGE}$container${BLUE} as it's the last sentinel of its group${NC}"
          continue
        fi
        
        echo -e "${YELLOW}Killing container: $container${NC}"
        
        # Modify kill_container.sh output handling
        output=$(./kill_container.sh "$container" 2>&1)
        
        # Process and reformat the output
        if echo "$output" | grep -q "Found container:"; then
          container_name=$(echo "$output" | grep "Found container:" | sed -E 's/.*tp1-distribuidos-([^,]*).*/\1/')
          echo -e "Found container: Name='tp1-distribuidos-${ORANGE}${container_name}${NC}'"
        fi
        
        if echo "$output" | grep -q "Successfully sent SIGKILL"; then
          echo -e "${GREEN}Successfully sent SIGKILL to container${NC}"
          killed_count=$((killed_count + 1))
        fi
      fi
    done <<< "$selected_containers"
    
    echo -e "${GREEN}Killed $killed_count containers this cycle${NC}"
  fi
  
  # Wait for the next interval
  echo -e "Waiting for $INTERVAL seconds until next kill cycle..."
  echo  # Keep this blank line to separate cycles
  sleep $INTERVAL
done