#!/bin/bash

# Script to kill a Docker container by a partial name match.
# USAGE: ./kill_container.sh sentinel1-2

# Check if a container name argument is provided
if [ -z "$1" ]; then
  echo "Usage: $0 <partial_container_name>"
  exit 1
fi

PARTIAL_NAME="$1"

# ANSI Color Codes
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

# Find the container ID and full name
# We use a tab as a delimiter for easier parsing with awk
# We also add a unique marker "END_OF_LINE_MARKER" to handle cases where names might have spaces,
# though docker names usually don't. This is more robust.
# Check if we're explicitly trying to kill a sentinel container
if [[ "$PARTIAL_NAME" == sentinel_* ]]; then
  # Looking for a sentinel container
  CONTAINER_INFO=$(docker ps --format "{{.ID}}\t{{.Names}}\tEND_OF_LINE_MARKER" | grep -E "tp1-distribuidos-${PARTIAL_NAME}($|\t)")
else
  # Looking for a non-sentinel container - avoid matching sentinel prefixes
  CONTAINER_INFO=$(docker ps --format "{{.ID}}\t{{.Names}}\tEND_OF_LINE_MARKER" | grep -E "tp1-distribuidos-${PARTIAL_NAME}($|\t)" | grep -v "tp1-distribuidos-sentinel_")
fi

if [ -z "$CONTAINER_INFO" ]; then
  # If the first search failed, try a more permissive search
  if [[ "$PARTIAL_NAME" == sentinel_* ]]; then
    # For sentinel containers, do a more generic search
    CONTAINER_INFO=$(docker ps --format "{{.ID}}\t{{.Names}}\tEND_OF_LINE_MARKER" | grep "${PARTIAL_NAME}" | head -n 1)
  else
    # For non-sentinel containers, filter out sentinel containers
    CONTAINER_INFO=$(docker ps --format "{{.ID}}\t{{.Names}}\tEND_OF_LINE_MARKER" | grep "${PARTIAL_NAME}" | grep -v "sentinel" | head -n 1)
  fi
  
  if [ -z "$CONTAINER_INFO" ]; then
    echo -e "${RED}Error: Container with name containing '$PARTIAL_NAME' not found.${NC}"
    echo "Did you mean one of these running containers related to 'tp1-distribuidos'?"
    # List running container names that include "tp1-distribuidos", removing the "tp1-distribuidos-" prefix
    docker ps --filter name=tp1-distribuidos --format "{{.Names}}" | sed 's/tp1-distribuidos-//g' | sed 's/^ *//;s/ *$//'
    exit 1
  else
    echo -e "${GREEN}No exact match found, but found a container with a similar name.${NC}"
  fi
fi

# If multiple containers match, grep will return multiple lines.
# We'll take the first match. For more specific targeting, the user should provide a more unique name.
FIRST_MATCH=$(echo "$CONTAINER_INFO" | head -n 1)

CONTAINER_ID=$(echo "$FIRST_MATCH" | awk -F'\t' '{print $1}')
CONTAINER_NAME=$(echo "$FIRST_MATCH" | awk -F'\t' '{print $2}')

if [ -z "$CONTAINER_ID" ]; then
  # This case should ideally not be reached if CONTAINER_INFO was populated, but as a safeguard:
  echo -e "${RED}Error: Could not extract CONTAINER ID for '$PARTIAL_NAME'. Found info: $FIRST_MATCH${NC}"
  exit 1
fi

echo "Found container: Name='$CONTAINER_NAME', ID='$CONTAINER_ID'"
echo "Attempting to kill container $CONTAINER_ID ($CONTAINER_NAME) with SIGKILL..."

if docker kill -s SIGKILL "$CONTAINER_ID" > /dev/null; then # Redirect stdout to suppress ID output
  echo -e "${GREEN}Successfully sent SIGKILL to container $CONTAINER_ID ($CONTAINER_NAME).${NC}"
else
  echo -e "${RED}Error: Failed to kill container $CONTAINER_ID ($CONTAINER_NAME).${NC}"
  exit 1
fi

exit 0
