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
CONTAINER_INFO=$(docker ps --format "{{.ID}}\t{{.Names}}\tEND_OF_LINE_MARKER" | grep "$PARTIAL_NAME")

if [ -z "$CONTAINER_INFO" ]; then
  echo -e "${RED}Error: Container containing '$PARTIAL_NAME' not found.${NC}"
  echo "Did you mean one of these running containers related to 'fault_tolerance'?"
  # List running container names that include "fault_tolerance", removing the "fault_tolerance-" prefix
  docker ps --filter name=fault_tolerance --format "{{.Names}}" | sed 's/fault_tolerance-//g' | sed 's/^ *//;s/ *$//'
  exit 1
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
