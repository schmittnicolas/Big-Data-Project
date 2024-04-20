#!/bin/bash
# This script builds and runs the bourse docker containers

# Exit script on first error
set -e

# Get the absolute directory of the script
BASE_DIR=$(realpath $(dirname "$0"))
echo "Running the bourse application from $BASE_DIR"

# Build the analyzer docker container
cd "$BASE_DIR/bourse/docker/analyzer"
make

# Build the dashboard docker container
cd "$BASE_DIR/bourse/docker/dashboard"
make

# Run the docker containers using docker-compose
cd "$BASE_DIR/bourse/docker"
docker compose --file docker-compose.yml up