#!/bin/bash

# Function to restart a random Docker container
restart_random_container() {
    container_name=$(docker ps --format "{{.Names}}" | grep -v -e "rabbitmq" -e "client" | shuf -n 1)
    docker kill "$container_name"
    echo "$(TZ=UTC date '+%Y-%m-%d %H:%M:%S,%3N') | Killed: $container_name (Waited: $random_wait seconds)"
}

# Main script
wait_seconds=60         # Default wait time in seconds
percentage_range=0.3    # Default percentage range
if [ -n "$1" ]; then
    wait_seconds=$1     # Use provided wait time if available
fi
if [ -n "$2" ]; then
    percentage_range=$2  # Use provided percentage range if available
fi

random_wait=0
while true; do
    sleep "$random_wait"
    restart_random_container

    # Calculate the random wait time
    range=$(echo "$wait_seconds * $percentage_range" | bc)
    min_wait=$(echo "$wait_seconds - $range" | bc)
    max_wait=$(echo "$wait_seconds + $range" | bc)
    random_wait=$(awk "BEGIN{srand(); print ($max_wait - $min_wait) * rand() + $min_wait}")
done
