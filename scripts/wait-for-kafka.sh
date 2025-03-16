#!/bin/bash

echo "Waiting for Kafka to be ready..."

# Maximum number of attempts
max_attempts=30
attempt=1

while [ $attempt -le $max_attempts ]; do
    if kafka-topics --bootstrap-server kafka:29092 --list >/dev/null 2>&1; then
        echo "Kafka is ready!"
        exit 0
    fi
    
    echo "Attempt $attempt of $max_attempts: Kafka is not ready yet..."
    sleep 2
    attempt=$((attempt + 1))
done

echo "Kafka did not become ready in time"
exit 1 