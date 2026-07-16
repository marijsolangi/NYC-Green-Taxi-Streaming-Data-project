#!/bin/bash
# =============================================================================
# NYC GREEN TAXI STREAMING - INFRASTRUCTURE SETUP
# Author: Muhammad Marij
# Email: mohammedmarij@gmail.com
# =============================================================================

# Step 1: Download the official docker-compose.yml from Redpanda blog
# (This is the reference file; our customized version is in the same directory)
curl -O https://raw.githubusercontent.com/redpanda-data-blog/2023-python-gsg/main/docker-compose.yml

# Step 2: Start all services in detached mode
# Services: Redpanda (Kafka), Flink JobManager, Flink TaskManager, PostgreSQL
docker-compose up -d

# Step 3: Verify all containers are running
echo "Checking container status..."
docker-compose ps

# Step 4: Wait for services to be ready
echo "Waiting for services to initialize (30 seconds)..."
sleep 30

# Step 5: Test Redpanda/Kafka connection
echo "Testing Kafka connection..."
docker exec -it redpanda rpk cluster info

# Step 6: Access points:
echo "========================================"
echo "Flink Web UI: http://localhost:8081"
echo "Redpanda Console: http://localhost:8082"
echo "PostgreSQL: localhost:5432 (user: postgres, pass: postgres)"
echo "========================================"
