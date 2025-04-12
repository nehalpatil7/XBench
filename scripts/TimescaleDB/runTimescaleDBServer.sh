#!/bin/bash

isPurge="$1"
WORKING_DIR="$2"

# Purge existing dataStore & Start server
if [ "$isPurge" == "true" ]; then
    sudo -u postgres bash -c "cd /tmp && psql -p 9493 -c \"DROP DATABASE IF EXISTS bench_db;\""
    sudo -u postgres bash -c "cd /tmp && psql -p 9493 -c \"CREATE DATABASE bench_db;\""
    sudo -u postgres bash -c "cd /tmp && psql -p 9493 -d bench_db -c \"CREATE EXTENSION IF NOT EXISTS timescaledb;\""
fi

# Stop PostgreSQL/TimescaleDB
sudo systemctl stop postgresql
sleep 5

# Clear caches
free > /dev/null && sync > /dev/null && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches' && free > /dev/null

# Start PostgreSQL/TimescaleDB
sudo systemctl start postgresql && sudo systemctl restart postgresql
sleep 10

printf "[INFO $(date '+%Y-%m-%d %H:%M:%S')] TimescaleDB server is ready."
