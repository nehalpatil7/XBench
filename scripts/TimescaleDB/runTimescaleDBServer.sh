#!/bin/bash

isPurge="$1"
WORKING_DIR="$2"

# Purge existing dataStore & Start server
if [ "$isPurge" == "true" ]; then
    echo "Purging existing dataStore & Starting TimescaleDB server..."
    # Drop the BENCH_DB database if it exists
    sudo -u postgres psql -p 9493 -c "DROP DATABASE IF EXISTS bench_db;"

    # Recreate the BENCH_DB database
    sudo -u postgres psql -p 9493 -c "CREATE DATABASE bench_db;"

    # Enable TimescaleDB extension in the new database
    sudo -u postgres psql -p 9493 -d bench_db -c "CREATE EXTENSION IF NOT EXISTS timescaledb;"
fi

# Stop PostgreSQL/TimescaleDB
sudo systemctl stop postgresql
sleep 5

# Clear caches
free > /dev/null && sync > /dev/null && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches' && free > /dev/null

# Start PostgreSQL/TimescaleDB
sudo systemctl start postgresql
sleep 10

echo "TimescaleDB server is ready."
