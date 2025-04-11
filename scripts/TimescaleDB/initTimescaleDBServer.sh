#!/bin/bash

WORKING_DIR="$1"

# Postgres-14 path definitions
PG_VERSION="14"
PG_CONF_DIR="/etc/postgresql/${PG_VERSION}/main"
PG_DATA_DIR="/var/lib/postgresql/${PG_VERSION}/main"

# Install sysstat, build-essential, g++, libpqxx-dev, libpq-dev
sudo apt install sysstat build-essential g++ libpqxx-dev libpq-dev -y

# Clean up any existing PostgreSQL installations
sudo systemctl stop postgresql@14-main || true
sudo systemctl disable postgresql@14-main || true
sudo systemctl reset-failed postgresql@14-main || true

# Remove packages with noninteractive frontend
echo "postgresql-14 postgresql-14/purge-data boolean true" | sudo debconf-set-selections
sudo DEBIAN_FRONTEND=noninteractive apt-get remove --purge postgresql* timescaledb* -y || true
sudo DEBIAN_FRONTEND=noninteractive apt-get autoremove -y

# Clean up directories
sudo rm -rf /var/lib/postgresql/
sudo rm -rf /var/log/postgresql/
sudo rm -rf /etc/postgresql/
sudo rm -rf /run/postgresql/
sudo rm -f /lib/systemd/system/postgresql@.service
sudo rm -rf ${WORKING_DIR}/postgresql/*
printf "\nRemoved existing PostgreSQL and TimescaleDB installations.\n\n"

# cd inproper working dir
cd ${WORKING_DIR}

# Add PostgreSQL repo
sudo apt install -y gnupg postgresql-common apt-transport-https lsb-release wget

# Run the PostgreSQL repo setup script
sudo /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y

# Add TimescaleDB repo
echo "deb https://packagecloud.io/timescale/timescaledb/ubuntu/ $(lsb_release -c -s) main" | sudo tee /etc/apt/sources.list.d/timescaledb.list
wget --quiet -O - https://packagecloud.io/timescale/timescaledb/gpgkey | sudo apt-key add -

# Update package lists
sudo apt update

# Install PostgreSQL and TimescaleDB
sudo apt install -y postgresql-14 postgresql-client-14 timescaledb-2-postgresql-14

# Configure TimescaleDB
sudo timescaledb-tune --quiet --yes

# Change PostgreSQL's default port to 9493 & bind to all interfaces
sudo sed -i "s/^port = [0-9]*/port = 9493/" ${PG_CONF_DIR}/postgresql.conf
sudo sed -i "s/#listen_addresses = 'localhost'/listen_addresses = '*'/" ${PG_CONF_DIR}/postgresql.conf
sleep 2

# Allow remote connections
echo "host all all 0.0.0.0/0 md5" | sudo tee -a ${PG_CONF_DIR}/pg_hba.conf

# If data will be stored on different drive
#  Otherwise, leave as default
if [ -d "$WORKING_DIR" ]; then
    # Create directory for PostgreSQL data
    mkdir -p ${WORKING_DIR}/postgresql
    sudo chmod -R 700 ${WORKING_DIR}/postgresql
    sudo chown -R postgres:postgres ${WORKING_DIR}/postgresql

    # Stop PostgreSQL service
    sudo systemctl stop postgresql@14-main
    sleep 10

    # Initialize the new data directory
    sudo -u postgres /usr/lib/postgresql/14/bin/initdb -D ${WORKING_DIR}/postgresql

    # Update data directory in PostgreSQL configuration
    sudo sed -i "s|data_directory = '${PG_DATA_DIR}'|data_directory = '${WORKING_DIR}/postgresql'|" ${PG_CONF_DIR}/postgresql.conf
    sleep 2
fi

# Set PostgreSQL password
sudo -u postgres psql -p 9493 -c "ALTER USER postgres WITH PASSWORD 'postgres';"
sleep 5

# Start PostgreSQL service
sudo systemctl start postgresql@14-main
sleep 10

# Create database and extension with error handling
sudo -u postgres psql -p 9493 -c "CREATE DATABASE bench_db;" || true
sudo -u postgres psql -p 9493 -d bench_db -c "CREATE EXTENSION IF NOT EXISTS timescaledb;" || true

# Restart PostgreSQL to reflect all changes
sudo systemctl restart postgresql@14-main

printf "\n\nTimescaleDB installation and configuration completed.\n\n"
