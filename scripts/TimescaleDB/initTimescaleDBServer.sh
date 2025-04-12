#!/bin/bash

WORKING_DIR="$1"

# Postgres-14 path definitions
PG_CONF_DIR="/etc/postgresql/14/main"
PG_DATA_DIR="/var/lib/postgresql/14/main"
PG_HBA="${PG_CONF_DIR}/pg_hba.conf"

# Install sysstat, build-essential, g++, libpqxx-dev, libpq-dev
sudo apt install sysstat build-essential g++ libpqxx-dev libpq-dev -y

# Clean up any existing PostgreSQL installations (noninteractive)
sudo systemctl stop postgresql@14-main || true
sudo systemctl disable postgresql@14-main || true
sudo systemctl reset-failed postgresql@14-main || true
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
printf "\n[INFO $(date '+%Y-%m-%d %H:%M:%S')] Removed existing PostgreSQL and TimescaleDB installations.\n"







cd ${WORKING_DIR}
# Add PostgreSQL repo & setup
sudo apt install -y gnupg postgresql-common apt-transport-https lsb-release wget
sudo /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y

# Add TimescaleDB repo
echo "deb https://packagecloud.io/timescale/timescaledb/ubuntu/ $(lsb_release -c -s) main" | sudo tee /etc/apt/sources.list.d/timescaledb.list
wget --quiet -O - https://packagecloud.io/timescale/timescaledb/gpgkey | sudo apt-key add -

# Update package lists
sudo apt update
sleep 5

# Install PostgreSQL and TimescaleDB & timescaledb-tune
sudo apt install -y postgresql-14 postgresql-client-14 timescaledb-2-postgresql-14
sleep 5
sudo timescaledb-tune --quiet --yes
sudo systemctl restart postgresql@14-main
sleep 10







echo "[INFO $(date '+%Y-%m-%d %H:%M:%S')] Creating DB and user setup on default port (5432)..."
sudo -u postgres bash -c "cd /tmp && psql -c \"ALTER USER postgres WITH PASSWORD 'postgres';\""
sudo -u postgres bash -c "cd /tmp && psql -c \"CREATE DATABASE bench_db;\"" || true
sudo -u postgres bash -c "cd /tmp && psql -d bench_db -c \"CREATE EXTENSION IF NOT EXISTS timescaledb;\"" || true
sudo systemctl restart postgresql@14-main
sleep 10

# Change PostgreSQL's default port to 9493 & bind to all interfaces
sudo sed -i "s/^port = [0-9]*/port = 9493/" ${PG_CONF_DIR}/postgresql.conf
sudo sed -i "s/#listen_addresses = 'localhost'/listen_addresses = '*'/" ${PG_CONF_DIR}/postgresql.conf
sleep 5
cat <<EOF > ${PG_HBA}
local   all             postgres                                trust

# TYPE  DATABASE        USER            ADDRESS                 METHOD

# "local" is for Unix domain socket connections only
local   all             all                                     trust
# IPv4 local connections:
host    all             all             0.0.0.0/0               trust
# IPv6 local connections:
host    all             all             ::0/0                   trust
# Allow replication connections from localhost, by a user with the replication privilege.
local   replication     all                                     trust
host    replication     all             127.0.0.1/32            trust
host    replication     all             ::1/128                 trust
host    all             all             0.0.0.0/0               trust
EOF







# If data will be stored on different drive
if [ -d "$WORKING_DIR" ]; then
    mkdir -p ${WORKING_DIR}/postgresql
    sudo chmod -R 700 ${WORKING_DIR}/postgresql
    sudo chown -R postgres:postgres ${WORKING_DIR}/postgresql

    sudo systemctl stop postgresql@14-main
    sleep 10

    sudo -u postgres /usr/lib/postgresql/14/bin/initdb -D ${WORKING_DIR}/postgresql
    sudo sed -i "s|data_directory = '${PG_DATA_DIR}'|data_directory = '${WORKING_DIR}/postgresql'|" ${PG_CONF_DIR}/postgresql.conf
    sleep 2
fi







printf "\n[INFO $(date '+%Y-%m-%d %H:%M:%S')] Switching PostgreSQL auth to md5 for remote connections...\n"
# Backup pg_hba.conf & replace peer with md5
sudo cp "${PG_HBA}" "${PG_HBA}.bak"
sudo sed -i 's/^\(local\s\+all\s\+postgres\s\+\)peer/\1md5/' "$PG_HBA"
sudo sed -i 's/^\(local\s\+all\s\+all\s\+\)peer/\1md5/' "$PG_HBA"
sudo systemctl reload postgresql
sleep 10







# Final restart
sudo systemctl restart postgresql@14-main
printf "\n\nTimescaleDB installation and configuration completed.\n\n"
