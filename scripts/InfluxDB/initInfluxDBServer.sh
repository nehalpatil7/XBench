WORKING_DIR="$1"

# Go to proper working dir
cd ${WORKING_DIR}

# Add Influx repo & Install
wget -q https://repos.influxdata.com/influxdata-archive_compat.key
echo '393e8779c89ac8d958f81f942f9ad7fb82a25e133faddaf92e15b16e6ac9ce4c influxdata-archive_compat.key' | sha256sum -c && cat influxdata-archive_compat.key | gpg --dearmor | sudo tee /etc/apt/trusted.gpg.d/influxdata-archive_compat.gpg > /dev/null
echo 'deb [signed-by=/etc/apt/trusted.gpg.d/influxdata-archive_compat.gpg] https://repos.influxdata.com/debian stable main' | sudo tee /etc/apt/sources.list.d/influxdata.list
sudo apt-get update && sudo apt-get install influxdb sysstat -y

# Run InfluxDB
sudo service influxdb start

# Change InfluxDB's default port to 9492 & bind to all interfaces
sudo sed -i 's/# bind-address = ":8086"/bind-address= ":9492"/' /etc/influxdb/influxdb.conf

# Disable cached memory limit
# sudo sed -i 's/# cache-max-memory-size = "1g"/cache-max-memory-size = 0/' /etc/influxdb/influxdb.conf

# If data will be stored on different drive
#  Otherwise, leave as default
if [ -d "$WORKING_DIR" ]; then
    # Create dirs for InfluxDB
    mkdir -p influxdb/meta
    mkdir -p influxdb/data
    mkdir -p influxdb/wal
    sudo chmod -R 777 ${WORKING_DIR}

    sedMetaTarget='s/\/var\/lib\/influxdb\/meta/\'"$WORKING_DIR\/influxdb\/meta"'/'
    sedDataTarget='s/\/var\/lib\/influxdb\/data/\'"$WORKING_DIR\/influxdb\/data"'/'
    sedWalTarget='s/\/var\/lib\/influxdb\/wal/\'"$WORKING_DIR\/influxdb\/wal"'/'
    
    sudo sed -i "${sedMetaTarget}" /etc/influxdb/influxdb.conf
    sudo sed -i "${sedDataTarget}" /etc/influxdb/influxdb.conf
    sudo sed -i "${sedWalTarget}" /etc/influxdb/influxdb.conf
fi

# Configure firewall
if command -p firewall-cmd >/dev/null 2>&1
  then
    sudo firewall-cmd --add-port 9492/tcp --permanent && sudo firewall-cmd --reload
fi

# Restart InfluxDB to reflect all changes
sudo service influxdb restart