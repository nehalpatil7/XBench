SITE_NAME="$1"
DB_NAME="$2"
WORKING_DIR="$3"

urlTACC="https://chi.uc.chameleoncloud.org:7480/swift/v1/AUTH_e3c8a3b3b5a74e09b10957e3ad358e11/XStore_ExperimentDataset/${DB_NAME}_BENCH_DB.tar.zst"
urlUC="https://chi.uc.chameleoncloud.org:7480/swift/v1/AUTH_e3c8a3b3b5a74e09b10957e3ad358e11/XStore_ExperimentDataset/${DB_NAME}_BENCH_DB.tar.zst"

# Default download URL to TACC site
defaultURL=${urlUC}

if [ "$SITE_NAME" == "CHI@TACC" ]; then
    defaultURL=${urlTACC}
fi

cd ${WORKING_DIR}

# Intialize tmux
tmux new-session -d -s XStore -n RestoreDB

if [ "$DB_NAME" == "XStore" ]; then
    # Fetch experiment data from ObjectStore
    tmux send-keys -t "RestoreDB" "wget -qO- ${defaultURL} | tar xvS --zstd && sudo rm -rf XStore_Server/dataStore/BENCH_DB && mv BENCH_DB XStore_Server/dataStore && tmux kill-server" C-m

elif [ "$DB_NAME" == "MongoDB" ]; then
    # Purge existing dataStore
    mongosh --port 9491 quiet --eval "use BENCH_DB" --eval "db.dropDatabase()"

    # Fetch experiment data from ObjectStore
    tmux send-keys -t "RestoreDB" "wget -qO- ${defaultURL} | tar xvSO --zstd | mongorestore --host 0.0.0.0 --port 9491 --nsInclude=BENCH_DB.BENCH_DB --archive --drop && tmux kill-server" C-m

elif [ "$DB_NAME" == "InfluxDB" ]; then
    # Purge existing dataStore
    influx -port 9492 -execute "DROP DATABASE BENCH_DB"

    # Fetch experiment data from ObjectStore
    tmux send-keys -t "RestoreDB" "wget -qO- ${defaultURL} | tar xvS --zstd && influxd restore -portable InfluxDB_BENCH_DB && sudo rm -r InfluxDB_BENCH_DB && tmux kill-server" C-m

elif [ "$DB_NAME" == "TimescaleDB" ]; then
    # Drop and recreate the database using the 'postgres' user
    dropdb --if-exists -p 9493 -U postgres bench_db
    createdb -p 9493 -U postgres bench_db

    # Fetch experiment data and restore using pg_restore as 'postgres' user
    # Assumes the .tar.zst contains a single pg_dump custom format file
    tmux send-keys -t "RestoreDB" "wget -qO- ${defaultURL} | tar xvSO --zstd | pg_restore -p 9493 -U postgres --no-owner --no-privileges -d bench_db && tmux kill-server" C-m

fi