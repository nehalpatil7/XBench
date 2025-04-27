#!/bin/bash

# Parse command line arguments
BLOCK=false
while getopts "b" opt; do
  case $opt in
    b) BLOCK=true ;;
    \?) echo "Invalid option: -$OPTARG" >&2; exit 1 ;;
  esac
done

if [ "$BLOCK" = true ]; then
    SOURCE_DIR="1D_451-Cols_Experiments"
else
    SOURCE_DIR="10Y_32-Cols_Experiments"
fi

EXPERIMENT_TYPE="$1"
NODE_NUM="$2"
SERVER_ADDR="$3"
SERVER_PORT="$4"
DB_NAME="$5"
THREAD_COUNT="$6"
N_ITER="$7"
BATCH_ITER="$8"
INVOKE_AT="$9"
DEBUG="$10"

# Parsed EXPERIMENT_TYPE
WORKLOAD=$(echo $EXPERIMENT_TYPE | cut -d '_' -f 1)
PATTERN=$(echo $EXPERIMENT_TYPE | cut -d '_' -f 2)

tmux has-session 2>/dev/null

# IF there's a tmux session exists -> Kill
if [ $? == 0 ]; then
    tmux kill-server
fi

# Pre-emptively clean any .csv/.parquet files
rm -f *.csv *.parquet && sleep 5

# Clear caches
free > /dev/null && sync > /dev/null && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches' && free > /dev/null

# Copy data to home directory
cp -r $SOURCE_DIR/$WORKLOAD\_INSERT_$PATTERN/$WORKLOAD\_INSERT_$PATTERN\_Node-$NODE_NUM/BASIC-INSERT-$WORKLOAD-$PATTERN\_Client-* .

# Init tmux session
tmux new-session -d -s $DB_NAME -n Client

# Dispatch workload
if [ "$DEBUG" = "true" ] || [ "$DEBUG" = "1" ]; then
    tmux send-keys -t "Client" "./benchmarkClient insertBench -i $SERVER_ADDR -p $SERVER_PORT -d $DB_NAME -t $THREAD_COUNT -e $EXPERIMENT_TYPE -n $N_ITER -b $BATCH_ITER -ia $INVOKE_AT --debug && tmux kill-server" C-m
else
    tmux send-keys -t "Client" "./benchmarkClient insertBench -i $SERVER_ADDR -p $SERVER_PORT -d $DB_NAME -t $THREAD_COUNT -e $EXPERIMENT_TYPE -n $N_ITER -b $BATCH_ITER -ia $INVOKE_AT && tmux kill-server" C-m
fi