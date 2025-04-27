#!/bin/bash

BASE_DIR="1D_451-Cols_Experiments"
N_CLIENTS=4
ROWS_PER_CLIENT=25000

# Create folder structure (one-liners)
for w in UNARY_QUERY_SEQ UNARY_QUERY_RAND BATCH_QUERY_SEQ BATCH_QUERY_RAND UNARY_INSERT_SEQ UNARY_INSERT_RAND BATCH_INSERT_SEQ BATCH_INSERT_RAND; do for i in $(seq 0 $((N_CLIENTS-1))); do mkdir -p $BASE_DIR/$w/${w}_Node-$i; done; done

cd "$BASE_DIR"

# READ workloads (one-liners)
for i in $(seq 0 $((N_CLIENTS-1))); do OFFSET=$((i * ROWS_PER_CLIENT)); cd UNARY_QUERY_SEQ/UNARY_QUERY_SEQ_Node-$i; pwd; python3 ../../../XBench/DataManager/Parquet_OPS.py queryWorkload -t UNARY_SEQ -o $OFFSET -s 1000 -n $N_CLIENTS -st 946684800 -et 1006684800; cd ../../; done
for i in $(seq 0 $((N_CLIENTS-1))); do OFFSET=$((i * ROWS_PER_CLIENT)); cd UNARY_QUERY_RAND/UNARY_QUERY_RAND_Node-$i; pwd; python3 ../../../XBench/DataManager/Parquet_OPS.py queryWorkload -t UNARY_RAND -o $OFFSET -s 1000 -n $N_CLIENTS -st 946684800 -et 1006684800; cd ../../; done
for i in $(seq 0 $((N_CLIENTS-1))); do OFFSET=$((i * ROWS_PER_CLIENT)); cd BATCH_QUERY_SEQ/BATCH_QUERY_SEQ_Node-$i; pwd; python3 ../../../XBench/DataManager/Parquet_OPS.py queryWorkload -t BATCH_SEQ -o $OFFSET -s 1000 -n $N_CLIENTS -st 946684800 -et 1006684800 -b 10; cd ../../; done
for i in $(seq 0 $((N_CLIENTS-1))); do OFFSET=$((i * ROWS_PER_CLIENT)); cd BATCH_QUERY_RAND/BATCH_QUERY_RAND_Node-$i; pwd; python3 ../../../XBench/DataManager/Parquet_OPS.py queryWorkload -t BATCH_RAND -o $OFFSET -s 1000 -n $N_CLIENTS -st 946684800 -et 1006684800 -b 10; cd ../../; done

# WRITE workloads (one-liners)
for i in $(seq 0 $((N_CLIENTS-1))); do OFFSET=$((i * ROWS_PER_CLIENT)); cd UNARY_INSERT_SEQ/UNARY_INSERT_SEQ_Node-$i; pwd; python3 ../../../XBench/DataManager/Parquet_OPS.py insertWorkload -t UNARY_SEQ -o $OFFSET -s 1000 -n $N_CLIENTS -i ../../../blockDataset/blockSmall.parquet; cd ../../; done
for i in $(seq 0 $((N_CLIENTS-1))); do OFFSET=$((i * ROWS_PER_CLIENT)); cd UNARY_INSERT_RAND/UNARY_INSERT_RAND_Node-$i; pwd; python3 ../../../XBench/DataManager/Parquet_OPS.py insertWorkload -t UNARY_RAND -o $OFFSET -s 1000 -n $N_CLIENTS -i ../../../blockDataset/blockSmall.parquet; cd ../../; done
for i in $(seq 0 $((N_CLIENTS-1))); do OFFSET=$((i * ROWS_PER_CLIENT)); cd BATCH_INSERT_SEQ/BATCH_INSERT_SEQ_Node-$i; pwd; python3 ../../../XBench/DataManager/Parquet_OPS.py insertWorkload -t BATCH_SEQ -o $OFFSET -s 1000 -n $N_CLIENTS -i ../../../blockDataset/blockSmall.parquet -b 10; cd ../../; done
for i in $(seq 0 $((N_CLIENTS-1))); do OFFSET=$((i * ROWS_PER_CLIENT)); cd BATCH_INSERT_RAND/BATCH_INSERT_RAND_Node-$i; pwd; python3 ../../../XBench/DataManager/Parquet_OPS.py insertWorkload -t BATCH_RAND -o $OFFSET -s 1000 -n $N_CLIENTS -i ../../../blockDataset/blockSmall.parquet -b 10; cd ../../; done

cd ..
wait