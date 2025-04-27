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
    FILE_NAME="1D_451-Cols_Experiments.tar.zst"
else
    FILE_NAME="10Y_32-Cols_Experiments.tar.zst"
fi

FOLDER_NAME=${FILE_NAME%.tar.zst}

# Preemptively remove old stuffs
rm -rf ${FOLDER_NAME} ${FILE_NAME}*

# Fetch experiment data from ObjectStore
wget -q https://chi.uc.chameleoncloud.org:7480/swift/v1/AUTH_8d36e613c7d54f5bbbae886fb06d45e7/XStore_ExperimentDataset/${FILE_NAME}

# Deflate compressed dataset
tar -xvSf ${FILE_NAME} && rm ${FILE_NAME}