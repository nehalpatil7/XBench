FILE_NAME="10Y_32-Cols_Experiments.tar.zst"
FOLDER_NAME=${FILE_NAME%.tar.zst}

# Preemptively remove old stuffs
rm -rf ${FOLDER_NAME} ${FILE_NAME}*

# Fetch experiment data from ObjectStore
wget -q https://chi.uc.chameleoncloud.org:7480/swift/v1/AUTH_e3c8a3b3b5a74e09b10957e3ad358e11/XStore_ExperimentDataset/${FILE_NAME}

# Deflate compressed dataset
tar -xvSf ${FILE_NAME} && rm ${FILE_NAME}