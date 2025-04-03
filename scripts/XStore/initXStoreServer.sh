WORKING_DIR="$1"

# Install dependencies of XStore
sudo apt install build-essential cmake g++ libzmq3-dev sysstat -y

# Go to proper working dir
cd ${WORKING_DIR}

# Clone XStore repo (Which includes client code as well)
sudo rm -rf XStore; git clone https://gitlab.com/lvn2007/XStore.git

# Install protobuf - v28.3
git clone https://github.com/protocolbuffers/protobuf.git; cd protobuf && git reset --hard 10a8f64e4ae93cbbdd4d7eed1d8b2f07ae769da8 && git submodule update --init --recursive && mkdir -p build && cd build && cmake .. -DCMAKE_BUILD_TYPE=Release -Dprotobuf_BUILD_SHARED_LIBS=ON -Dprotobuf_BUILD_TESTS=OFF && sudo make -j install && sudo ldconfig && cd ${WORKING_DIR}

# Compile
cd XStore && bash build.sh

# Configure firewall
if command -p firewall-cmd >/dev/null 2>&1
  then
    sudo firewall-cmd --add-port 9497/tcp --permanent && sudo firewall-cmd --reload
fi