sudo apt update

# Get DPDK.
wget https://fast.dpdk.org/rel/dpdk-20.11.3.tar.gz
tar -xvf https://fast.dpdk.org/rel/dpdk-20.11.3.tar.gz

# Install dependencies.
sudo apt install -y rdma-core
sudo apt install -y libibverbs-dev
sudo apt install -y libevent-dev

# Build DPDK.
cd dpdk-stable-20.11.3
meson setup build
cd build
ninja
sudo ninja install
