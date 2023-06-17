ip link set dev eth0 up
ifconfig eth0 10.212.84.119 netmask 255.255.255.0   # change IP addr to match with traces
arp -s 10.212.84.118 00:80:00:00:00:01  # change IP addr to match with traces
ifconfig
m5 checkpoint;

echo "Starting memcached server in Kernel mode"
memcached -p 0 -U 11211 -u root
# memcached -p 0 -U 11211 -u root -vvv
# tcpdump -i eth0 -X
