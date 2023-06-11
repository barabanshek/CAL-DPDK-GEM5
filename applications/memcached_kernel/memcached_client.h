#ifndef _MEMCACHED_CLIENT_H_
#define _MEMCACHED_CLIENT_H_

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>

#ifdef _USE_DPDK_CLIENT_
  #include <dpdk.h>
#endif

#include <helpers.h>

static constexpr size_t kClSize = 64;
static constexpr size_t kMaxPacketSize = 1500;
static constexpr size_t kSockTimeout = 1;  // sec.

class MemcachedClient {
 public:
  enum Status {
    kOK = 0x0,
    kKeyNotFound = 0x1,
    kKeyExists = 0x2,
    kValueTooLarge = 0x3,
    kInvalidArgument = 0x4,
    kItemNotStored = 0x5,
    kNotAValue = 0x6,
    kUnknownComand = 0x81,
    kOutOfMemory = 0x82,
    kOtherError = 0xff
  };

  enum DispatchMode { kBlocking = 0x0, kNonBlocking = 0x1 };

#ifdef _USE_DPDK_CLIENT_
  // Constructor for DPDK networking.
  MemcachedClient(const std::string& server_mac_addr)
      : serverMacAddrStr(server_mac_addr),
        tx_buff(nullptr),
        dispatchMode(DispatchMode::kBlocking),
        runRecvThread(false) {}
#else
  // Constructor for Kernel networking.
  MemcachedClient(const std::string &server_hostname, uint16_t port)
      : serverHostname(server_hostname),
        port(port),
        sock(-1),
        rx_buff(nullptr),
        tx_buff(nullptr),
        dispatchMode(DispatchMode::kBlocking),
        runRecvThread(false) {}
#endif

  ~MemcachedClient() {
    if (dispatchMode == DispatchMode::kNonBlocking) {
      if (runRecvThread) {
        runRecvThread = false;
        recvThread.join();
      }
    }

#ifndef _USE_DPDK_CLIENT_
    if (sock != -1) close(sock);
    if (rx_buff != nullptr) std::free(rx_buff);
#endif

    if (tx_buff != nullptr) std::free(tx_buff);
  }

  int Init() {
    // Init networking.
#ifdef _USE_DPDK_CLIENT_
    std::cout << "Initializing Kernel-bypass (DPDK) networking" << std::endl;
    int ret = rte_ether_unformat_addr(serverMacAddrStr.c_str(), &serverMacAddr);
    if (ret) {
      std::cerr << "Wrong server MAC address format." << std::endl;
      return -1;
    }

    ret = InitDPDK(&dpdkObj);
    if (ret) {
      std::cerr << "Failed to initialize network!" << std::endl;
      return -1;
    }
#else
    std::cout << "Initializing Kernel (socket) networking" << std::endl;
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port = htons(port);
    if (inet_pton(AF_INET, serverHostname.c_str(), &(serverAddress.sin_addr)) <=
        0) {
      std::cerr << "Invalid address or address not supported." << std::endl;
      return -1;
    }

    std::cout << serverHostname.c_str() << "\n";

    sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock == -1) {
      std::cerr << "Failed to create socket." << std::endl;
      return -1;
    }

    // Set recv. timeout on sock.
    struct timeval tv;
    tv.tv_sec = kSockTimeout;
    tv.tv_usec = 0;
    if (setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
      std::cerr << "Faile to set socket timeout." << std::endl;
      return -1;
    }
#endif

    // Init buffers.
    tx_buff =
        static_cast<uint8_t *>(std::aligned_alloc(kClSize, kMaxPacketSize));
    assert(tx_buff != nullptr);

#ifndef _USE_DPDK_CLIENT_
    rx_buff =
        static_cast<uint8_t *>(std::aligned_alloc(kClSize, kMaxPacketSize));
    assert(rx_buff != nullptr);
#endif

    return 0;
  }

  // Set dispatch mode: if going from blocking (default) to non-blocking,
  // spin-up recv thread; if going from non-blocking to blocking, stop it.
  void setDispatchMode(DispatchMode dispatch_mode) {
    if (dispatchMode == DispatchMode::kBlocking &&
        dispatch_mode == DispatchMode::kNonBlocking) {
      zeroOutRecvStat();
      dispatchMode = DispatchMode::kNonBlocking;
      runRecvThread = true;
      recvThread = std::move(std::thread(&MemcachedClient::recvCallback, this));
      std::cout << "Dispatch mode changed: bloking -> non-blocking\n";
      return;
    }

    if (dispatchMode == DispatchMode::kNonBlocking &&
        dispatch_mode == DispatchMode::kBlocking) {
      if (runRecvThread) {
        runRecvThread = false;
        recvThread.join();
      }
      dispatchMode = DispatchMode::kBlocking;
      std::cout << "Dispatch mode changed: non-bloking -> blocking\n";
      return;
    }
  }

  int set(uint16_t request_id, uint16_t sequence_n, const uint8_t *key,
          uint16_t key_len, const uint8_t *val, uint32_t val_len) {
    uint32_t len = 0;
    int res =
        form_set(request_id, sequence_n, key, key_len, val, val_len, &len);
    if (res != 0) return res;

    res = send(len);
    if (res != 0) return res;

    if (dispatchMode == DispatchMode::kBlocking) {
      uint16_t req_id_rcv;
      uint16_t seq_n_recv;
      int n_pcks = recv();
      if (n_pcks != 1)
        return -1;

      int ret = parse_set_response(&req_id_rcv, &seq_n_recv);
      if (ret != kOK)  // || req_id_rcv != request_id)
        return -1;
      else
        return 0;
    }

    return 0;
  }

  int get(uint16_t request_id, uint16_t sequence_n, const uint8_t *key,
          uint16_t key_len, uint8_t *val_out, uint32_t *val_out_length) {
    uint32_t len = 0;
    int res = form_get(request_id, sequence_n, key, key_len, &len);
    if (res != 0) return res;

    res = send(len);
    if (res != 0) return res;

    if (dispatchMode == DispatchMode::kBlocking) {
      assert(val_out != nullptr);
      assert(val_out_length != nullptr);

      uint16_t req_id_rcv;
      uint16_t seq_n_recv;
      int n_pcks = recv();
      if (n_pcks != 1)
        return -1;

      int ret =
          parse_get_response(&req_id_rcv, &seq_n_recv, val_out, val_out_length);
      if (ret != kOK)  // || req_id_rcv != request_id)
        return -1;
      else
        return 0;
    }

    return 0;
  }

  size_t dumpRecvStat() const {
    if (dispatchMode == DispatchMode::kNonBlocking)
      return nonBlockRecvStat.totalRecved;
    else {
      std::cout << "WARNING: attempting to read recv statistics for a blocking "
                   "connection, this does not really make sense.\n";
    }

    return 0;
  }

  void zeroOutRecvStat() {
    nonBlockRecvStat.totalRecved = 0;
    nonBlockRecvStat.setRecved = 0;
    nonBlockRecvStat.getRecved = 0;
  }

 private:
#ifdef _USE_DPDK_CLIENT_
  // We only need the MAC address for the DPDK stack.
  std::string serverMacAddrStr;
  rte_ether_addr serverMacAddr;

  // Main object storing DPDK-related low-level information.
  DPDKObj dpdkObj;

  // Used to implement zero-copy packet recv. path.
  uint8_t *rx_packets[kMaxBurst];
#else
  // Full Linux UDP/IP stack here.
  std::string serverHostname;
  sockaddr_in serverAddress;
  uint16_t port;

  // Just a UNIX socket.
  int sock;

  // Used to implement Kernel packet recv. path.
  uint8_t *rx_buff;
#endif

  // TX Buffer - same for both stacks.
  uint8_t *tx_buff;

  // Blocking/Non-Blocking.
  DispatchMode dispatchMode;

  // Recv thread and statistics for nonBlocking calls.
  volatile bool runRecvThread;
  std::thread recvThread;
  struct NonBlockStat {
    size_t totalRecved;
    size_t setRecved;
    size_t getRecved;
  };
  NonBlockStat nonBlockRecvStat;

  int form_set(uint16_t request_id, uint16_t sequence_n, const uint8_t *key,
               uint16_t key_len, const uint8_t *val, uint32_t val_len,
               uint32_t *pckt_length) {
    uint8_t *tx_buff_ptr = tx_buff;

    // Form memcached UDP header.
    size_t h_size = HelperFormUdpHeader(reinterpret_cast<MemcacheUdpHeader*>(tx_buff_ptr), request_id, sequence_n);
    tx_buff_ptr += sizeof(MemcacheUdpHeader);

    // Form request header.
    size_t rh_size = HelperFormSetReqHeader(reinterpret_cast<ReqHdr*>(tx_buff_ptr), key_len, val_len);
    tx_buff_ptr += sizeof(ReqHdr);

    // Fill packet: extra, unlimited storage time.
    uint32_t extra[2] = {0x00, 0x00};
    std::memcpy(tx_buff_ptr, extra, kExtraSizeForSet);
    tx_buff_ptr += kExtraSizeForSet;

    // Fill packet: key.
    std::memcpy(tx_buff_ptr, key, key_len);
    tx_buff_ptr += key_len;

    // Fill packet: value.
    std::memcpy(tx_buff_ptr, val, val_len);

    // Check total packet size.
    uint32_t total_length = h_size + rh_size;
    if (total_length > kMaxPacketSize) {
      std::cerr << "Packet size of " << total_length << " is too large\n";
      return -1;
    }

    *pckt_length = total_length;
    return 0;
  }

  int form_get(uint16_t request_id, uint16_t sequence_n, const uint8_t *key,
               uint16_t key_len, uint32_t *pckt_length) {
    uint8_t *tx_buff_ptr = tx_buff;

    // Form memcached UDP header.
    size_t h_size = HelperFormUdpHeader(reinterpret_cast<MemcacheUdpHeader*>(tx_buff_ptr), request_id, sequence_n);
    tx_buff_ptr += sizeof(MemcacheUdpHeader);

    // Form request header.
    size_t rh_size = HelperFormGetReqHeader(reinterpret_cast<ReqHdr*>(tx_buff_ptr), key_len);
    tx_buff_ptr += sizeof(ReqHdr);

    // Fill packet: key.
    std::memcpy(tx_buff_ptr, key, key_len);

    // Check total packet size.
    uint32_t total_length = h_size + rh_size;
    if (total_length > kMaxPacketSize) {
      std::cerr << "Packet size of " << total_length << " is too large\n";
      return -1;
    }
  
    *pckt_length = total_length;
    return 0;
  }

  Status parse_set_response(uint16_t *request_id, uint16_t *sequence_n) const {
#ifdef _USE_DPDK_CLIENT_
    const uint8_t *rx_buff_ptr = reinterpret_cast<uint8_t*>((rx_packets[0]) + sizeof(struct rte_ether_hdr));
#else
    const uint8_t *rx_buff_ptr = rx_buff;
#endif

    size_t h_size = HelperParseUdpHeader(reinterpret_cast<const MemcacheUdpHeader *>(rx_buff_ptr), request_id, sequence_n);
    rx_buff_ptr += h_size;

    const RespHdr *rsp_hdr = reinterpret_cast<const RespHdr *>(rx_buff_ptr);
    if (rsp_hdr->magic != 0x81) {
      std::cerr << "Wrong response received: " << static_cast<int>(rsp_hdr->magic) << "\n";
      return kOtherError;
    }

    Status status =
        static_cast<Status>(rsp_hdr->status[1] | (rsp_hdr->status[0] << 8));

#ifdef _USE_DPDK_CLIENT_
    // In DPDK stack, we need to free packets here.
    FreeDPDKPacket((struct rte_mbuf*)(&rx_packets[0]));
#endif

    return status;
  }

  Status parse_get_response(uint16_t *request_id, uint16_t *sequence_n,
                            uint8_t *val, uint32_t *val_len) const {
#ifdef _USE_DPDK_CLIENT_
    uint8_t *rx_buff_ptr = reinterpret_cast<uint8_t*>((rx_packets[0]) + sizeof(struct rte_ether_hdr));
#else
    uint8_t *rx_buff_ptr = rx_buff;
#endif

    size_t h_size = HelperParseUdpHeader(reinterpret_cast<const MemcacheUdpHeader *>(rx_buff_ptr), request_id, sequence_n);
    rx_buff_ptr += h_size;

    const RespHdr *rsp_hdr = reinterpret_cast<RespHdr *>(rx_buff_ptr);
    if (rsp_hdr->magic != 0x81) {
      std::cerr << "Wrong response received!\n";
      return kOtherError;
    }

    Status status =
        static_cast<Status>(rsp_hdr->status[1] | (rsp_hdr->status[0] << 8));
    if (status == kOK) {
      size_t rh_size = HelperParseRspHeader(rsp_hdr, val_len);
      rx_buff_ptr += rh_size;
      std::memcpy(val, rx_buff_ptr, *val_len);
    }

#ifdef _USE_DPDK_CLIENT_
    // In DKD stack, we need to free packets here.
    FreeDPDKPacket((struct rte_mbuf*)(&rx_packets[0]));
#endif

    return status;
  }

  int send(size_t length) {
#ifdef _USE_DPDK_CLIENT_
    int ret = SendOverDPDK(&dpdkObj, &serverMacAddr, tx_buff, length);
    if (ret) {
      std::cerr << "Failed to send data to the server." << std::endl;
      return -1;
    }
#else
    ssize_t bytesSent =
        sendto(sock, tx_buff, length, MSG_CONFIRM,
               (const struct sockaddr *)&serverAddress, sizeof(serverAddress));
    if (bytesSent != length) {
      std::cerr << "Failed to send data to the server." << std::endl;
      close(sock);
      return -1;
    }
#endif

    return 0;
  }

  // This is a blocking call.
  int recv() {
#ifdef _USE_DPDK_CLIENT_
    int pckt_n = 0;
    while (pckt_n == 0) {
      pckt_n = RecvOverDPDK(&dpdkObj, rx_packets);
    }
    return pckt_n;
#else
    sockaddr_in serverAddress_rvc;
    socklen_t len;
    recvfrom(sock, rx_buff, kMaxPacketSize, 0,
             (struct sockaddr *)&serverAddress_rvc, &len);
    return 1;
#endif
  }

  // Runs in nonBlocking mode to receive responses.
  void recvCallback() {
    while (runRecvThread) {
      int pckt_cnt = recv();
      nonBlockRecvStat.totalRecved += static_cast<size_t>(pckt_cnt);
    }
  }
};

#endif
