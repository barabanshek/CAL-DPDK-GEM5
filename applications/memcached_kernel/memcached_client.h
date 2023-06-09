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
        dispatchMode(DispatchMode::kBlocking),
        tx_buff(nullptr),
        rx_buff(nullptr),
        runRecvThread(false) {}
#else
  // Constructor for Kernel networking.
  MemcachedClient(const std::string &server_hostname, uint16_t port)
      : serverHostname(server_hostname),
        port(port),
        sock(-1),
        dispatchMode(DispatchMode::kBlocking),
        tx_buff(nullptr),
        rx_buff(nullptr),
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
#endif

    if (tx_buff != nullptr) std::free(tx_buff);
    if (rx_buff != nullptr) std::free(rx_buff);
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
    struct timeval tv = {.tv_sec = kSockTimeout, .tv_usec = 0};
    if (setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
      std::cerr << "Faile to set socket timeout." << std::endl;
      return -1;
    }
#endif

    // Init buffers.
    tx_buff =
        static_cast<uint8_t *>(std::aligned_alloc(kClSize, kMaxPacketSize));
    assert(tx_buff != nullptr);

    rx_buff =
        static_cast<uint8_t *>(std::aligned_alloc(kClSize, kMaxPacketSize));
    assert(rx_buff != nullptr);

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
      recv();
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
      recv();
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
  // Net things.
#ifdef _USE_DPDK_CLIENT_
  std::string serverMacAddrStr;
  rte_ether_addr serverMacAddr;
  DPDKObj dpdkObj;
#else
  std::string serverHostname;
  uint16_t port;
  sockaddr_in serverAddress;
  int sock;
#endif

  DispatchMode dispatchMode;

  // Buffers.
  uint8_t *tx_buff;
  uint8_t *rx_buff;

  // Recv thread and statistics for nonBlocking calls.
  volatile bool runRecvThread;
  std::thread recvThread;
  struct NonBlockStat {
    size_t totalRecved;
    size_t setRecved;
    size_t getRecved;
  };
  NonBlockStat nonBlockRecvStat;

  // Memcached has it's own extra header for the UDP protocol; it's not really
  // documented, but can be found here: memcached.c:build_udp_header().
  struct MemcacheUdpHeader {
    uint8_t request_id[2];
    uint8_t udp_sequence[2];
    uint8_t udp_total[2];
    uint8_t RESERVED[2];
  } __attribute__((packed));
  static_assert(sizeof(MemcacheUdpHeader) == 8);

  struct ReqHdr {
    uint8_t magic;
    uint8_t opcode;
    uint8_t key_length[2];
    uint8_t extra_length;
    uint8_t data_type;
    uint8_t RESERVED[2];
    uint8_t total_body_length[4];
    uint8_t opaque[4];
    uint8_t CAS[8];
  } __attribute__((packed));
  static_assert(sizeof(ReqHdr) == 24);

  struct RespHdr {
    uint8_t magic;
    uint8_t opcode;
    uint8_t key_length[2];
    uint8_t extra_length;
    uint8_t data_type;
    uint8_t status[2];
    uint8_t total_body_length[4];
    uint8_t opaque[4];
    uint8_t CAS[8];
  } __attribute__((packed));
  static_assert(sizeof(RespHdr) == 24);

  int form_set(uint16_t request_id, uint16_t sequence_n, const uint8_t *key,
               uint16_t key_len, const uint8_t *val, uint32_t val_len,
               uint32_t *pckt_length) {
    // Form memcached UDP header.
    MemcacheUdpHeader hdr = {{static_cast<uint8_t>((request_id >> 8) & 0xff),
                              static_cast<uint8_t>(request_id & 0xff)},
                             {static_cast<uint8_t>((sequence_n >> 8) & 0xff),
                              static_cast<uint8_t>(sequence_n & 0xff)},
                             {0, 1},
                             {0, 0}};
    std::memcpy(tx_buff, &hdr, sizeof(MemcacheUdpHeader));

    // Form packet.
    constexpr uint8_t kExtraSize = 8;
    uint32_t total_payld_length = kExtraSize + key_len + val_len;
    uint32_t total_length =
        sizeof(MemcacheUdpHeader) + sizeof(ReqHdr) + total_payld_length;
    if (total_length > kMaxPacketSize) {
      std::cerr << "Packet size of " << total_length << " is too large\n";
      return -1;
    }
    ReqHdr req_hdr = {0x80,
                      0x01,  // set
                      {static_cast<uint8_t>((key_len >> 8) & 0xff),
                       static_cast<uint8_t>(key_len & 0xff)},
                      kExtraSize,
                      0x00,
                      {0x00, 0x00},
                      {static_cast<uint8_t>((total_payld_length >> 24) & 0xff),
                       static_cast<uint8_t>((total_payld_length >> 16) & 0xff),
                       static_cast<uint8_t>((total_payld_length >> 8) & 0xff),
                       static_cast<uint8_t>(total_payld_length & 0xff)},
                      {0x00, 0x00, 0x00, 0x00},
                      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}};
    // Fill packet: unlimited storage time.
    uint32_t extra[2] = {0x00, 0x00};
    std::memcpy(tx_buff + sizeof(MemcacheUdpHeader), &req_hdr, sizeof(ReqHdr));
    std::memcpy(tx_buff + sizeof(MemcacheUdpHeader) + sizeof(ReqHdr), extra,
                kExtraSize);
    std::memcpy(
        tx_buff + sizeof(MemcacheUdpHeader) + sizeof(ReqHdr) + kExtraSize, key,
        key_len);
    std::memcpy(tx_buff + sizeof(MemcacheUdpHeader) + sizeof(ReqHdr) +
                    kExtraSize + key_len,
                val, val_len);

    *pckt_length = total_length;
    return 0;
  }

  int form_get(uint16_t request_id, uint16_t sequence_n, const uint8_t *key,
               uint16_t key_len, uint32_t *pckt_length) {
    // Form memcached UDP header.
    MemcacheUdpHeader hdr = {{static_cast<uint8_t>((request_id >> 8) & 0xff),
                              static_cast<uint8_t>(request_id & 0xff)},
                             {static_cast<uint8_t>((sequence_n >> 8) & 0xff),
                              static_cast<uint8_t>(sequence_n & 0xff)},
                             {0, 1},
                             {0, 0}};
    std::memcpy(tx_buff, &hdr, sizeof(MemcacheUdpHeader));

    // Form packet.
    uint32_t total_payld_length = key_len;
    uint32_t total_length =
        sizeof(MemcacheUdpHeader) + sizeof(ReqHdr) + total_payld_length;
    if (total_length > kMaxPacketSize) {
      std::cerr << "Packet size of " << total_length << " is too large\n";
      return -1;
    }
    ReqHdr req_hdr = {0x80,
                      0x00,  // get
                      {static_cast<uint8_t>((key_len >> 8) & 0xff),
                       static_cast<uint8_t>(key_len & 0xff)},
                      0x00,
                      0x00,
                      {0x00, 0x00},
                      {static_cast<uint8_t>((total_payld_length >> 24) & 0xff),
                       static_cast<uint8_t>((total_payld_length >> 16) & 0xff),
                       static_cast<uint8_t>((total_payld_length >> 8) & 0xff),
                       static_cast<uint8_t>(total_payld_length & 0xff)},
                      {0x00, 0x00, 0x00, 0x00},
                      {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}};
    // Fill packet.
    std::memcpy(tx_buff + sizeof(MemcacheUdpHeader), &req_hdr, sizeof(ReqHdr));
    std::memcpy(tx_buff + sizeof(MemcacheUdpHeader) + sizeof(ReqHdr), key,
                key_len);

    *pckt_length = total_length;
    return 0;
  }

  Status parse_set_response(uint16_t *request_id, uint16_t *sequence_n) const {
    const MemcacheUdpHeader *udp_hdr =
        reinterpret_cast<MemcacheUdpHeader *>(rx_buff);
    *request_id = (udp_hdr->request_id[1] << 8) | (udp_hdr->request_id[0]);
    *sequence_n = (udp_hdr->udp_sequence[1] << 8) | (udp_hdr->udp_sequence[0]);

    const RespHdr *rsp_hdr =
        reinterpret_cast<RespHdr *>(rx_buff + sizeof(MemcacheUdpHeader));
    if (rsp_hdr->magic != 0x81) {
      std::cerr << "Wrong response received: " << (int)(rsp_hdr->magic) << "\n";
      return kOtherError;
    }

    Status status =
        static_cast<Status>(rsp_hdr->status[1] | (rsp_hdr->status[0] << 8));
    return status;
  }

  Status parse_get_response(uint16_t *request_id, uint16_t *sequence_n,
                            uint8_t *val, uint32_t *val_len) const {
    const MemcacheUdpHeader *udp_hdr =
        reinterpret_cast<MemcacheUdpHeader *>(rx_buff);
    *request_id = (udp_hdr->request_id[1] << 8) | (udp_hdr->request_id[0]);
    *sequence_n = (udp_hdr->udp_sequence[1] << 8) | (udp_hdr->udp_sequence[0]);

    const RespHdr *rsp_hdr =
        reinterpret_cast<RespHdr *>(rx_buff + sizeof(MemcacheUdpHeader));
    if (rsp_hdr->magic != 0x81) {
      std::cerr << "Wrong response received!\n";
      return kOtherError;
    }

    Status status =
        static_cast<Status>(rsp_hdr->status[1] | (rsp_hdr->status[0] << 8));
    if (status == kOK) {
      uint8_t extra_len_ = rsp_hdr->extra_length;
      uint16_t key_len_ =
          rsp_hdr->key_length[1] | (rsp_hdr->key_length[0] << 8);
      uint32_t total_body_len = rsp_hdr->total_body_length[3] |
                                (rsp_hdr->total_body_length[2] << 8) |
                                (rsp_hdr->total_body_length[1] << 16) |
                                (rsp_hdr->total_body_length[0] << 24);
      uint32_t val_len_ = total_body_len - extra_len_ - key_len_;
      std::memcpy(val,
                  rx_buff + sizeof(MemcacheUdpHeader) + sizeof(RespHdr) +
                      extra_len_ + key_len_,
                  val_len_);

      *val_len = val_len_;
    }

    return status;
  }

  int send(size_t length) {
#ifdef _USE_DPDK_CLIENT_
    assert (sizeof(rte_ether_hdr) + length <= kMTUStandardFrames);

    // Create packet.
    rte_mbuf *created_pkt = rte_pktmbuf_alloc(dpdkObj.mpool);
    if (created_pkt == nullptr) {
      std::cerr << "Failed to get packet mbuf.\n";
      return -1;
    }
    size_t pkt_size = sizeof(rte_ether_hdr) + length;
    created_pkt->data_len = pkt_size;
    created_pkt->pkt_len = pkt_size;

    // Append Ethernet header.
    rte_ether_hdr *eth_hdr = rte_pktmbuf_mtod(created_pkt, rte_ether_hdr *);
    rte_ether_addr_copy(&dpdkObj.pmd_eth_addrs[dpdkObj.pmd_port_to_use], &eth_hdr->s_addr);
    rte_ether_addr_copy(&serverMacAddr, &eth_hdr->d_addr);
    eth_hdr->ether_type = rte_cpu_to_be_16(RTE_ETHER_TYPE_IPV4);

    // Append data.
    uint8_t* pckt_data = (uint8_t *)eth_hdr + sizeof(rte_ether_hdr);
    std::memcpy(pckt_data, tx_buff, length);

    // Send packet.
    uint16_t pckt_sent = rte_eth_tx_burst(dpdkObj.pmd_ports[dpdkObj.pmd_port_to_use], 0, &created_pkt, 1);
    if (pckt_sent != 1) {
      std::cerr << "Failed to send packet.\n";
      rte_pktmbuf_free(created_pkt);
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

  void recv() {
#ifdef _USE_DPDK_CLIENT_
#else
    sockaddr_in serverAddress_rvc;
    socklen_t len;
    recvfrom(sock, rx_buff, kMaxPacketSize, 0,
             (struct sockaddr *)&serverAddress_rvc, &len);
#endif
  }

  // Runs in nonBlocking mode to receive responses.
  void recvCallback() {
    while (runRecvThread) {
      recv();
      ++nonBlockRecvStat.totalRecved;
    }
  }
};

#endif
