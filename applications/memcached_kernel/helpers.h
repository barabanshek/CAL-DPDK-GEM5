#ifndef _HELPERS_H_
#define _HELPERS_H_

// Memcached has it's own extra header for the UDP protocol; it's not really
// documented, but can be found here: memcached.c:build_udp_header().
struct MemcacheUdpHeader {
  uint8_t request_id[2];
  uint8_t udp_sequence[2];
  uint8_t udp_total[2];
  uint8_t RESERVED[2];
} __attribute__((packed));
static_assert(sizeof(MemcacheUdpHeader) == 8);

// We use all bytes here to avoid issues with endianness (to be able to run on both x86 and ARM platforms).
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

static size_t HelperFormUdpHeader(struct MemcacheUdpHeader* hdr, uint16_t request_id, uint16_t sequence_n) {
    hdr->request_id[0] = static_cast<uint8_t>((request_id >> 8) & 0xff);
    hdr->request_id[1] = static_cast<uint8_t>(request_id & 0xff);
    hdr->udp_sequence[0] = static_cast<uint8_t>((sequence_n >> 8) & 0xff);
    hdr->udp_sequence[1] = static_cast<uint8_t>(sequence_n & 0xff);
    hdr->udp_total[0] = 0;
    hdr->udp_total[1] = 1;
    hdr->RESERVED[0] = 0xaa;
    hdr->RESERVED[1] = 0x33;

    return sizeof(struct MemcacheUdpHeader);
}

static const uint8_t kExtraSizeForSet = 8;
static size_t HelperFormSetReqHeader(struct ReqHdr* hdr, uint16_t key_len, uint32_t val_len) {
    uint32_t total_payld_length = kExtraSizeForSet + key_len + val_len;

    memset(hdr, 0x00, sizeof(struct ReqHdr));
    hdr->magic = 0x80;  // req
    hdr->opcode = 0x01; // set
    hdr->key_length[0] = static_cast<uint8_t>((key_len >> 8) & 0xff);
    hdr->key_length[1] = static_cast<uint8_t>(key_len & 0xff);
    hdr->extra_length = kExtraSizeForSet;
    hdr->total_body_length[0] = static_cast<uint8_t>((total_payld_length >> 24) & 0xff);
    hdr->total_body_length[1] = static_cast<uint8_t>((total_payld_length >> 16) & 0xff);
    hdr->total_body_length[2] = static_cast<uint8_t>((total_payld_length >> 8) & 0xff);
    hdr->total_body_length[3] = static_cast<uint8_t>(total_payld_length & 0xff);

    return sizeof(struct ReqHdr) + total_payld_length;
}

static size_t HelperFormGetReqHeader(struct ReqHdr* hdr, uint16_t key_len) {
    uint32_t total_payld_length = key_len;

    memset(hdr, 0x00, sizeof(struct ReqHdr));
    hdr->magic = 0x80;  // req
    hdr->opcode = 0x00; // get
    hdr->key_length[0] = static_cast<uint8_t>((key_len >> 8) & 0xff);
    hdr->key_length[1] = static_cast<uint8_t>(key_len & 0xff);
    hdr->total_body_length[0] = static_cast<uint8_t>((total_payld_length >> 24) & 0xff);
    hdr->total_body_length[1] = static_cast<uint8_t>((total_payld_length >> 16) & 0xff);
    hdr->total_body_length[2] = static_cast<uint8_t>((total_payld_length >> 8) & 0xff);
    hdr->total_body_length[3] = static_cast<uint8_t>(total_payld_length & 0xff);

    return sizeof(struct ReqHdr) + total_payld_length;
}

static size_t HelperParseUdpHeader(const struct MemcacheUdpHeader *udp_hdr, uint16_t *request_id, uint16_t *sequence_n) {
    *request_id = (udp_hdr->request_id[1] << 8) | (udp_hdr->request_id[0]);
    *sequence_n = (udp_hdr->udp_sequence[1] << 8) | (udp_hdr->udp_sequence[0]);
    return sizeof(struct MemcacheUdpHeader);
}

static size_t HelperParseRspHeader(const struct RespHdr* hdr, uint32_t* val_len) {
    uint8_t extra_len = hdr->extra_length;
    uint16_t key_len = hdr->key_length[1] | (hdr->key_length[0] << 8);
    uint32_t total_body_len = (uint32_t)(hdr->total_body_length[3]) |
                              (uint32_t)(hdr->total_body_length[2] << 8) |
                              (uint32_t)(hdr->total_body_length[1] << 16) |
                              (uint32_t)(hdr->total_body_length[0] << 24);
    *val_len = total_body_len - extra_len - key_len;
    return sizeof(RespHdr) + extra_len + key_len;
}

#endif
