#include "EsFilePayloadProtocol.h"

const uint32_t kEsFilePacketMagic = 0x47544659;
const uint8_t kEsFilePacketVersion = 1;
const size_t kEsFileFixedHeaderSize = 48;
const uint16_t kEsFileFlagFileInfoHasHttpResponseHeaders = 0x0001;
const uint8_t kEsFileCarrierNalHeader = 0x61;
const size_t kEsFileCarrierPrefixSize = 5;

void WriteEsFileU16BE(std::vector<uint8_t> &out, uint16_t value) {
    out.push_back(static_cast<uint8_t>((value >> 8) & 0xFF));
    out.push_back(static_cast<uint8_t>(value & 0xFF));
}

void WriteEsFileU32BE(std::vector<uint8_t> &out, uint32_t value) {
    out.push_back(static_cast<uint8_t>((value >> 24) & 0xFF));
    out.push_back(static_cast<uint8_t>((value >> 16) & 0xFF));
    out.push_back(static_cast<uint8_t>((value >> 8) & 0xFF));
    out.push_back(static_cast<uint8_t>(value & 0xFF));
}

void WriteEsFileU64BE(std::vector<uint8_t> &out, uint64_t value) {
    out.push_back(static_cast<uint8_t>((value >> 56) & 0xFF));
    out.push_back(static_cast<uint8_t>((value >> 48) & 0xFF));
    out.push_back(static_cast<uint8_t>((value >> 40) & 0xFF));
    out.push_back(static_cast<uint8_t>((value >> 32) & 0xFF));
    out.push_back(static_cast<uint8_t>((value >> 24) & 0xFF));
    out.push_back(static_cast<uint8_t>((value >> 16) & 0xFF));
    out.push_back(static_cast<uint8_t>((value >> 8) & 0xFF));
    out.push_back(static_cast<uint8_t>(value & 0xFF));
}

uint16_t ReadEsFileU16BE(const uint8_t *p) {
    return static_cast<uint16_t>((static_cast<uint16_t>(p[0]) << 8) |
                                 static_cast<uint16_t>(p[1]));
}

uint32_t ReadEsFileU32BE(const uint8_t *p) {
    return (static_cast<uint32_t>(p[0]) << 24) |
           (static_cast<uint32_t>(p[1]) << 16) |
           (static_cast<uint32_t>(p[2]) << 8) |
           static_cast<uint32_t>(p[3]);
}

uint64_t ReadEsFileU64BE(const uint8_t *p) {
    return (static_cast<uint64_t>(p[0]) << 56) |
           (static_cast<uint64_t>(p[1]) << 48) |
           (static_cast<uint64_t>(p[2]) << 40) |
           (static_cast<uint64_t>(p[3]) << 32) |
           (static_cast<uint64_t>(p[4]) << 24) |
           (static_cast<uint64_t>(p[5]) << 16) |
           (static_cast<uint64_t>(p[6]) << 8) |
           static_cast<uint64_t>(p[7]);
}

void AppendEsFileCarrierPrefix(std::vector<uint8_t> &out) {
    out.push_back(0x00);
    out.push_back(0x00);
    out.push_back(0x00);
    out.push_back(0x01);
    out.push_back(kEsFileCarrierNalHeader);
}

bool HasEsFileCarrierPrefix(const uint8_t *data, size_t size) {
    return data && size >= kEsFileCarrierPrefixSize &&
           data[0] == 0x00 && data[1] == 0x00 &&
           data[2] == 0x00 && data[3] == 0x01 &&
           data[4] == kEsFileCarrierNalHeader;
}

void AppendEsFilePacketHeader(std::vector<uint8_t> &out,
                              const EsFilePacketHeader &header) {
    WriteEsFileU32BE(out, header.magic);
    out.push_back(header.version);
    out.push_back(static_cast<uint8_t>(header.type));
    WriteEsFileU16BE(out, header.task_id_len);
    WriteEsFileU16BE(out, header.file_name_len);
    WriteEsFileU16BE(out, header.flags);
    WriteEsFileU32BE(out, header.seq);
    WriteEsFileU64BE(out, header.data_offset);
    WriteEsFileU32BE(out, header.payload_len);
    WriteEsFileU64BE(out, header.file_size);
    WriteEsFileU32BE(out, header.crc32);
    WriteEsFileU32BE(out, header.total_len);
    WriteEsFileU32BE(out, header.reserved);
}

bool DecodeEsFilePacketHeader(const uint8_t *data, size_t size,
                              EsFilePacketHeader &header) {
    if (!data || size < kEsFileFixedHeaderSize) {
        return false;
    }
    header.magic = ReadEsFileU32BE(data + 0);
    header.version = data[4];
    header.type = static_cast<EsFilePacketType>(data[5]);
    header.task_id_len = ReadEsFileU16BE(data + 6);
    header.file_name_len = ReadEsFileU16BE(data + 8);
    header.flags = ReadEsFileU16BE(data + 10);
    header.seq = ReadEsFileU32BE(data + 12);
    header.data_offset = ReadEsFileU64BE(data + 16);
    header.payload_len = ReadEsFileU32BE(data + 24);
    header.file_size = ReadEsFileU64BE(data + 28);
    header.crc32 = ReadEsFileU32BE(data + 36);
    header.total_len = ReadEsFileU32BE(data + 40);
    header.reserved = ReadEsFileU32BE(data + 44);
    return true;
}

std::string EsFilePacketTypeToString(EsFilePacketType type){
    switch (type) {
        case EsFilePacketType::Unknown:
            return "Unknown";
        case EsFilePacketType::FileInfo:
            return "FileInfo";
        case EsFilePacketType::FileChunk:
            return "FileChunk";
        case EsFilePacketType::FileEnd:
            return "FileEnd";
        case EsFilePacketType::TaskStatus:
            return "TaskStatus";
        default:
            return "Unknown";
    }
}
