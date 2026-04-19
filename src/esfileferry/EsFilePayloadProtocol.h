#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

// ES 文件负载包类型
enum class EsFilePacketType : uint8_t {
    // 未知类型
    Unknown = 0,
    // 文件元信息包
    FileInfo = 1,
    // 文件分片数据包
    FileChunk = 2,
    // 文件结束包
    FileEnd = 3,
    // 任务状态包
    TaskStatus = 4
};

// 固定头逻辑模型（线协议固定为 48 字节，整数字段均为大端）。
// 注意：该结构体只作为字段容器，禁止直接 memcpy/按 sizeof(EsFilePacketHeader)
// 作为网络包头长度，因为不同编译器/平台下可能存在对齐差异。
struct EsFilePacketHeader {
    // 魔数，固定 0x47544659
    uint32_t magic = 0;
    // 协议版本
    uint8_t version = 0;
    // 包类型
    EsFilePacketType type = EsFilePacketType::Unknown;
    // task_id 字节长度
    uint16_t task_id_len = 0;
    // file_name 字节长度
    uint16_t file_name_len = 0;
    // 扩展标记位
    uint16_t flags = 0;
    // 包序号
    uint32_t seq = 0;
    // 当前数据块对应文件偏移
    uint64_t data_offset = 0;
    // payload 字节长度
    uint32_t payload_len = 0;
    // 文件总大小
    uint64_t file_size = 0;
    // payload 的 CRC32，可选
    uint32_t crc32 = 0xFFFFFFFF;
    // 包总长度：48 + task_id_len + file_name_len + payload_len
    uint32_t total_len = 0;
    // 预留字段
    uint32_t reserved = 0;
};

// 完整协议包：固定头 + 变长 task_id/file_name/payload
struct EsFilePacket {
    EsFilePacketHeader header;
    // 任务标识
    std::string task_id;
    // 文件名
    std::string file_name;
    // 文件负载/状态负载
    std::vector<uint8_t> payload;
};

extern const uint32_t kEsFilePacketMagic;
extern const uint8_t kEsFilePacketVersion;
extern const size_t kEsFileFixedHeaderSize;
extern const uint16_t kEsFileFlagFileInfoHasHttpResponseHeaders;
extern const uint8_t kEsFileCarrierNalHeader;
extern const size_t kEsFileCarrierPrefixSize;

void WriteEsFileU16BE(std::vector<uint8_t> &out, uint16_t value);
void WriteEsFileU32BE(std::vector<uint8_t> &out, uint32_t value);
void WriteEsFileU64BE(std::vector<uint8_t> &out, uint64_t value);
uint16_t ReadEsFileU16BE(const uint8_t *p);
uint32_t ReadEsFileU32BE(const uint8_t *p);
uint64_t ReadEsFileU64BE(const uint8_t *p);
void AppendEsFileCarrierPrefix(std::vector<uint8_t> &out);
bool HasEsFileCarrierPrefix(const uint8_t *data, size_t size);
void AppendEsFilePacketHeader(std::vector<uint8_t> &out,
                              const EsFilePacketHeader &header);
bool DecodeEsFilePacketHeader(const uint8_t *data, size_t size,
                              EsFilePacketHeader &header);

std::string EsFilePacketTypeToString(EsFilePacketType type);
