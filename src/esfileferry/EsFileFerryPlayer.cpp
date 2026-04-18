#include "EsFileFerryPlayer.h"
#include "Util/logger.h"
#include <cstring>
#include <mutex>
#include <unordered_map>

using namespace toolkit;

namespace {
size_t scanPacketStart(const uint8_t *data, size_t size, uint32_t packet_magic,
                       bool &found) {
  size_t start = 0;
  found = false;
  while (start + 9 <= size) {
    if (HasEsFileCarrierPrefix(data + start, size - start) &&
        ReadEsFileU32BE(data + start + kEsFileCarrierPrefixSize) ==
            packet_magic) {
      found = true;
      break;
    }
    ++start;
  }
  return start;
}

enum class PacketDecodeStatus { Success, NeedMoreData, Invalid };

const char *packetDecodeStatusName(PacketDecodeStatus status) {
  switch (status) {
  case PacketDecodeStatus::Success:
    return "success";
  case PacketDecodeStatus::NeedMoreData:
    return "need_more_data";
  case PacketDecodeStatus::Invalid:
    return "invalid";
  }
  return "unknown";
}

bool shouldLogFrameCandidate(const uint8_t *data, size_t size) {
  return data && size > 0 &&
         (size >= 64 * 1024 || HasEsFileCarrierPrefix(data, size));
}

bool tryDecodeCandidateHeader(const uint8_t *data, size_t size,
                              EsFilePacketHeader &header) {
  if (!data || size < kEsFileCarrierPrefixSize + kEsFileFixedHeaderSize ||
      !HasEsFileCarrierPrefix(data, size)) {
    return false;
  }
  return DecodeEsFilePacketHeader(data + kEsFileCarrierPrefixSize,
                                  size - kEsFileCarrierPrefixSize, header);
}

bool shouldLogMissingTask(const std::string &task_id, size_t &count) {
  static std::mutex s_mtx;
  static std::unordered_map<std::string, size_t> s_missing_counts;
  std::lock_guard<std::mutex> lock(s_mtx);
  count = ++s_missing_counts[task_id];
  return count <= 5 || count % 100 == 0;
}

PacketDecodeStatus decodePacketAt(const uint8_t *data, size_t size,
                                  uint32_t packet_magic,
                                  size_t fixed_header_size,
                                  size_t max_packet_size, EsFilePacket &packet,
                                  size_t &consumed) {
  consumed = 0;
  packet = EsFilePacket{};
  if (!data || size < fixed_header_size + kEsFileCarrierPrefixSize) {
    return PacketDecodeStatus::NeedMoreData;
  }
  if (!HasEsFileCarrierPrefix(data, size) ||
      ReadEsFileU32BE(data + kEsFileCarrierPrefixSize) != packet_magic) {
    return PacketDecodeStatus::Invalid;
  }
  const uint8_t *p = data + kEsFileCarrierPrefixSize;
  if (!DecodeEsFilePacketHeader(p, size - kEsFileCarrierPrefixSize,
                                packet.header)) {
    return PacketDecodeStatus::NeedMoreData;
  }

  if (packet.header.total_len < fixed_header_size ||
      packet.header.total_len > max_packet_size) {
    return PacketDecodeStatus::Invalid;
  }
  const auto min_total = static_cast<uint32_t>(
      fixed_header_size + packet.header.task_id_len +
      packet.header.file_name_len + packet.header.payload_len);
  if (packet.header.total_len < min_total) {
    return PacketDecodeStatus::Invalid;
  }
  if (size <
      static_cast<size_t>(packet.header.total_len + kEsFileCarrierPrefixSize)) {
    return PacketDecodeStatus::NeedMoreData;
  }

  size_t pos = fixed_header_size;
  if (packet.header.task_id_len > 0) {
    packet.task_id.assign(reinterpret_cast<const char *>(p + pos),
                          packet.header.task_id_len);
    pos += packet.header.task_id_len;
  }
  if (packet.header.file_name_len > 0) {
    packet.file_name.assign(reinterpret_cast<const char *>(p + pos),
                            packet.header.file_name_len);
    pos += packet.header.file_name_len;
  }
  if (packet.header.payload_len > 0) {
    packet.payload.resize(packet.header.payload_len);
    std::memcpy(packet.payload.data(), p + pos, packet.header.payload_len);
  }
  consumed =
      static_cast<size_t>(packet.header.total_len + kEsFileCarrierPrefixSize);
  return PacketDecodeStatus::Success;
}
} // namespace

EsFileFerryUnPacker &EsFileFerryUnPacker::Instance() {
  static std::shared_ptr<EsFileFerryUnPacker> instance(
      new EsFileFerryUnPacker());
  static EsFileFerryUnPacker &ref = *instance;
  return ref;
}

void EsFileFerryUnPacker::setTaskCallback(const std::string &task_id,
                                          OnTaskData cb) {
  std::lock_guard<std::mutex> lock(_mtx);
  if (task_id.empty()) {
    return;
  }
  DebugL << "set task callback, task_id:" << task_id << " cb:" << (void *)&cb << " this:" << this;
  if (cb) {
    _task_callbacks[task_id] = std::move(cb);
    if (_task_states.find(task_id) == _task_states.end()) {
      _task_states.emplace(task_id, TaskRuntimeState{});
    }
  } else {
    _task_callbacks.erase(task_id);
    _task_states.erase(task_id);
  }
}

void EsFileFerryUnPacker::removeTask(const std::string &task_id) {
  std::lock_guard<std::mutex> lock(_mtx);
  _task_callbacks.erase(task_id);
  _task_states.erase(task_id);
}

void EsFileFerryUnPacker::clearTasks() {
  std::lock_guard<std::mutex> lock(_mtx);
  _task_callbacks.clear();
  _task_states.clear();
}

std::vector<std::string> EsFileFerryUnPacker::getTaskIds() const {
  std::lock_guard<std::mutex> lock(_mtx);
  std::vector<std::string> ids;
  ids.reserve(_task_callbacks.size());
  for (auto &it : _task_callbacks) {
    ids.emplace_back(it.first);
  }
  return ids;
}

std::string EsFileFerryUnPacker::getLastError() const {
  std::lock_guard<std::mutex> lock(_mtx);
  return _last_error;
}

void EsFileFerryUnPacker::setOnError(OnError cb) {
  std::lock_guard<std::mutex> lock(_mtx);
  _on_error = std::move(cb);
}


static const char *memfind_ferry(const char *buf, ssize_t len, const char *subbuf, ssize_t sublen) {
    for (auto i = 0; i < len - sublen; ++i) {
        if (memcmp(buf + i, subbuf, sublen) == 0) {
            return buf + i;
        }
    }
    return NULL;
}

#define H264_TYPE(v) ((uint8_t)(v) & 0x1F)

void splitFerryH264(const char *ptr, size_t len, size_t prefix, const std::function<void(const char *, size_t, size_t)> &cb) {
    auto start = ptr + prefix;
    auto last_start = ptr + prefix;
    auto end = ptr + len;
    size_t next_prefix;
    while (true) {
        auto next_start = memfind_ferry(start, end - start, "\x00\x00\x01", 3);
        if (next_start) {
            // 找到下一帧  [AUTO-TRANSLATED:7161f54a]
            // Find the next frame
            if (*(next_start - 1) == 0x00) {
                // 这个是00 00 00 01开头  [AUTO-TRANSLATED:b0d79e9e]
                // This starts with 00 00 00 01
                next_start -= 1;
                next_prefix = 4;
            } else {
                // 这个是00 00 01开头  [AUTO-TRANSLATED:18ae81d8]
                // This starts with 00 00 01
                next_prefix = 3;
            }
            // 记得加上本帧prefix长度  [AUTO-TRANSLATED:8bde5d52]
            // Remember to add the prefix length of this frame
            auto nal_type = H264_TYPE((uint8_t)*start);
            if (nal_type == 6 || nal_type == 7 || nal_type == 8 || nal_type == 5 || nal_type == 1) {
                cb(start - prefix, next_start - start + prefix, prefix);
                // 搜索下一帧末尾的起始位置  [AUTO-TRANSLATED:8976b719]
                // Search for the starting position of the end of the next frame
                last_start = start = next_start + next_prefix;
                // 记录下一帧的prefix长度  [AUTO-TRANSLATED:756aee4e]
                // Record the prefix length of the next frame
                prefix = next_prefix;
            } else {
                start = next_start + next_prefix;
                // 记录下一帧的prefix长度  [AUTO-TRANSLATED:756aee4e]
                // Record the prefix length of the next frame
                prefix = next_prefix;
            }
            continue;
        }
        // 未找到下一帧,这是最后一帧  [AUTO-TRANSLATED:58365453]
        // The next frame was not found, this is the last frame
        cb(start - prefix, end - start + prefix, prefix);
        break;
    }
}

#ifndef MIN
#define MIN(A, B) ((A) < (B) ? (A) : (B))
#endif
#ifndef MAX
#define MAX(A, B) ((A) > (B) ? (A) : (B))
#endif

bool EsFileFerryUnPacker::inputFrame(const uint8_t *data, size_t size) {
  if (!data || size == 0) {
    return false;
  }

  // Fast path: a complete ferry packet arrives in one frame.
  EsFilePacket packet;
  size_t consumed = 0;
  if (parseOnePacketFromRaw(data, size, packet, consumed) && consumed == size) {
    dispatchPacket(std::move(packet));
    return true;
  }

  {
    std::lock_guard<std::mutex> lock(_mtx);
    _buffer.insert(_buffer.end(), data, data + size);
  }
  parseBuffer();
  return true;
}
void EsFileFerryUnPacker::parseBuffer() {
  while (true) {
    EsFilePacket packet;
    if (!parseOnePacket(packet)) {
      break;
    }
    dispatchPacket(std::move(packet));
  }
}

bool EsFileFerryUnPacker::parseOnePacket(EsFilePacket &packet) {
  std::lock_guard<std::mutex> lock(_mtx);
  const auto available = _buffer.size() - _buffer_start;
  if (available < kEsFileFixedHeaderSize + kEsFileCarrierPrefixSize) {
    return false;
  }
  const auto *buffer_data = _buffer.data() + _buffer_start;

  bool found = false;
  const auto start = scanPacketStart(buffer_data, available, kEsFilePacketMagic, found);

  if (!found) {
    if (start > 0) {
      _buffer_start += start;
      compactBufferLocked();
    }
    return false;
  }

  if (start > 0) {
    _buffer_start += start;
    compactBufferLocked();
  }

  size_t consumed = 0;
  const auto packet_available = _buffer.size() - _buffer_start;
  const auto status = decodePacketAt(_buffer.data() + _buffer_start, packet_available,
                                     kEsFilePacketMagic, kEsFileFixedHeaderSize,
                                     kMaxPacketSize, packet, consumed);
  if (status == PacketDecodeStatus::Success) {
    _buffer_start += consumed;
    compactBufferLocked();
    return true;
  }
  if (shouldLogFrameCandidate(_buffer.data() + _buffer_start, packet_available)) {
    EsFilePacketHeader header;
    const auto has_header = tryDecodeCandidateHeader(_buffer.data() + _buffer_start,
                                                     packet_available, header);
    const auto sample_size = packet_available > 8 ? 8 : packet_available;
    if (status == PacketDecodeStatus::Invalid || packet_available >= 64 * 1024) {
      WarnL << "parse buffered packet, status:" << packetDecodeStatusName(status)
            << " available:" << packet_available
            << " buffer_start:" << _buffer_start
            << " hex:" << toolkit::hexmem(_buffer.data() + _buffer_start, sample_size)
            << " has_header:" << has_header
            << " total_len:" << (has_header ? header.total_len : 0)
            << " payload_len:" << (has_header ? header.payload_len : 0)
            << " task_id_len:" << (has_header ? header.task_id_len : 0)
            << " file_name_len:" << (has_header ? header.file_name_len : 0);
    } else {
      DebugL << "parse buffered packet, status:" << packetDecodeStatusName(status)
             << " available:" << packet_available
             << " buffer_start:" << _buffer_start
             << " hex:" << toolkit::hexmem(_buffer.data() + _buffer_start, sample_size)
             << " has_header:" << has_header
             << " total_len:" << (has_header ? header.total_len : 0)
             << " payload_len:" << (has_header ? header.payload_len : 0)
             << " task_id_len:" << (has_header ? header.task_id_len : 0)
             << " file_name_len:" << (has_header ? header.file_name_len : 0);
    }
  }
  if (status == PacketDecodeStatus::Invalid) {
    ++_buffer_start;
    compactBufferLocked();
  }
  return false;
}

bool EsFileFerryUnPacker::parseOnePacketFromRaw(const uint8_t *data,
                                                size_t size,
                                                EsFilePacket &packet,
                                                size_t &consumed) const {
  consumed = 0;
  if (!data || size < kEsFileFixedHeaderSize + kEsFileCarrierPrefixSize) {
    return false;
  }

  bool found = false;
  const auto start = scanPacketStart(data, size, kEsFilePacketMagic, found);
  if (!found ||
      size < start + kEsFileFixedHeaderSize + kEsFileCarrierPrefixSize) {
    return false;
  }

  size_t local_consumed = 0;
  const auto status =
      decodePacketAt(data + start, size - start, kEsFilePacketMagic,
                     kEsFileFixedHeaderSize, kMaxPacketSize, packet,
                     local_consumed);
  if (status != PacketDecodeStatus::Success) {
    const auto *candidate = data + start;
    const auto candidate_size = size - start;
    if (shouldLogFrameCandidate(candidate, candidate_size)) {
      EsFilePacketHeader header;
      const auto has_header =
          tryDecodeCandidateHeader(candidate, candidate_size, header);
      const auto sample_size = candidate_size > 8 ? 8 : candidate_size;
      DebugL << "parse raw frame, status:" << packetDecodeStatusName(status)
             << " size:" << candidate_size
             << " start:" << start
             << " hex:" << toolkit::hexmem(candidate, sample_size)
             << " has_header:" << has_header
             << " total_len:" << (has_header ? header.total_len : 0)
             << " payload_len:" << (has_header ? header.payload_len : 0)
             << " task_id_len:" << (has_header ? header.task_id_len : 0)
             << " file_name_len:" << (has_header ? header.file_name_len : 0);
    }
    return false;
  }
  consumed = start + local_consumed;
  return true;
}

void EsFileFerryUnPacker::dispatchPacket(EsFilePacket packet) {
  OnTaskData on_task_data;
  TaskRuntimeState state_snapshot;
  bool has_state = false;
  const bool is_control_packet = packet.task_id == kBootstrapTaskId;
  {
    std::lock_guard<std::mutex> lock(_mtx);
    auto it = _task_callbacks.find(packet.task_id);
    if (it == _task_callbacks.end()) {
      size_t miss_count = 0;
      if (shouldLogMissingTask(packet.task_id, miss_count)) {
        WarnL << "task_id not found, task_id:" << packet.task_id
              << " packet_type:" << EsFilePacketTypeToString(packet.header.type)
              << " seq:" << packet.header.seq
              << " payload_len:" << packet.header.payload_len
              << " file_size:" << packet.header.file_size
              << " miss_count:" << miss_count
              << " this:" << this;
      }
      return;
    }
    if (packet.header.type == EsFilePacketType::FileInfo) {
      DebugL << "dispatch file info, task_id:" << packet.task_id
             << " seq:" << packet.header.seq
             << " payload_len:" << packet.header.payload_len
             << " file_size:" << packet.header.file_size
             << " flags:" << packet.header.flags
             << " callback_count:" << _task_callbacks.size()
             << " this:" << this;
    }
    on_task_data = it->second;
    if (!is_control_packet) {
      auto &state = _task_states[packet.task_id];
      state.matched_packet_count++;
      state.matched_bytes += packet.header.payload_len;
      if (packet.header.file_size > 0) {
        state.file_size = packet.header.file_size;
      }
      if (packet.header.type == EsFilePacketType::FileInfo) {
        state.received_size = 0;
        state.completed = false;
        state.has_seq = false;
      } else if (packet.header.type == EsFilePacketType::FileChunk) {
        const auto current =
            packet.header.data_offset + packet.header.payload_len;
        if (current > state.received_size) {
          state.received_size = current;
        }
      } else if (packet.header.type == EsFilePacketType::FileEnd) {
        const auto current =
            packet.header.data_offset + packet.header.payload_len;
        if (current > state.received_size) {
          state.received_size = current;
        }
        if (state.file_size > 0) {
          state.received_size = state.file_size;
        }
        state.completed = true;
      }
      if (state.has_seq) {
        if (packet.header.seq == state.last_seq) {
          state.duplicate_seq_count++;
        } else if (packet.header.seq < state.last_seq) {
          state.out_of_order_seq_count++;
        }
      }
      state.last_seq = packet.header.seq;
      state.has_seq = true;
      state_snapshot = state;
      has_state = true;
    }
  }

  if (!on_task_data) {
    return;
  }
  EsTaskDataEvent event;
  event.type = packet.header.type;
  event.task_id = packet.task_id;
  event.file_name = packet.file_name;
  event.file_size = packet.header.file_size > 0
                        ? packet.header.file_size
                        : (has_state ? state_snapshot.file_size : 0);
  event.offset = packet.header.data_offset;
  event.seq = packet.header.seq;
  event.flags = packet.header.flags;
  if (has_state) {
    event.received_size = state_snapshot.received_size;
    event.completed = state_snapshot.completed;
  } else {
    event.received_size = packet.header.data_offset + packet.header.payload_len;
    event.completed = packet.header.type == EsFilePacketType::FileEnd;
  }
  if (event.file_size > 0) {
    event.progress = static_cast<double>(event.received_size) /
                     static_cast<double>(event.file_size);
    if (event.progress > 1.0) {
      event.progress = 1.0;
    }
  } else {
    event.progress =
        packet.header.type == EsFilePacketType::FileEnd ? 1.0 : 0.0;
  }
  event.payload = std::move(packet.payload);
  switch (packet.header.type) {
  case EsFilePacketType::TaskStatus:
    event.status.assign(event.payload.begin(), event.payload.end());
    if (event.status.empty()) {
      event.status = "task_status";
    }
    break;
  case EsFilePacketType::FileInfo:
    event.status = "file_info";
    break;
  case EsFilePacketType::FileChunk:
    event.status = "file_chunk";
    break;
  case EsFilePacketType::FileEnd:
    event.status = "file_end";
    break;
  default:
    event.status = "unknown";
    break;
  }
  on_task_data(event);
}

void EsFileFerryUnPacker::setLastError(const std::string &err) {
  std::lock_guard<std::mutex> lock(_mtx);
  _last_error = err;
}

void EsFileFerryUnPacker::emitError(const std::string &err) {
  OnError cb;
  {
    std::lock_guard<std::mutex> lock(_mtx);
    cb = _on_error;
  }
  if (cb) {
    cb(err);
  }
}

void EsFileFerryUnPacker::compactBufferLocked() {
  if (_buffer_start == 0) {
    return;
  }
  if (_buffer_start >= _buffer.size()) {
    _buffer.clear();
    _buffer_start = 0;
    return;
  }
  if (_buffer_start >= 4096 && _buffer_start * 2 >= _buffer.size()) {
    _buffer.erase(_buffer.begin(), _buffer.begin() + static_cast<long>(_buffer_start));
    _buffer_start = 0;
  }
}
