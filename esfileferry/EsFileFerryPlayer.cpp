#include "EsFileFerryPlayer.h"
#include "Util/logger.h"
#include <cstring>

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

bool EsFileFerryUnPacker::inputFrame(const uint8_t *data, size_t size) {
  if (!data || size == 0) {
    return true;
  }
//  DebugL << " hex:" << toolkit::hexmem(data, 8)  << " size:" << size;

  size_t offset = 0;
  while (offset < size) {
    EsFilePacket packet;
    size_t consumed = 0;
    if (!parseOnePacketFromRaw(data + offset, size - offset, packet,
                               consumed) ||
        consumed == 0) {
      break;
    }
    offset += consumed;
    if (packet.task_id == kBootstrapTaskId) {
      continue;
    }
    dispatchPacket(std::move(packet));
  }
  if (offset < size) {
    std::lock_guard<std::mutex> lock(_mtx);
    _buffer.insert(_buffer.end(), data + offset, data + size);
    compactBufferLocked();
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
      return;
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
