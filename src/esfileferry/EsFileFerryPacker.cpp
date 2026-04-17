#include "EsFileFerryPacker.h"
#include "Util/logger.h"
#include <algorithm>
#include <cctype>
#include <cstring>
#include <cstdlib>
#include <thread>
#include <utility>

constexpr size_t EsFileFerryPacker::kDefaultPacketChunkBytes;
constexpr size_t EsFileFerryPacker::kDefaultHttpBufferChunkBytes;
constexpr size_t EsFileFerryPacker::kDefaultMaxHttpBufferBlocksPerTask;
constexpr uint64_t EsFileFerryPacker::kDefaultMaxHttpBufferedBytesPerTask;
constexpr size_t EsFileFerryPacker::kDefaultHttpFetchConcurrency;
constexpr uint64_t EsFileFerryPacker::kDefaultMaxHttpBufferedBytes;
constexpr uint32_t EsFileFerryPacker::kDefaultBootstrapIntervalMs;
constexpr uint64_t EsFileFerryPacker::kDefaultTickPayloadBudgetBytes;

EsFileFerryPacker &EsFileFerryPacker::Instance() {
  static std::shared_ptr<EsFileFerryPacker> instance(new EsFileFerryPacker());
  static EsFileFerryPacker &ref = *instance;
  return ref;
}

EsFileFerryPacker::EsFileFerryPacker()
    : _http_chunk_pool(kDefaultHttpBufferChunkBytes) {
  std::lock_guard<std::mutex> lock(_mtx);
  _http_chunk_pool.setSize(kDefaultMaxHttpBufferBlocksPerTask * 8);
  startPacketThreadLocked();
  startBootstrapTimerLocked();
}

EsFileFerryPacker::~EsFileFerryPacker() {
  std::thread join_thread;
  {
    std::lock_guard<std::mutex> lock(_mtx);
    stopBootstrapTimerLocked();
    stopPacketThreadLocked(join_thread);
  }
  if (join_thread.joinable()) {
    join_thread.join();
  }
}

namespace {
constexpr int64_t kHttpBufferWaitSlowLogMs = 1000;
constexpr int64_t kEmitPacketSlowLogMs = 1000;

bool isHttpUrl(const std::string &value) {
  return value.rfind("http://", 0) == 0 || value.rfind("https://", 0) == 0;
}

std::string inferHttpFileName(const std::string &url, const std::string &task_id) {
  auto qpos = url.find('?');
  auto pure = qpos == std::string::npos ? url : url.substr(0, qpos);
  auto pos = pure.find_last_of("/\\");
  if (pos != std::string::npos && pos + 1 < pure.size()) {
    auto name = pure.substr(pos + 1);
    if (!name.empty()) {
      return name;
    }
  }
  return "api_" + task_id + ".bin";
}

std::string toLowerCopy(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(),
                 [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  return value;
}

void writeU32BEAt(std::vector<uint8_t> &out, size_t offset, uint32_t value) {
  out[offset + 0] = static_cast<uint8_t>((value >> 24) & 0xFF);
  out[offset + 1] = static_cast<uint8_t>((value >> 16) & 0xFF);
  out[offset + 2] = static_cast<uint8_t>((value >> 8) & 0xFF);
  out[offset + 3] = static_cast<uint8_t>(value & 0xFF);
}

bool equalsIgnoreCase(std::string lhs, std::string rhs) {
  lhs = toLowerCopy(std::move(lhs));
  rhs = toLowerCopy(std::move(rhs));
  return lhs == rhs;
}

std::vector<uint8_t> buildHttpResponseMetaPayload(
    uint32_t status_code, const EsFileFerryPacker::HttpHeaders &headers) {
  std::string text = ":status: " + std::to_string(status_code) + "\n";
  for (const auto &header : headers) {
    if (header.first.empty()) {
      continue;
    }
    text += header.first;
    text += ": ";
    text += header.second;
    text.push_back('\n');
  }
  return std::vector<uint8_t>(text.begin(), text.end());
}

bool tryParseContentLength(const EsFileFerryPacker::HttpHeaders &headers,
                           uint64_t &content_length) {
  for (const auto &header : headers) {
    if (!equalsIgnoreCase(header.first, "content-length")) {
      continue;
    }
    content_length = std::strtoull(header.second.c_str(), nullptr, 10);
    return true;
  }
  return false;
}

} // namespace

void EsFileFerryPacker::setChunkSize(size_t chunk_size) {
  std::lock_guard<std::mutex> lock(_mtx);
  _chunk_size = chunk_size == 0 ? kDefaultPacketChunkBytes : chunk_size;
}

void EsFileFerryPacker::setTickPayloadQuotaBytes(uint64_t quota_bytes) {
  std::lock_guard<std::mutex> lock(_mtx);
  _tick_payload_quota_bytes =
      quota_bytes == 0 ? kDefaultTickPayloadBudgetBytes : quota_bytes;
  _packet_sem.post();
}

void EsFileFerryPacker::setMaxHttpFetchConcurrency(size_t max_concurrency) {
  {
    std::lock_guard<std::mutex> lock(_mtx);
    _max_http_fetch_concurrency =
        max_concurrency == 0 ? kDefaultHttpFetchConcurrency : max_concurrency;
  }
  maybeStartPendingHttpFetches();
  _packet_sem.post();
}

void EsFileFerryPacker::setMaxTotalHttpBufferedBytes(uint64_t max_buffered_bytes) {
  {
    std::lock_guard<std::mutex> lock(_mtx);
    _max_total_http_buffered_bytes = max_buffered_bytes == 0
                                         ? kDefaultMaxHttpBufferedBytes
                                         : max_buffered_bytes;
  }
  maybeStartPendingHttpFetches();
  _packet_sem.post();
}

void EsFileFerryPacker::setPacketCallback(PacketCallback cb) {
  std::lock_guard<std::mutex> lock(_mtx);
  _packet_cb = std::move(cb);
  if (_packet_cb) {
    _ts_started = false;
    _last_ts_ms = 0;
    _bootstrap_due = true;
    _packet_sem.post();
  } else {
    _bootstrap_due = false;
  }
}

bool EsFileFerryPacker::addFileTask(const std::string &task_id,
                                    const std::string &file_path,
                                    const std::string &file_name) {
  return addTask(task_id, file_path, "GET", {}, "", file_name);
}

bool EsFileFerryPacker::addHttpTask(const std::string &task_id,
                                    const std::string &url,
                                    const std::string &method,
                                    const HttpHeaders &headers,
                                    const std::string &body,
                                    const std::string &file_name) {
  return addTask(task_id, url, method, headers, body, file_name);
}

bool EsFileFerryPacker::addTask(const std::string &task_id,
                                const std::string &source,
                                const std::string &method,
                                const HttpHeaders &headers,
                                const std::string &body,
                                const std::string &file_name) {
  if (task_id.empty()) {
    setLastError("task_id is empty");
    return false;
  }
  if (source.empty()) {
    setLastError("source is empty");
    return false;
  }
  TaskState state;
  state.task_id = task_id;
  if (isHttpUrl(source)) {
    std::string method_upper = method;
    std::transform(
        method_upper.begin(), method_upper.end(), method_upper.begin(),
        [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
    if (method_upper != "GET" && method_upper != "POST") {
      setLastError("method must be GET or POST");
      return false;
    }
    state.file_path = source;
    state.file_name =
        file_name.empty() ? inferHttpFileName(source, task_id) : file_name;
    state.file_size = 0;
    state.http.source = true;
    state.http.queued = true;
    state.http.active = false;
    state.http.failed = false;
    state.http.headers_ready = false;
    state.http.size_known = false;
    state.http.received_bytes = 0;
    state.http.method = method_upper;
    state.http.request_headers = headers;
    state.http.request_body = body;
    state.http_buffer.cv = std::make_shared<std::condition_variable>();
    uint64_t generation = 0;
    {
      std::lock_guard<std::mutex> lock(_mtx);
      generation = ++_task_generation;
      state.generation = generation;
      _tasks[task_id] = std::move(state);
      _pending_http_fetches.emplace_back(task_id, generation);
    }
    maybeStartPendingHttpFetches();
    _packet_sem.post();
    return true;
  } else {
    uint64_t file_size = 0;
    if (!getFileSize(source, file_size)) {
      setLastError("open file failed: " + source);
      return false;
    }
    auto stream = std::make_shared<std::ifstream>(source, std::ios::binary);
    if (!stream->is_open()) {
      setLastError("open file failed: " + source);
      return false;
    }
    state.file_path = source;
    state.file_name = pickFileName(source, file_name);
    state.file_size = file_size;
    state.memory_mode = false;
    state.stream = std::move(stream);
    std::lock_guard<std::mutex> lock(_mtx);
    state.generation = ++_task_generation;
    _tasks[task_id] = std::move(state);
    _packet_sem.post();
    return true;
  }

}

void EsFileFerryPacker::removeTask(const std::string &task_id) {
  std::shared_ptr<std::condition_variable> buffer_cv;
  bool task_found = false;
  bool info_sent = false;
  bool end_sent = false;
  bool http_pending = false;
  uint64_t sent_bytes = 0;
  uint64_t file_size = 0;
  bool should_schedule_http = false;
  {
    std::lock_guard<std::mutex> lock(_mtx);
    auto it = _tasks.find(task_id);
    if (it != _tasks.end()) {
      task_found = true;
      buffer_cv = it->second.http_buffer.cv;
      info_sent = it->second.send.info_sent;
      end_sent = it->second.send.end_sent;
      http_pending = it->second.http.queued || it->second.http.active;
      sent_bytes = it->second.send.sent_bytes;
      file_size = it->second.file_size;
      erasePendingHttpFetchLocked(task_id, it->second.generation);
      clearTaskHttpBufferLocked(it->second);
    }
    _tasks.erase(task_id);
    should_schedule_http = !_pending_http_fetches.empty();
  }
  InfoL << "remove ferry task, task_id:" << task_id
        << " found:" << task_found
        << " info_sent:" << info_sent
        << " end_sent:" << end_sent
        << " http_pending:" << http_pending
        << " sent_bytes:" << sent_bytes
        << " file_size:" << file_size;
  if (buffer_cv) {
    buffer_cv->notify_all();
  }
  if (should_schedule_http) {
    maybeStartPendingHttpFetches();
  }
  _packet_sem.post();
}

void EsFileFerryPacker::clearTasks() {
  std::vector<std::shared_ptr<std::condition_variable>> buffer_cvs;
  bool should_schedule_http = false;
  {
    std::lock_guard<std::mutex> lock(_mtx);
    buffer_cvs.reserve(_tasks.size());
    for (const auto &it : _tasks) {
      if (it.second.http_buffer.cv) {
        buffer_cvs.emplace_back(it.second.http_buffer.cv);
      }
    }
    _pending_http_fetches.clear();
    _total_http_buffered_bytes = 0;
    _tasks.clear();
    _rr_cursor = 0;
    should_schedule_http = !_pending_http_fetches.empty();
  }
  for (const auto &buffer_cv : buffer_cvs) {
    buffer_cv->notify_all();
  }
  if (should_schedule_http) {
    maybeStartPendingHttpFetches();
  }
  _packet_sem.post();
}

bool EsFileFerryPacker::onBootstrapTimer() {
  std::lock_guard<std::mutex> lock(_mtx);
  if (_packet_thread_exit) {
    return false;
  }
  if (_packet_cb) {
    _bootstrap_due = true;
    _packet_sem.post();
  }
  return true;
}

void EsFileFerryPacker::startBootstrapTimerLocked() {
  if (_bootstrap_timer) {
    return;
  }
  _bootstrap_timer = std::make_shared<toolkit::Timer>(
      static_cast<float>(kDefaultBootstrapIntervalMs) / 1000.0f,
      [this]() { return onBootstrapTimer(); }, nullptr);
}

void EsFileFerryPacker::stopBootstrapTimerLocked() { _bootstrap_timer.reset(); }

void EsFileFerryPacker::startPacketThreadLocked() {
  if (_packet_thread_running) {
    return;
  }
  _packet_thread_exit = false;
  _packet_thread_running = true;
  _packet_thread = std::thread([this]() { packetThreadLoop(); });
}

void EsFileFerryPacker::stopPacketThreadLocked(std::thread &join_thread) {
  if (!_packet_thread_running) {
    return;
  }
  _packet_thread_exit = true;
  _packet_sem.post();
  if (_packet_thread.joinable()) {
    join_thread = std::move(_packet_thread);
  }
  _packet_thread_running = false;
  _bootstrap_due = false;
}

void EsFileFerryPacker::packetThreadLoop() {
  while (true) {
    _packet_sem.wait();
    while (true) {
      PacketCallback bootstrap_cb;
      bool should_exit = false;
      bool should_emit_bootstrap = false;
      uint64_t tick_payload_quota_bytes = 0;
      {
        std::lock_guard<std::mutex> lock(_mtx);
        should_exit = _packet_thread_exit;
        if (should_exit) {
          break;
        }
        should_emit_bootstrap = _bootstrap_due;
        if (should_emit_bootstrap) {
          _bootstrap_due = false;
          bootstrap_cb = _packet_cb;
        }
        tick_payload_quota_bytes = _tick_payload_quota_bytes;
      }
      if (should_emit_bootstrap && bootstrap_cb) {
        emitBootstrapPackets(bootstrap_cb);
      }
      const auto packet_count = processTickPackets(tick_payload_quota_bytes);
      if (!should_emit_bootstrap && packet_count == 0) {
        break;
      }
    }
    bool should_exit = false;
    {
      std::lock_guard<std::mutex> lock(_mtx);
      should_exit = _packet_thread_exit;
    }
    if (should_exit) {
      break;
    }
  }
}

size_t EsFileFerryPacker::processTickPackets(uint64_t total_payload_quota_bytes) {
  size_t packet_count = 0;
  auto active_ids = snapshotActiveTasks();
  if (active_ids.empty()) {
    return packet_count;
  }

  auto round_ids = pickFairRound(active_ids);
  const auto active_count = round_ids.size();

  struct TaskSendSnapshot {
    std::string task_id;
    std::string file_name;
    uint64_t file_size = 0;
    uint32_t next_seq = 0;
    bool info_sent = false;
    bool end_sent = false;
    bool http_fetch_pending = false;
    bool http_failed = false;
    std::string http_error;
    bool http_source = false;
    bool http_headers_ready = false;
    bool need_http_meta_payload = false;
  };

  for (size_t i = 0; i < active_count; ++i) {
    const auto &task_id = round_ids[i];
    uint64_t quota =
        computeTaskQuota(total_payload_quota_bytes, active_count, i);

    TaskSendSnapshot snapshot;
    {
      std::lock_guard<std::mutex> lock(_mtx);
      auto it = _tasks.find(task_id);
      if (it == _tasks.end() || it->second.send.end_sent) {
        continue;
      }
      const auto &task = it->second;
      snapshot.task_id = task.task_id;
      snapshot.file_name = task.file_name;
      snapshot.file_size = task.file_size;
      snapshot.next_seq = task.send.next_seq;
      snapshot.info_sent = task.send.info_sent;
      snapshot.end_sent = task.send.end_sent;
      snapshot.http_fetch_pending = task.http.source && (task.http.queued || task.http.active);
      snapshot.http_failed = task.http.failed;
      snapshot.http_error = task.http.error;
      snapshot.http_source = task.http.source;
      snapshot.http_headers_ready = task.http.headers_ready;
      snapshot.need_http_meta_payload =
          task.http.source && !task.http.response_meta_payload.empty();
    }

    if (snapshot.http_fetch_pending && !snapshot.http_headers_ready) {
      continue;
    }

    if (snapshot.http_failed) {
      std::vector<uint8_t> status_payload(snapshot.http_error.begin(),
                                          snapshot.http_error.end());
      const auto status_ts = nextRelativeTimestampMs();
      TaskState status_task;
      status_task.task_id = snapshot.task_id;
      status_task.file_name = snapshot.file_name;
      status_task.file_size = snapshot.file_size;
      auto status_header =
          makePacketHeader(status_task, EsFilePacketType::TaskStatus, 0,
                           static_cast<uint32_t>(status_payload.size()), 0,
                           snapshot.next_seq, status_ts);
      auto status_packet = buildPacket(status_task, status_header, status_payload);
      if (!emitPacket(task_id, std::move(status_packet), status_header)) {
        continue;
      }
      ++packet_count;
      std::lock_guard<std::mutex> lock(_mtx);
      auto it = _tasks.find(task_id);
      if (it != _tasks.end()) {
        it->second.send.info_sent = true;
        it->second.send.end_sent = true;
        it->second.send.next_seq++;
      }
      continue;
    }

    if (!snapshot.info_sent) {
      std::vector<uint8_t> info_payload;
      uint16_t info_flags = 0;
      if (snapshot.need_http_meta_payload) {
        std::lock_guard<std::mutex> lock(_mtx);
        auto it = _tasks.find(task_id);
        if (it != _tasks.end() && !it->second.http.response_meta_payload.empty()) {
          info_payload = it->second.http.response_meta_payload;
          info_flags = kEsFileFlagFileInfoHasHttpResponseHeaders;
        }
      }
      const auto info_ts = nextRelativeTimestampMs();
      TaskState info_task;
      info_task.task_id = snapshot.task_id;
      info_task.file_name = snapshot.file_name;
      info_task.file_size = snapshot.file_size;
      auto info_header = makePacketHeader(
          info_task, EsFilePacketType::FileInfo, 0,
          static_cast<uint32_t>(info_payload.size()), info_flags, snapshot.next_seq,
          info_ts);
      auto info_packet = buildPacket(info_task, info_header, info_payload);
      if (!emitPacket(task_id, std::move(info_packet), info_header)) {
        continue;
      }
      ++packet_count;
      std::lock_guard<std::mutex> lock(_mtx);
      auto it = _tasks.find(task_id);
      if (it != _tasks.end()) {
        it->second.send.info_sent = true;
        it->second.send.next_seq++;
      }
    }

    while (quota > 0) {
      uint64_t offset = 0;
      uint32_t seq = 0;
      bool no_data = false;
      bool memory_mode = false;
      bool http_chunk_mode = false;
      uint64_t generation = 0;
      size_t read_len = 0;
      const auto packet_ts = nextRelativeTimestampMs();
      std::shared_ptr<std::ifstream> file_stream;
      std::shared_ptr<std::condition_variable> buffer_cv;
      bool can_emit_packet = false;
      bool should_try_start_http = false;
      TaskState packet_task;
      size_t payload_offset = 0;
      EsFilePacketHeader packet_header;
      std::vector<uint8_t> packet;
      {
        std::lock_guard<std::mutex> lock(_mtx);
        auto it = _tasks.find(task_id);
        if (it == _tasks.end() || it->second.send.end_sent) {
          break;
        }
        auto &task = it->second;
        memory_mode = task.memory_mode;
        http_chunk_mode = task.http.source;
        generation = task.generation;
        offset = task.send.sent_bytes;
        seq = task.send.next_seq;
        if (task.http.source) {
          if (!task.http_buffer.chunks.empty()) {
            auto &front_chunk = task.http_buffer.chunks.front();
            const auto available =
                front_chunk->size - task.http_buffer.front_chunk_offset;
            read_len = static_cast<size_t>(std::min<uint64_t>(
                std::min<uint64_t>(quota, _chunk_size), available));
            if (read_len == 0) {
              no_data = true;
            } else {
              packet_task.task_id = task.task_id;
              packet_task.file_name = task.file_name;
              packet_task.file_size = task.file_size;
              packet_header = makePacketHeader(
                  packet_task, EsFilePacketType::FileChunk, offset,
                  static_cast<uint32_t>(read_len), 0, seq, packet_ts);
              packet = buildPacket(packet_task, packet_header, read_len, &payload_offset);
              std::memcpy(packet.data() + payload_offset,
                          front_chunk->data.data() +
                              task.http_buffer.front_chunk_offset,
                          read_len);
              task.send.sent_bytes += read_len;
              task.send.next_seq++;
              task.http_buffer.front_chunk_offset += read_len;
              task.http_buffer.buffered_bytes -= read_len;
              _total_http_buffered_bytes -= read_len;
              if (task.http_buffer.front_chunk_offset >= front_chunk->size) {
                task.http_buffer.chunks.pop_front();
                task.http_buffer.front_chunk_offset = 0;
              }
              quota -= read_len;
              buffer_cv = task.http_buffer.cv;
              can_emit_packet = true;
              should_try_start_http =
                  !_pending_http_fetches.empty() &&
                  _active_http_fetches < _max_http_fetch_concurrency &&
                  _total_http_buffered_bytes < _max_total_http_buffered_bytes;
            }
          } else {
            no_data = true;
          }
        } else if (task.send.sent_bytes >= task.file_size) {
          no_data = true;
        } else {
          auto remain = task.file_size - task.send.sent_bytes;
          read_len = static_cast<size_t>(std::min<uint64_t>(
              std::min<uint64_t>(quota, _chunk_size), remain));
          if (task.memory_mode) {
            if (task.send.sent_bytes < task.memory_payload.size()) {
              const auto available = task.memory_payload.size() -
                                     static_cast<size_t>(task.send.sent_bytes);
              read_len = std::min(read_len, available);
              if (read_len == 0) {
                no_data = true;
              } else {
                packet_task.task_id = task.task_id;
                packet_task.file_name = task.file_name;
                packet_task.file_size = task.file_size;
                packet_header = makePacketHeader(
                    packet_task, EsFilePacketType::FileChunk, offset,
                    static_cast<uint32_t>(read_len), 0, seq, packet_ts);
                packet = buildPacket(packet_task, packet_header, read_len, &payload_offset);
                std::memcpy(packet.data() + payload_offset,
                            task.memory_payload.data() + task.send.sent_bytes, read_len);
                task.send.sent_bytes += read_len;
                task.send.next_seq++;
                quota -= read_len;
                can_emit_packet = true;
              }
            } else {
              no_data = true;
            }
          } else {
            file_stream = task.stream;
          }
        }
      }

      if (no_data || read_len == 0) {
        if (buffer_cv) {
          buffer_cv->notify_one();
        }
        break;
      }

      if (buffer_cv) {
        buffer_cv->notify_one();
      }
      if (should_try_start_http) {
        maybeStartPendingHttpFetches();
      }

      if (http_chunk_mode || memory_mode) {
        if (!can_emit_packet) {
          break;
        }
      } else {
        {
          std::lock_guard<std::mutex> lock(_mtx);
          auto it = _tasks.find(task_id);
          if (it == _tasks.end()) {
            break;
          }
          packet_task.task_id = it->second.task_id;
          packet_task.file_name = it->second.file_name;
          packet_task.file_size = it->second.file_size;
        }
        packet_header = makePacketHeader(packet_task, EsFilePacketType::FileChunk, offset,
                                         static_cast<uint32_t>(read_len), 0, seq, packet_ts);
        packet = buildPacket(packet_task, packet_header, read_len, &payload_offset);
        file_stream->read(
            reinterpret_cast<char *>(packet.data() + static_cast<long>(payload_offset)),
            static_cast<std::streamsize>(read_len));
        auto read_size = static_cast<size_t>(file_stream->gcount());
        if (read_size == 0) {
          break;
        }
        if (read_size < read_len) {
          packet_header.payload_len = static_cast<uint32_t>(read_size);
          packet_header.total_len = static_cast<uint32_t>(
              kEsFileFixedHeaderSize + packet_header.task_id_len +
              packet_header.file_name_len + packet_header.payload_len);
          const auto payload_len_pos = static_cast<size_t>(
              kEsFileCarrierPrefixSize + 24);
          const auto total_len_pos = static_cast<size_t>(
              kEsFileCarrierPrefixSize + 40);
          writeU32BEAt(packet, payload_len_pos, packet_header.payload_len);
          writeU32BEAt(packet, total_len_pos, packet_header.total_len);
          packet.resize(payload_offset + read_size);
        }
        std::lock_guard<std::mutex> lock(_mtx);
        auto it = _tasks.find(task_id);
        if (it == _tasks.end() || it->second.send.end_sent) {
          break;
        }
        auto &task = it->second;
        if (task.generation != generation || task.send.sent_bytes != offset ||
            task.send.next_seq != seq) {
          continue;
        }
        task.send.sent_bytes += read_size;
        task.send.next_seq++;
        quota -= read_size;
      }
      if (emitPacket(task_id, std::move(packet), packet_header)) {
        ++packet_count;
      }
    }

    bool should_end = false;
    uint64_t end_offset = 0;
    uint32_t end_seq = 0;
    TaskState end_task;
    {
      std::lock_guard<std::mutex> lock(_mtx);
      auto it = _tasks.find(task_id);
      const bool ready_to_finish =
          it != _tasks.end() && isTaskReadyToEmitEnd(it->second);
      if (ready_to_finish) {
        should_end = true;
        end_offset = it->second.send.sent_bytes;
        end_seq = it->second.send.next_seq;
        end_task.task_id = it->second.task_id;
        end_task.file_name = it->second.file_name;
        end_task.file_size = it->second.file_size;
        it->second.send.next_seq++;
        it->second.send.end_sent = true;
        it->second.stream.reset();
        clearTaskHttpBufferLocked(it->second);
        it->second.memory_payload.clear();
        it->second.memory_payload.shrink_to_fit();
      }
    }
    if (should_end) {
      const auto end_ts = nextRelativeTimestampMs();
      auto end_header =
          makePacketHeader(end_task, EsFilePacketType::FileEnd, end_offset, 0, 0,
                           end_seq, end_ts);
      auto end_packet = buildPacket(end_task, end_header, {});
      if (emitPacket(task_id, std::move(end_packet), end_header)) {
        ++packet_count;
      }
    }
  }
  return packet_count;
}

void EsFileFerryPacker::clearTaskHttpBufferLocked(TaskState &task) {
  if (task.http_buffer.buffered_bytes > 0) {
    _total_http_buffered_bytes -= task.http_buffer.buffered_bytes;
  }
  task.http_buffer.chunks.clear();
  task.http_buffer.buffered_bytes = 0;
  task.http_buffer.front_chunk_offset = 0;
}

void EsFileFerryPacker::erasePendingHttpFetchLocked(const std::string &task_id,
                                                    uint64_t generation) {
  _pending_http_fetches.erase(
      std::remove_if(_pending_http_fetches.begin(), _pending_http_fetches.end(),
                     [&task_id, generation](const std::pair<std::string, uint64_t> &item) {
                       return item.first == task_id && item.second == generation;
                     }),
      _pending_http_fetches.end());
}

std::vector<std::pair<std::string, uint64_t>>
EsFileFerryPacker::collectHttpFetchLaunchesLocked() {
  std::vector<std::pair<std::string, uint64_t>> launches;
  while (_active_http_fetches < _max_http_fetch_concurrency &&
         !_pending_http_fetches.empty()) {
    if (_total_http_buffered_bytes >= _max_total_http_buffered_bytes) {
      break;
    }
    auto item = _pending_http_fetches.front();
    _pending_http_fetches.pop_front();
    auto it = _tasks.find(item.first);
    if (it == _tasks.end() || it->second.generation != item.second) {
      continue;
    }
    auto &task = it->second;
    if (!task.http.source || !task.http.queued || task.http.active) {
      continue;
    }
    task.http.queued = false;
    task.http.active = true;
    ++_active_http_fetches;
    launches.emplace_back(item);
  }
  return launches;
}

void EsFileFerryPacker::maybeStartPendingHttpFetches() {
  auto launches = [&]() {
    std::lock_guard<std::mutex> lock(_mtx);
    return collectHttpFetchLaunchesLocked();
  }();
  for (const auto &launch : launches) {
    launchHttpFetchTask(launch.first, launch.second);
  }
}

void EsFileFerryPacker::launchHttpFetchTask(const std::string &task_id,
                                            uint64_t generation) {
  std::string source;
  std::string method;
  HttpHeaders headers;
  std::string body;
  {
    std::lock_guard<std::mutex> lock(_mtx);
    auto it = _tasks.find(task_id);
    if (it == _tasks.end() || it->second.generation != generation) {
      if (_active_http_fetches > 0) {
        --_active_http_fetches;
      }
      return;
    }
    source = it->second.file_path;
    method = it->second.http.method;
    headers = it->second.http.request_headers;
    body = it->second.http.request_body;
  }

  std::thread([this, task_id, generation, source, method, headers, body]() {
    const auto fetch_begin = std::chrono::steady_clock::now();
    HttpHeaders response_headers;
    uint32_t response_status_code = 0;
    std::string fetch_err;
    const bool ok = HttpStreamFetcher::stream(
        source, method, headers, body,
        [this, task_id, generation](const uint8_t *data, size_t size) {
          if (!data || size == 0) {
            return true;
          }
          size_t consumed = 0;
          while (consumed < size) {
            size_t copy_len = 0;
            {
              std::unique_lock<std::mutex> lock(_mtx);
              auto has_buffer_space = [this, &task_id, generation]() {
                auto it = _tasks.find(task_id);
                if (it == _tasks.end() || it->second.generation != generation) {
                  return true;
                }
                const auto &task = it->second;
                return task.http_buffer.buffered_bytes <
                           kDefaultMaxHttpBufferedBytesPerTask &&
                       _total_http_buffered_bytes < _max_total_http_buffered_bytes;
              };
              bool waited_for_buffer = false;
              const auto wait_begin = std::chrono::steady_clock::now();
              while (true) {
                if (has_buffer_space()) {
                  break;
                }
                waited_for_buffer = true;
                auto it = _tasks.find(task_id);
                if (it == _tasks.end() || it->second.generation != generation) {
                  return false;
                }
                auto buffer_cv = it->second.http_buffer.cv;
                if (!buffer_cv) {
                  return false;
                }
                buffer_cv->wait(lock, has_buffer_space);
              }
              if (waited_for_buffer) {
                const auto wait_ms =
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - wait_begin)
                        .count();
                if (wait_ms >= kHttpBufferWaitSlowLogMs) {
                  auto it = _tasks.find(task_id);
                  const auto buffered_bytes =
                      it != _tasks.end() ? it->second.http_buffer.buffered_bytes : 0;
                  WarnL << "http chunk buffer wait, task_id:" << task_id
                        << " wait_ms:" << wait_ms
                        << " task_buffered_bytes:" << buffered_bytes
                        << " total_buffered_bytes:" << _total_http_buffered_bytes
                        << " chunk_block_size:" << kDefaultHttpBufferChunkBytes
                        << " block_limit:" << kDefaultMaxHttpBufferBlocksPerTask;
                }
              }
              auto it = _tasks.find(task_id);
              if (it == _tasks.end() || it->second.generation != generation) {
                return false;
              }
              auto &task = it->second;
              auto chunk = _http_chunk_pool.obtain([](HttpChunkBuffer *buffer) {
                buffer->size = 0;
              });
              copy_len = std::min(size - consumed, chunk->data.size());
              std::memcpy(chunk->data.data(), data + consumed, copy_len);
              chunk->size = copy_len;
              task.http.received_bytes += copy_len;
              task.http_buffer.buffered_bytes += copy_len;
              _total_http_buffered_bytes += copy_len;
              task.http_buffer.chunks.emplace_back(std::move(chunk));
            }
            consumed += copy_len;
            _packet_sem.post();
          }
          return true;
        },
        [this, task_id, generation](uint32_t status_code,
                                    const HttpHeaders &headers_in) {
          uint64_t content_length = 0;
          const bool has_content_length =
              tryParseContentLength(headers_in, content_length);
          std::lock_guard<std::mutex> lock(_mtx);
          auto it = _tasks.find(task_id);
          if (it == _tasks.end() || it->second.generation != generation) {
            return;
          }
          auto &task = it->second;
          task.http.status_code = status_code;
          task.http.response_meta_payload =
              buildHttpResponseMetaPayload(status_code, headers_in);
          task.http.headers_ready = true;
          if (has_content_length) {
            task.file_size = content_length;
            task.http.size_known = true;
          }
          _packet_sem.post();
        },
        &response_headers, &response_status_code, fetch_err);

    const auto fetch_cost_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - fetch_begin)
            .count();
    uint64_t fetched_bytes = 0;
    uint64_t final_file_size = 0;
    bool final_size_known = false;
    size_t final_buffered_bytes = 0;
    std::shared_ptr<std::condition_variable> buffer_cv;
    {
      std::lock_guard<std::mutex> lock(_mtx);
      if (_active_http_fetches > 0) {
        --_active_http_fetches;
      }
      auto it = _tasks.find(task_id);
      if (it != _tasks.end() && it->second.generation == generation) {
        auto &task = it->second;
        fetched_bytes = task.http.received_bytes;
        final_file_size = task.file_size;
        final_size_known = task.http.size_known;
        final_buffered_bytes = task.http_buffer.buffered_bytes;
        task.http.active = false;
        task.http.status_code = response_status_code;
        task.http.response_meta_payload =
            buildHttpResponseMetaPayload(response_status_code, response_headers);
        task.http.headers_ready = true;
        if (!task.http.size_known) {
          task.file_size = task.http.received_bytes;
          task.http.size_known = true;
          final_file_size = task.file_size;
          final_size_known = true;
        }
        buffer_cv = task.http_buffer.cv;
        if (!ok) {
          task.http.failed = true;
          task.http.error = fetch_err.empty() ? "http fetch failed" : fetch_err;
          clearTaskHttpBufferLocked(task);
          final_buffered_bytes = task.http_buffer.buffered_bytes;
        } else {
          task.http.failed = false;
          task.http.error.clear();
        }
      }
    }
    if (buffer_cv) {
      buffer_cv->notify_all();
    }
    InfoL << "http fetch task_id:" << task_id << " method:" << method
          << " status:" << response_status_code << " ok:" << ok
          << " bytes:" << fetched_bytes << " file_size:" << final_file_size
          << " size_known:" << final_size_known
          << " buffered_bytes:" << final_buffered_bytes
          << " fetch_err:" << fetch_err << " cost_ms:" << fetch_cost_ms
          << " url:" << source;
    maybeStartPendingHttpFetches();
    _packet_sem.post();
  }).detach();
}

std::vector<EsFilePackTaskInfo> EsFileFerryPacker::getTaskInfos() const {
  std::lock_guard<std::mutex> lock(_mtx);
  std::vector<EsFilePackTaskInfo> infos;
  infos.reserve(_tasks.size());
  for (const auto &it : _tasks) {
    EsFilePackTaskInfo info;
    info.task_id = it.second.task_id;
    info.file_path = it.second.file_path;
    info.file_name = it.second.file_name;
    info.file_size = it.second.file_size;
    info.sent_bytes = it.second.send.sent_bytes;
    info.next_seq = it.second.send.next_seq;
    info.info_sent = it.second.send.info_sent;
    info.completed = it.second.send.end_sent;
    infos.emplace_back(std::move(info));
  }
  return infos;
}

std::string EsFileFerryPacker::getLastError() const {
  std::lock_guard<std::mutex> lock(_mtx);
  return _last_error;
}

bool EsFileFerryPacker::getFileSize(const std::string &file_path,
                                    uint64_t &size) {
  std::ifstream ifs(file_path, std::ios::binary | std::ios::ate);
  if (!ifs.is_open()) {
    return false;
  }
  auto end = ifs.tellg();
  if (end < 0) {
    return false;
  }
  size = static_cast<uint64_t>(end);
  return true;
}

std::string EsFileFerryPacker::pickFileName(const std::string &file_path,
                                            const std::string &file_name) {
  if (!file_name.empty()) {
    return file_name;
  }
  auto pos = file_path.find_last_of("/\\");
  if (pos == std::string::npos) {
    return file_path;
  }
  return file_path.substr(pos + 1);
}

EsFilePacketHeader EsFileFerryPacker::makePacketHeader(
    const TaskState &task, EsFilePacketType type, uint64_t data_offset,
    uint32_t payload_len, uint16_t flags, uint32_t seq,
    uint32_t timestamp_ms) const {
  EsFilePacketHeader header;
  header.magic = kEsFilePacketMagic;
  header.version = kEsFilePacketVersion;
  header.type = type;
  header.task_id_len = static_cast<uint16_t>(task.task_id.size());
  header.file_name_len = static_cast<uint16_t>(task.file_name.size());
  header.flags = flags;
  header.seq = seq;
  header.data_offset = data_offset;
  header.payload_len = payload_len;
  header.file_size = task.file_size;
  header.crc32 = 0;
  header.total_len = static_cast<uint32_t>(
      kEsFileFixedHeaderSize + header.task_id_len + header.file_name_len + payload_len);
  header.reserved = timestamp_ms;
  return header;
}

void EsFileFerryPacker::emitBootstrapPackets(const PacketCallback &cb) const {
  std::vector<uint8_t> sps = {0x00, 0x00, 0x00, 0x01, 0x67, 0x42,
                              0xC0, 0x1E, 0xDA, 0x02, 0x80, 0x2D,
                              0xD0, 0x80, 0x80, 0xA0};
  std::vector<uint8_t> pps = {0x00, 0x00, 0x00, 0x01,
                              0x68, 0xCE, 0x06, 0xE2};
  std::vector<uint8_t> idr = {0x00, 0x00, 0x00, 0x01, 0x65,
                              0x88, 0x84, 0x21, 0xA0};
  EsFilePacketHeader header;
  cb(kBootstrapTaskId, std::move(sps), header);
  cb(kBootstrapTaskId, std::move(pps), header);
  cb(kBootstrapTaskId, std::move(idr), header);
}

uint32_t EsFileFerryPacker::nextRelativeTimestampMs() {
  std::lock_guard<std::mutex> lock(_mtx);
  const auto now = std::chrono::steady_clock::now();
  if (!_ts_started) {
    _ts_started = true;
    _ts_start_time = now;
    _last_ts_ms = 0;
    return _last_ts_ms;
  }
  const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              now - _ts_start_time)
                              .count();
  const uint64_t elapsed_u64 =
      elapsed_ms < 0 ? 0 : static_cast<uint64_t>(elapsed_ms);
  const uint32_t current = static_cast<uint32_t>(elapsed_u64 & 0xFFFFFFFFu);
  _last_ts_ms = current;
  return _last_ts_ms;
}

std::vector<uint8_t> EsFileFerryPacker::buildPacket(
    const TaskState &task, EsFilePacketHeader &header,
    const std::vector<uint8_t> &payload) const {
  size_t payload_offset = 0;
  auto out = buildPacket(task, header, payload.size(), &payload_offset);
  if (!payload.empty()) {
    std::memcpy(out.data() + payload_offset, payload.data(), payload.size());
  }
  return out;
}

std::vector<uint8_t> EsFileFerryPacker::buildPacket(
    const TaskState &task, EsFilePacketHeader &header, size_t payload_len,
    size_t *payload_offset) const {
  header.task_id_len = static_cast<uint16_t>(task.task_id.size());
  header.file_name_len = static_cast<uint16_t>(task.file_name.size());
  header.payload_len = static_cast<uint32_t>(payload_len);
  header.total_len = static_cast<uint32_t>(kEsFileFixedHeaderSize + header.task_id_len +
                                           header.file_name_len + header.payload_len);
  header.magic = kEsFilePacketMagic;
  header.version = kEsFilePacketVersion;
  header.file_size = task.file_size;
  const auto total_len = header.total_len;

  std::vector<uint8_t> out;
  out.reserve(total_len + kEsFileCarrierPrefixSize);
  AppendEsFileCarrierPrefix(out);
  AppendEsFilePacketHeader(out, header);
  out.insert(out.end(), task.task_id.begin(), task.task_id.end());
  out.insert(out.end(), task.file_name.begin(), task.file_name.end());
  const auto data_offset = out.size();
  if (payload_offset) {
    *payload_offset = data_offset;
  }
  out.resize(data_offset + payload_len);
  return out;
}

bool EsFileFerryPacker::emitPacket(
    const std::string &task_id, std::vector<uint8_t> &&packet,
    const EsFilePacketHeader &header) const {
  PacketCallback cb;
  {
    std::lock_guard<std::mutex> lock(_mtx);
    cb = _packet_cb;
  }
  if (cb) {
    if (header.type == EsFilePacketType::FileInfo ||
        header.type == EsFilePacketType::FileEnd) {
      ErrorL << "emit packet callback, task_id:" << task_id
             << " type:" << EsFilePacketTypeToString(header.type)
             << " seq:" << header.seq
             << " offset:" << header.data_offset
             << " payload_len:" << header.payload_len
             << " file_size:" << header.file_size
             << " total_packet_size:" << packet.size();
    }
    const auto emit_begin = std::chrono::steady_clock::now();
    cb(task_id, std::move(packet), header);
    const auto emit_cost_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - emit_begin)
            .count();
    if (emit_cost_ms >= kEmitPacketSlowLogMs) {
      WarnL << "emit packet callback slow, task_id:" << task_id
            << " type:" << EsFilePacketTypeToString(header.type)
            << " seq:" << header.seq
            << " payload_len:" << header.payload_len
            << " cost_ms:" << emit_cost_ms;
    }
  }
  return true;
}

void EsFileFerryPacker::setLastError(const std::string &err) {
  std::lock_guard<std::mutex> lock(_mtx);
  _last_error = err;
}

std::vector<std::string> EsFileFerryPacker::snapshotActiveTasks() const {
  std::lock_guard<std::mutex> lock(_mtx);
  std::vector<std::string> ids;
  ids.reserve(_tasks.size());
  for (const auto &it : _tasks) {
    if (!it.second.send.end_sent) {
      ids.emplace_back(it.first);
    }
  }
  return ids;
}

std::vector<std::string>
EsFileFerryPacker::pickFairRound(const std::vector<std::string> &active_ids) {
  if (active_ids.size() <= 1) {
    return active_ids;
  }
  std::vector<std::string> sorted = active_ids;
  std::sort(sorted.begin(), sorted.end());
  if (sorted.empty()) {
    return sorted;
  }
  std::lock_guard<std::mutex> lock(_mtx);
  _rr_cursor %= sorted.size();
  std::vector<std::string> round;
  round.reserve(sorted.size());
  for (size_t i = 0; i < sorted.size(); ++i) {
    round.emplace_back(sorted[(_rr_cursor + i) % sorted.size()]);
  }
  _rr_cursor = (_rr_cursor + 1) % sorted.size();
  return round;
}

uint64_t EsFileFerryPacker::computeTaskQuota(uint64_t total_payload_quota_bytes,
                                             size_t active_count,
                                             size_t index) const {
  if (active_count == 0) {
    return 0;
  }
  const auto base = total_payload_quota_bytes / active_count;
  const auto extra = total_payload_quota_bytes % active_count;
  return base + (index < extra ? 1 : 0);
}

bool EsFileFerryPacker::isTaskReadyToEmitEnd(
    const EsFileFerryPacker::TaskState &task) {
  if (task.send.end_sent || !task.send.info_sent) {
    return false;
  }
  if (task.http.source) {
    if (task.http.queued || task.http.active || task.http.failed) {
      return false;
    }
    return task.http_buffer.chunks.empty() &&
           task.http_buffer.buffered_bytes == 0 &&
           task.http_buffer.front_chunk_offset == 0;
  }
  return task.send.sent_bytes >= task.file_size;
}
