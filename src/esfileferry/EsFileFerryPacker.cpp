#include "EsFileFerryPacker.h"
#include "Util/logger.h"
#include "Util/base64.h"
#include <algorithm>
#include <cctype>
#include <cstring>
#include <cstdlib>
#include <thread>
#include <unordered_set>
#include <utility>

constexpr size_t EsFileFerryPacker::kDefaultPacketChunkBytes;
constexpr size_t EsFileFerryPacker::kDefaultHttpBufferChunkBytes;
constexpr size_t EsFileFerryPacker::kDefaultMaxHttpBufferBlocksPerTask;
constexpr uint64_t EsFileFerryPacker::kDefaultMaxHttpBufferedBytesPerTask;
constexpr size_t EsFileFerryPacker::kDefaultHttpFetchConcurrency;
constexpr uint64_t EsFileFerryPacker::kDefaultMaxHttpBufferedBytes;
constexpr uint32_t EsFileFerryPacker::kDefaultBootstrapIntervalMs;
constexpr uint32_t EsFileFerryPacker::kDefaultPaceIntervalMs;
constexpr uint64_t EsFileFerryPacker::kDefaultSchedulerRoundBudgetBytes;
constexpr uint64_t EsFileFerryPacker::kDefaultUnifiedBufferedBytes;

EsFileFerryPacker &EsFileFerryPacker::Instance() {
  static std::shared_ptr<EsFileFerryPacker> instance(new EsFileFerryPacker());
  static EsFileFerryPacker &ref = *instance;
  return ref;
}

EsFileFerryPacker::EsFileFerryPacker()
    : _http_chunk_pool(kDefaultHttpBufferChunkBytes),
      _http_fetch_engine(new CurlMultiHttpFetchEngine()) {
  std::lock_guard<std::mutex> lock(_mtx);
  _global_options = EsFileGlobalOptions{};
  _http_chunk_pool.setSize(kDefaultMaxHttpBufferBlocksPerTask * 8);
  startPacketThreadLocked();
  startBootstrapTimerLocked();
  startPaceTimerLocked();
}

EsFileFerryPacker::~EsFileFerryPacker() {
  std::thread join_thread;
  std::vector<std::thread> http_join_threads;
  std::vector<std::shared_ptr<std::condition_variable>> buffer_cvs;
  {
    std::lock_guard<std::mutex> lock(_mtx);
    stopPaceTimerLocked();
    stopBootstrapTimerLocked();
    stopPacketThreadLocked(join_thread);
    buffer_cvs.reserve(_task_registry.tasks.size());
    for (const auto &item : _task_registry.tasks) {
      if (item.second.http.buffer.cv) {
        buffer_cvs.emplace_back(item.second.http.buffer.cv);
      }
    }
    if (_http_fetch_engine) {
      _http_fetch_engine->shutdown(http_join_threads);
    }
  }
  for (const auto &buffer_cv : buffer_cvs) {
    buffer_cv->notify_all();
  }
  if (join_thread.joinable()) {
    join_thread.join();
  }
  for (auto &thread : http_join_threads) {
    if (thread.joinable()) {
      thread.join();
    }
  }
}

namespace {
constexpr int64_t kEmitPacketSlowLogMs = 1000;
constexpr int64_t kRateLimitWaitStepMs = 20;
constexpr bool kEnableAnnexBPayloadEscape = true;
constexpr uint64_t kBitsPerByte = 8;
constexpr uint64_t kBitsPerMegabit = 1024 * 1024;
constexpr int64_t kHttpStallLogIntervalMs = 1000;

uint64_t mbpsToBytesPerSec(uint64_t mbps) {
  return (mbps * kBitsPerMegabit) / kBitsPerByte;
}

bool isHttpUrl(const std::string &value) {
  return value.rfind("http://", 0) == 0 || value.rfind("https://", 0) == 0;
}

uint64_t inferUnifiedBurstBytes(size_t chunk_size) {
  return std::max<uint64_t>(chunk_size * 2, 256 * 1024);
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

void writeU16BEAt(std::vector<uint8_t> &out, size_t offset, uint16_t value) {
  out[offset + 0] = static_cast<uint8_t>((value >> 8) & 0xFF);
  out[offset + 1] = static_cast<uint8_t>(value & 0xFF);
}

bool equalsIgnoreCase(std::string lhs, std::string rhs) {
  lhs = toLowerCopy(std::move(lhs));
  rhs = toLowerCopy(std::move(rhs));
  return lhs == rhs;
}

bool needsAnnexBEscape(const uint8_t *data, size_t size) {
  if (!data || size < 3) {
    return false;
  }
  for (size_t i = 2; i < size; ++i) {
    if (data[i - 2] == 0x00 && data[i - 1] == 0x00 && data[i] <= 0x03) {
      return true;
    }
  }
  return false;
}

void escapeAnnexBPayload(const uint8_t *data, size_t size,
                         std::vector<uint8_t> &out) {
  out.clear();
  out.reserve(size + size / 16 + 8);
  int zero_count = 0;
  for (size_t i = 0; i < size; ++i) {
    const auto byte = data[i];
    if (zero_count >= 2 && byte <= 0x03) {
      out.push_back(0x03);
      zero_count = 0;
    }
    out.push_back(byte);
    if (byte == 0x00) {
      ++zero_count;
    } else {
      zero_count = 0;
    }
  }
}

bool maybeEscapeCarrierPacket(std::vector<uint8_t> &packet) {
  size_t prefix_size = 0;
  if (!DetectEsFileCarrierPrefixSize(packet.data(), packet.size(), prefix_size) ||
      packet.size() < prefix_size + kEsFileFixedHeaderSize) {
    return false;
  }
  EsFilePacketHeader header;
  if (!DecodeEsFilePacketHeader(packet.data() + prefix_size,
                                packet.size() - prefix_size, header)) {
    return false;
  }
  const size_t payload_offset = prefix_size + kEsFileFixedHeaderSize +
                                header.task_id_len + header.file_name_len;
  if (packet.size() < payload_offset + header.payload_len) {
    return false;
  }
  auto *payload = packet.data() + payload_offset;
  const auto payload_len = static_cast<size_t>(header.payload_len);
  if (payload_len == 0 || !needsAnnexBEscape(payload, payload_len)) {
    return false;
  }

  std::vector<uint8_t> escaped_payload;
  escapeAnnexBPayload(payload, payload_len, escaped_payload);
  if (escaped_payload.size() == payload_len) {
    return false;
  }
  std::vector<uint8_t> escaped_packet;
  escaped_packet.reserve(payload_offset + escaped_payload.size());
  escaped_packet.insert(escaped_packet.end(), packet.begin(),
                        packet.begin() + static_cast<std::ptrdiff_t>(payload_offset));
  escaped_packet.insert(escaped_packet.end(), escaped_payload.begin(),
                        escaped_payload.end());
  packet.swap(escaped_packet);
  return true;
}

bool encodeFileInfoPayloadBase64(std::vector<uint8_t> &packet) {
  size_t prefix_size = 0;
  if (!DetectEsFileCarrierPrefixSize(packet.data(), packet.size(), prefix_size) ||
      packet.size() < prefix_size + kEsFileFixedHeaderSize) {
    return false;
  }
  EsFilePacketHeader header;
  if (!DecodeEsFilePacketHeader(packet.data() + prefix_size,
                                packet.size() - prefix_size, header) ||
      header.type != EsFilePacketType::FileInfo) {
    return false;
  }
  const size_t payload_offset = prefix_size + kEsFileFixedHeaderSize +
                                header.task_id_len + header.file_name_len;
  if (packet.size() < payload_offset + header.payload_len) {
    return false;
  }
  const auto payload_len = static_cast<size_t>(header.payload_len);
  if (payload_len == 0) {
    return false;
  }
  const auto encoded = encodeBase64(std::string(
      reinterpret_cast<const char *>(packet.data() + payload_offset),
      payload_len));
  std::vector<uint8_t> encoded_payload(encoded.begin(), encoded.end());
  std::vector<uint8_t> rebuilt;
  rebuilt.reserve(payload_offset + encoded_payload.size());
  rebuilt.insert(rebuilt.end(), packet.begin(),
                 packet.begin() + static_cast<std::ptrdiff_t>(payload_offset));
  rebuilt.insert(rebuilt.end(), encoded_payload.begin(), encoded_payload.end());
  const auto new_payload_len = static_cast<uint32_t>(encoded_payload.size());
  const auto new_total_len = static_cast<uint32_t>(
      kEsFileFixedHeaderSize + header.task_id_len + header.file_name_len +
      new_payload_len);
  writeU32BEAt(rebuilt, prefix_size + 24, new_payload_len);
  writeU32BEAt(rebuilt, prefix_size + 40, new_total_len);
  packet.swap(rebuilt);
  return true;
}

bool headerValueContains(const EsFileFerryPacker::HttpHeaders &headers,
                         const std::string &name,
                         const std::string &needle) {
  const auto needle_lower = toLowerCopy(needle);
  for (const auto &header : headers) {
    if (!equalsIgnoreCase(header.first, name)) {
      continue;
    }
    if (toLowerCopy(header.second).find(needle_lower) != std::string::npos) {
      return true;
    }
  }
  return false;
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

// Config And Lifecycle

void EsFileFerryPacker::setChunkSize(size_t chunk_size) {
  std::lock_guard<std::mutex> lock(_mtx);
  _global_options.packet_chunk_bytes =
      chunk_size == 0 ? kDefaultPacketChunkBytes : chunk_size;
}

void EsFileFerryPacker::setSchedulerRoundBudgetBytes(uint64_t budget_bytes) {
  std::lock_guard<std::mutex> lock(_mtx);
  _global_options.scheduler_round_budget_bytes =
      budget_bytes == 0 ? kDefaultSchedulerRoundBudgetBytes : budget_bytes;
  _packet_runtime.packet_sem.post();
}

void EsFileFerryPacker::setMinEmitPayloadBytes(uint64_t min_payload_bytes) {
  std::lock_guard<std::mutex> lock(_mtx);
  _global_options.min_emit_payload_bytes = min_payload_bytes;
  _packet_runtime.packet_sem.post();
}

void EsFileFerryPacker::setMaxHttpFetchConcurrency(size_t max_concurrency) {
  {
    std::lock_guard<std::mutex> lock(_mtx);
    _global_options.http_pull_concurrency_limit =
        max_concurrency == 0 ? kDefaultHttpFetchConcurrency : max_concurrency;
  }
  maybeStartPendingHttpFetches();
  _packet_runtime.packet_sem.post();
}

void EsFileFerryPacker::setMaxTotalHttpBufferedBytes(uint64_t max_buffered_bytes) {
  {
    std::lock_guard<std::mutex> lock(_mtx);
    _global_options.http_pull_total_buffer_limit_bytes =
        max_buffered_bytes == 0 ? kDefaultMaxHttpBufferedBytes
                                : max_buffered_bytes;
  }
  maybeStartPendingHttpFetches();
  _packet_runtime.packet_sem.post();
}

void EsFileFerryPacker::setGlobalOptions(const EsFileGlobalOptions &opts) {
  bool should_try_start_http = false;
  {
    std::lock_guard<std::mutex> lock(_mtx);
    _global_options.packet_chunk_bytes =
        opts.packet_chunk_bytes == 0 ? kDefaultPacketChunkBytes
                                     : opts.packet_chunk_bytes;
    _global_options.scheduler_round_budget_bytes =
        opts.scheduler_round_budget_bytes == 0
            ? kDefaultSchedulerRoundBudgetBytes
            : opts.scheduler_round_budget_bytes;
    _global_options.min_emit_payload_bytes = opts.min_emit_payload_bytes;
    _global_options.http_pull_concurrency_limit =
        opts.http_pull_concurrency_limit == 0 ? kDefaultHttpFetchConcurrency
                                              : opts.http_pull_concurrency_limit;
    _global_options.http_pull_total_buffer_limit_bytes =
        opts.http_pull_total_buffer_limit_bytes == 0
            ? kDefaultMaxHttpBufferedBytes
            : opts.http_pull_total_buffer_limit_bytes;
    _global_options.http_pull_per_task_buffer_limit_bytes =
        opts.http_pull_per_task_buffer_limit_bytes == 0
            ? kDefaultMaxHttpBufferedBytesPerTask
            : opts.http_pull_per_task_buffer_limit_bytes;
    _global_options.http_pull_total_rate_mbps =
        opts.http_pull_total_rate_mbps;
    _global_options.http_pull_fast_start_slot_reserve =
        opts.http_pull_fast_start_slot_reserve;
    _global_options.http_pull_fast_start_protection_ms =
        opts.http_pull_fast_start_protection_ms;
    recomputeAllTaskRateProfilesLocked(false);
    should_try_start_http = !_http_runtime.pending_fetches.empty();
    InfoL << "ferry global options updated"
          << ", packet_chunk_bytes:" << _global_options.packet_chunk_bytes
          << ", scheduler_round_budget_bytes:"
          << _global_options.scheduler_round_budget_bytes
          << ", min_emit_payload_bytes:" << _global_options.min_emit_payload_bytes
          << ", http_pull_concurrency_limit:"
          << _global_options.http_pull_concurrency_limit
          << ", http_pull_total_buffer_limit_bytes:"
          << _global_options.http_pull_total_buffer_limit_bytes
          << ", http_pull_per_task_buffer_limit_bytes:"
          << _global_options.http_pull_per_task_buffer_limit_bytes
          << ", http_pull_total_rate_mbps:"
          << _global_options.http_pull_total_rate_mbps;
  }
  if (should_try_start_http) {
    maybeStartPendingHttpFetches();
  }
  _packet_runtime.packet_sem.post();
}

void EsFileFerryPacker::setPacketCallback(PacketCallback cb) {
  std::lock_guard<std::mutex> lock(_mtx);
  _packet_runtime.callback = std::move(cb);
  if (_packet_runtime.callback) {
    _packet_runtime.ts_started = false;
    _packet_runtime.last_ts_ms = 0;
    _packet_runtime.bootstrap_due = true;
    _packet_runtime.packet_sem.post();
  } else {
    _packet_runtime.bootstrap_due = false;
  }
}

void EsFileFerryPacker::setDownstreamCongested(bool congested) {
  std::vector<std::shared_ptr<std::condition_variable>> cvs;
  bool should_try_start_http = false;
  {
    std::lock_guard<std::mutex> lock(_mtx);
    if (_packet_runtime.downstream_congested == congested) {
      return;
    }
    _packet_runtime.downstream_congested = congested;
    maybeUpdateAllHttpFetchControlsLocked();
    should_try_start_http = !congested && !_http_runtime.pending_fetches.empty();
    for (auto &item : _task_registry.tasks) {
      auto &cv = item.second.http.buffer.cv;
      if (cv) {
        cvs.emplace_back(cv);
      }
    }
  }
  for (const auto &cv : cvs) {
    cv->notify_all();
  }
  _packet_runtime.packet_sem.post();
  if (should_try_start_http) {
    maybeStartPendingHttpFetches();
  }
}

void EsFileFerryPacker::maybeUpdateHttpFetchControlLocked(TaskState &task) {
  if (!_http_fetch_engine || !task.http.source || !task.http.active) {
    task.http.fetch_paused = false;
    return;
  }

  const bool over_task_pause_threshold =
      task.http.buffer.buffered_bytes >= task.control.max_buffered_bytes;
  const bool over_task_resume_threshold =
      task.http.buffer.buffered_bytes > task.control.resume_buffered_bytes;
  const bool over_total_limit =
      _http_runtime.total_buffered_bytes >=
      _global_options.http_pull_total_buffer_limit_bytes;
  const bool should_pause = task.http.fetch_paused
                                ? (_packet_runtime.downstream_congested ||
                                   over_task_resume_threshold ||
                                   over_total_limit)
                                : (_packet_runtime.downstream_congested ||
                                   over_task_pause_threshold ||
                                   over_total_limit);
  if (should_pause == task.http.fetch_paused) {
    return;
  }

  bool updated = false;
  if (should_pause) {
    updated = _http_fetch_engine->pauseTask(task.task_id, task.generation);
  } else {
    updated = _http_fetch_engine->resumeTask(task.task_id, task.generation);
  }
  if (updated) {
    task.http.fetch_paused = should_pause;
    InfoL << "http fetch control updated, task_id:" << task.task_id
          << " generation:" << task.generation
          << " action:" << (should_pause ? "pause" : "resume")
          << " task_buffered_bytes:" << task.http.buffer.buffered_bytes
          << " total_buffered_bytes:" << _http_runtime.total_buffered_bytes
          << " max_buffered_bytes:" << task.control.max_buffered_bytes
          << " resume_buffered_bytes:" << task.control.resume_buffered_bytes;
  } else {
    WarnL << "http fetch control skipped, task_id:" << task.task_id
          << " generation:" << task.generation
          << " action:" << (should_pause ? "pause" : "resume")
          << " task_active:" << task.http.active
          << " task_buffered_bytes:" << task.http.buffer.buffered_bytes
          << " total_buffered_bytes:" << _http_runtime.total_buffered_bytes;
  }
}

void EsFileFerryPacker::maybeUpdateAllHttpFetchControlsLocked() {
  for (auto &entry : _task_registry.tasks) {
    maybeUpdateHttpFetchControlLocked(entry.second);
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
    state.source.file_path = source;
    state.source.file_name =
        file_name.empty() ? inferHttpFileName(source, task_id) : file_name;
    state.source.file_size = 0;
    state.http.source = true;
    state.http.queued = true;
    state.http.active = false;
    state.http.failed = false;
    state.http.headers_ready = false;
    state.http.size_known = false;
    state.http.received_bytes = 0;
    state.http.fast_start_candidate = true;
    state.http.fast_start_granted = false;
    state.http.first_chunk_emitted = false;
    state.http.queued_since = std::chrono::steady_clock::now();
    state.http.method = method_upper;
    state.http.request_headers = headers;
    state.http.request_body = body;
    state.http.buffer.cv = std::make_shared<std::condition_variable>();
    uint64_t generation = 0;
    {
      std::lock_guard<std::mutex> lock(_mtx);
      refreshUnifiedTaskProfileLocked(state, true);
      generation = ++_task_registry.generation;
      state.generation = generation;
      InfoL << "http ferry task profile, task_id:" << task_id
            << " generation:" << generation
            << " max_buffered_bytes:" << state.control.max_buffered_bytes
            << " resume_buffered_bytes:" << state.control.resume_buffered_bytes
            << " global_per_task_limit:"
            << _global_options.http_pull_per_task_buffer_limit_bytes
            << " packet_chunk_bytes:" << _global_options.packet_chunk_bytes;
      _task_registry.tasks[task_id] = std::move(state);
      recomputeAllTaskRateProfilesLocked(false);
      _http_runtime.pending_fetches.emplace_back(task_id, generation);
    }
    maybeStartPendingHttpFetches();
    _packet_runtime.packet_sem.post();
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
    state.source.file_path = source;
    state.source.file_name = pickFileName(source, file_name);
    state.source.file_size = file_size;
    state.source.memory_mode = false;
    state.source.stream = std::move(stream);
    std::lock_guard<std::mutex> lock(_mtx);
    refreshUnifiedTaskProfileLocked(state, true);
    state.generation = ++_task_registry.generation;
    _task_registry.tasks[task_id] = std::move(state);
    recomputeAllTaskRateProfilesLocked(false);
    _packet_runtime.packet_sem.post();
    return true;
  }

}

void EsFileFerryPacker::removeTask(const std::string &task_id) {
  std::shared_ptr<std::condition_variable> buffer_cv;
  bool task_found = false;
  bool info_sent = false;
  bool end_sent = false;
  bool http_pending = false;
  uint64_t task_generation = 0;
  uint64_t sent_bytes = 0;
  uint64_t file_size = 0;
  bool should_schedule_http = false;
  {
    std::lock_guard<std::mutex> lock(_mtx);
    auto it = _task_registry.tasks.find(task_id);
    if (it != _task_registry.tasks.end()) {
      task_found = true;
      buffer_cv = it->second.http.buffer.cv;
      info_sent = it->second.send.info_sent;
      end_sent = it->second.send.end_sent;
      http_pending = it->second.http.queued || it->second.http.active;
      task_generation = it->second.generation;
      sent_bytes = it->second.send.sent_bytes;
      file_size = it->second.source.file_size;
      erasePendingHttpFetchLocked(task_id, it->second.generation);
      clearTaskHttpBufferLocked(it->second);
    }
    _task_registry.tasks.erase(task_id);
    recomputeAllTaskRateProfilesLocked(false);
    should_schedule_http = !_http_runtime.pending_fetches.empty();
  }
  InfoL << "remove ferry task, task_id:" << task_id
        << " found:" << task_found
        << " info_sent:" << info_sent
        << " end_sent:" << end_sent
        << " http_pending:" << http_pending
        << " sent_bytes:" << sent_bytes
        << " file_size:" << file_size;
  if (_http_fetch_engine && task_generation != 0) {
    _http_fetch_engine->cancelTask(task_id, task_generation);
  }
  if (buffer_cv) {
    buffer_cv->notify_all();
  }
  if (should_schedule_http) {
    maybeStartPendingHttpFetches();
  }
  _packet_runtime.packet_sem.post();
}

void EsFileFerryPacker::clearTasks() {
  std::vector<std::shared_ptr<std::condition_variable>> buffer_cvs;
  std::vector<std::thread> http_join_threads;
  bool should_schedule_http = false;
  {
    std::lock_guard<std::mutex> lock(_mtx);
    buffer_cvs.reserve(_task_registry.tasks.size());
    for (const auto &it : _task_registry.tasks) {
      if (it.second.http.buffer.cv) {
        buffer_cvs.emplace_back(it.second.http.buffer.cv);
      }
    }
    _http_runtime.pending_fetches.clear();
    _http_runtime.total_buffered_bytes = 0;
    _task_registry.tasks.clear();
    _task_registry.rr_cursor = 0;
    recomputeAllTaskRateProfilesLocked(false);
    should_schedule_http = !_http_runtime.pending_fetches.empty();
  }
  for (const auto &buffer_cv : buffer_cvs) {
    buffer_cv->notify_all();
  }
  for (auto &thread : http_join_threads) {
    if (thread.joinable()) {
      thread.join();
    }
  }
  if (should_schedule_http) {
    maybeStartPendingHttpFetches();
  }
  _packet_runtime.packet_sem.post();
}

// Packet Runtime
bool EsFileFerryPacker::onBootstrapTimer() {
  std::lock_guard<std::mutex> lock(_mtx);
  if (_packet_runtime.packet_thread_exit) {
    return false;
  }
  if (_packet_runtime.callback) {
    _packet_runtime.bootstrap_due = true;
    _packet_runtime.packet_sem.post();
  }
  return true;
}

bool EsFileFerryPacker::onPaceTimer() {
  std::lock_guard<std::mutex> lock(_mtx);
  if (_packet_runtime.packet_thread_exit) {
    return false;
  }
  std::vector<std::string> stalled_http_tasks;
  const auto now = std::chrono::steady_clock::now();
  for (auto &item : _task_registry.tasks) {
    auto &task = item.second;
    if (!task.http.source || !task.http.active) {
      continue;
    }
    refillTokenBucket(task.control.fetch_bucket);
    refillTokenBucket(task.control.emit_bucket);
    if (task.http.headers_ready && !task.http.first_chunk_emitted &&
        !task.send.end_sent && !task.http.failed) {
      stalled_http_tasks.emplace_back(static_cast<std::string>(
          StrPrinter << task.task_id
                     << "{recv:" << task.http.received_bytes
                     << ",buf:" << task.http.buffer.buffered_bytes
                     << ",max_buf:" << task.control.max_buffered_bytes
                     << ",resume_buf:" << task.control.resume_buffered_bytes
                     << ",chunks:" << task.http.buffer.chunks.size()
                     << ",info_sent:" << task.send.info_sent
                     << ",emit_tokens:" << peekTokenBucketBytes(task.control.emit_bucket)
                     << ",min_emit:" << _global_options.min_emit_payload_bytes
                     << ",paused:" << task.http.fetch_paused
                     << ",granted:" << task.http.fast_start_granted
                     << "}"));
      if (stalled_http_tasks.size() >= 8) {
        break;
      }
    }
  }
  if (!stalled_http_tasks.empty() &&
      (_packet_runtime.last_http_stall_log_time ==
           std::chrono::steady_clock::time_point{} ||
       std::chrono::duration_cast<std::chrono::milliseconds>(
           now - _packet_runtime.last_http_stall_log_time)
               .count() >= kHttpStallLogIntervalMs)) {
    _packet_runtime.last_http_stall_log_time = now;
    std::string samples_text;
    for (size_t i = 0; i < stalled_http_tasks.size(); ++i) {
      if (i != 0) {
        samples_text += " | ";
      }
      samples_text += stalled_http_tasks[i];
    }
    InfoL << "http stalled before first chunk, active_fetches:"
          << _http_runtime.active_fetches
          << " pending_fetches:" << _http_runtime.pending_fetches.size()
          << " total_buffered_bytes:" << _http_runtime.total_buffered_bytes
          << " samples:" << samples_text;
  }
  maybeUpdateAllHttpFetchControlsLocked();
  if (!_task_registry.tasks.empty() && _packet_runtime.callback) {
    _packet_runtime.packet_sem.post();
  }
  return true;
}

void EsFileFerryPacker::startBootstrapTimerLocked() {
  if (_packet_runtime.bootstrap_timer) {
    return;
  }
  _packet_runtime.bootstrap_timer = std::make_shared<toolkit::Timer>(
      static_cast<float>(kDefaultBootstrapIntervalMs) / 1000.0f,
      [this]() { return onBootstrapTimer(); }, nullptr);
}

void EsFileFerryPacker::stopBootstrapTimerLocked() { _packet_runtime.bootstrap_timer.reset(); }

void EsFileFerryPacker::startPaceTimerLocked() {
  if (_packet_runtime.pace_timer) {
    return;
  }
  _packet_runtime.pace_timer = std::make_shared<toolkit::Timer>(
      static_cast<float>(kDefaultPaceIntervalMs) / 1000.0f,
      [this]() { return onPaceTimer(); }, nullptr);
}

void EsFileFerryPacker::stopPaceTimerLocked() { _packet_runtime.pace_timer.reset(); }

void EsFileFerryPacker::startPacketThreadLocked() {
  if (_packet_runtime.packet_thread_running) {
    return;
  }
  _packet_runtime.packet_thread_exit = false;
  _packet_runtime.packet_thread_running = true;
  _packet_runtime.packet_thread = std::thread([this]() { packetThreadLoop(); });
}

void EsFileFerryPacker::stopPacketThreadLocked(std::thread &join_thread) {
  if (!_packet_runtime.packet_thread_running) {
    return;
  }
  _packet_runtime.packet_thread_exit = true;
  _packet_runtime.packet_sem.post();
  if (_packet_runtime.packet_thread.joinable()) {
    join_thread = std::move(_packet_runtime.packet_thread);
  }
  _packet_runtime.packet_thread_running = false;
  _packet_runtime.bootstrap_due = false;
}

void EsFileFerryPacker::packetThreadLoop() {
  auto reap_http_fetch_threads = [this]() {
    std::vector<std::thread> http_join_threads;
    if (_http_fetch_engine) {
      _http_fetch_engine->reapCompleted(http_join_threads);
    }
    for (auto &thread : http_join_threads) {
      if (thread.joinable()) {
        thread.join();
      }
    }
  };
  while (true) {
    _packet_runtime.packet_sem.wait();
    reap_http_fetch_threads();
    while (true) {
      PacketCallback bootstrap_cb;
      bool should_exit = false;
      bool should_emit_bootstrap = false;
      uint64_t round_payload_budget_bytes = 0;
      {
        std::lock_guard<std::mutex> lock(_mtx);
        should_exit = _packet_runtime.packet_thread_exit;
        if (should_exit) {
          break;
        }
        should_emit_bootstrap = _packet_runtime.bootstrap_due;
        if (should_emit_bootstrap) {
          _packet_runtime.bootstrap_due = false;
          bootstrap_cb = _packet_runtime.callback;
        }
        round_payload_budget_bytes = _global_options.scheduler_round_budget_bytes;
      }
      if (should_emit_bootstrap && bootstrap_cb) {
        emitBootstrapPackets(bootstrap_cb);
      }
      const auto packet_count = processTickPackets(round_payload_budget_bytes);
      if (!should_emit_bootstrap && packet_count == 0) {
        break;
      }
    }
    reap_http_fetch_threads();
    bool should_exit = false;
    {
      std::lock_guard<std::mutex> lock(_mtx);
      should_exit = _packet_runtime.packet_thread_exit;
    }
    if (should_exit) {
      break;
    }
  }
}

size_t EsFileFerryPacker::processTickPackets(uint64_t total_payload_quota_bytes) {
  size_t packet_count = 0;
  bool downstream_congested = false;
  auto collect_ids = [&](const std::function<bool(const TaskState &)> &pred) {
    std::vector<std::string> ids;
    {
      std::lock_guard<std::mutex> lock(_mtx);
      for (const auto &item : _task_registry.tasks) {
        if (pred(item.second)) {
          ids.emplace_back(item.first);
        }
      }
    }
    return pickFairRound(ids);
  };

  {
    std::lock_guard<std::mutex> lock(_mtx);
    recomputeAllTaskRateProfilesLocked(false);
    downstream_congested = _packet_runtime.downstream_congested;
  }

  // Control plane first: failed/info/end packets bypass the data fair round.
  auto failed_ids = collect_ids([](const TaskState &task) {
    return !task.send.end_sent && task.http.failed;
  });
  if (!failed_ids.empty()) {
    bool dirty = false;
    for (const auto &task_id : failed_ids) {
      std::string file_name;
      uint64_t file_size = 0;
      uint32_t next_seq = 0;
      std::string http_error;
      {
        std::lock_guard<std::mutex> lock(_mtx);
        auto it = _task_registry.tasks.find(task_id);
        if (it == _task_registry.tasks.end() || it->second.send.end_sent ||
            !it->second.http.failed) {
          continue;
        }
        file_name = it->second.source.file_name;
        file_size = it->second.source.file_size;
        next_seq = it->second.send.next_seq;
        http_error = it->second.http.error;
      }
      std::vector<uint8_t> status_payload(http_error.begin(), http_error.end());
      const auto status_ts = nextRelativeTimestampMs();
      TaskState status_task;
      status_task.task_id = task_id;
      status_task.source.file_name = file_name;
      status_task.source.file_size = file_size;
      auto status_header =
          makePacketHeader(status_task, EsFilePacketType::TaskStatus, 0,
                           static_cast<uint32_t>(status_payload.size()), 0,
                           next_seq, status_ts);
      auto status_packet = buildPacket(status_task, status_header, status_payload);
      if (!emitPacket(task_id, std::move(status_packet), status_header)) {
        continue;
      }
      ++packet_count;
      std::lock_guard<std::mutex> lock(_mtx);
      auto it = _task_registry.tasks.find(task_id);
      if (it != _task_registry.tasks.end() && !it->second.send.end_sent &&
          it->second.http.failed) {
        it->second.send.info_sent = true;
        it->second.send.end_sent = true;
        it->second.send.next_seq++;
        dirty = true;
      }
    }
    if (dirty) {
      std::lock_guard<std::mutex> lock(_mtx);
      recomputeAllTaskRateProfilesLocked(false);
      maybeUpdateAllHttpFetchControlsLocked();
    }
  }

  auto info_ids = collect_ids([](const TaskState &task) {
    if (task.send.end_sent || task.http.failed || task.send.info_sent) {
      return false;
    }
    return !task.http.source || task.http.headers_ready;
  });
  if (!info_ids.empty()) {
    bool dirty = false;
    for (const auto &task_id : info_ids) {
      std::string file_name;
      uint64_t file_size = 0;
      uint32_t next_seq = 0;
      std::vector<uint8_t> http_meta_payload;
      {
        std::lock_guard<std::mutex> lock(_mtx);
        auto it = _task_registry.tasks.find(task_id);
        if (it == _task_registry.tasks.end() || it->second.send.end_sent ||
            it->second.send.info_sent || it->second.http.failed ||
            (it->second.http.source && !it->second.http.headers_ready)) {
          continue;
        }
        file_name = it->second.source.file_name;
        file_size = it->second.source.file_size;
        next_seq = it->second.send.next_seq;
        if (it->second.http.source) {
          http_meta_payload = it->second.http.response_meta_payload;
        }
      }
      uint16_t info_flags = 0;
      if (!http_meta_payload.empty()) {
        info_flags = kEsFileFlagFileInfoHasHttpResponseHeaders;
      }
      const auto info_ts = nextRelativeTimestampMs();
      TaskState info_task;
      info_task.task_id = task_id;
      info_task.source.file_name = file_name;
      info_task.source.file_size = file_size;
      auto info_header = makePacketHeader(
          info_task, EsFilePacketType::FileInfo, 0,
          static_cast<uint32_t>(http_meta_payload.size()), info_flags, next_seq,
          info_ts);
      auto info_packet = buildPacket(info_task, info_header, http_meta_payload);
      if (!emitPacket(task_id, std::move(info_packet), info_header)) {
        continue;
      }
      ++packet_count;
      std::lock_guard<std::mutex> lock(_mtx);
      auto it = _task_registry.tasks.find(task_id);
      if (it != _task_registry.tasks.end() && !it->second.send.end_sent &&
          !it->second.send.info_sent) {
        it->second.send.info_sent = true;
        it->second.send.next_seq++;
        dirty = true;
      }
    }
    if (dirty) {
      std::lock_guard<std::mutex> lock(_mtx);
      recomputeAllTaskRateProfilesLocked(false);
      maybeUpdateAllHttpFetchControlsLocked();
    }
  }

  // Data plane: all schedulable tasks enter one equal-weight fair round.
  auto round_ids =
      downstream_congested ? std::vector<std::string>{}
                           : snapshotSchedulableTaskIds();
  if (!round_ids.empty() && total_payload_quota_bytes > 0) {
    round_ids = pickFairRound(round_ids);
    {
      std::unordered_set<std::string> fast_start_ids;
      std::unordered_set<std::string> pressure_ids;
      std::unordered_set<std::string> ready_ids;
      {
        std::lock_guard<std::mutex> lock(_mtx);
        for (const auto &task_id : round_ids) {
          auto it = _task_registry.tasks.find(task_id);
          if (it == _task_registry.tasks.end()) {
            continue;
          }
          const auto &task = it->second;
          bool data_ready = false;
          if (task.http.source) {
            data_ready = task.http.buffer.buffered_bytes > 0 &&
                         !task.http.buffer.chunks.empty();
          } else if (task.source.memory_mode) {
            data_ready = task.send.sent_bytes < task.source.memory_payload.size();
          } else {
            data_ready = task.send.sent_bytes < task.source.file_size;
          }
          if (data_ready) {
            ready_ids.emplace(task_id);
          }
          if (task.http.source && task.http.fast_start_granted &&
              !task.http.first_chunk_emitted && data_ready) {
            fast_start_ids.emplace(task_id);
          }
          if (!task.http.source || !task.http.fetch_paused) {
            continue;
          }
          if (task.http.buffer.buffered_bytes <= task.control.resume_buffered_bytes) {
            continue;
          }
          pressure_ids.emplace(task_id);
        }
      }
      if (!ready_ids.empty()) {
        round_ids.erase(
            std::remove_if(
                round_ids.begin(), round_ids.end(),
                [&ready_ids](const std::string &task_id) {
                  return ready_ids.find(task_id) == ready_ids.end();
                }),
            round_ids.end());
      }
      if (!fast_start_ids.empty()) {
        auto first_non_fast_start = std::stable_partition(
            round_ids.begin(), round_ids.end(),
            [&fast_start_ids](const std::string &task_id) {
              return fast_start_ids.find(task_id) != fast_start_ids.end();
            });
        if (!pressure_ids.empty()) {
          std::stable_partition(
              first_non_fast_start, round_ids.end(),
              [&pressure_ids](const std::string &task_id) {
                return pressure_ids.find(task_id) != pressure_ids.end();
              });
        }
      } else if (!pressure_ids.empty()) {
        std::stable_partition(
            round_ids.begin(), round_ids.end(),
            [&pressure_ids](const std::string &task_id) {
              return pressure_ids.find(task_id) != pressure_ids.end();
            });
      }
    }
    const auto active_count = round_ids.size();
    for (size_t i = 0; i < active_count; ++i) {
      const auto &task_id = round_ids[i];
      uint64_t quota = computeTaskQuota(total_payload_quota_bytes, active_count, i);
      while (quota > 0) {
        uint64_t offset = 0;
        uint32_t seq = 0;
        bool no_data = false;
        bool memory_mode = false;
        bool http_chunk_mode = false;
        uint64_t generation = 0;
        size_t read_len = 0;
        uint64_t emit_token_quota = 0;
        uint64_t min_emit_payload_bytes = 0;
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
          auto it = _task_registry.tasks.find(task_id);
          if (it == _task_registry.tasks.end() || it->second.send.end_sent ||
              !isTaskDataSchedulable(it->second)) {
            break;
          }
          auto &task = it->second;
          memory_mode = task.source.memory_mode;
          http_chunk_mode = task.http.source;
          generation = task.generation;
          offset = task.send.sent_bytes;
          seq = task.send.next_seq;
          if (task.http.source && task.http.fetch_paused &&
              task.http.buffer.buffered_bytes > task.control.resume_buffered_bytes) {
            quota = std::max<uint64_t>(
                quota, task.http.buffer.buffered_bytes - task.control.resume_buffered_bytes);
          }
          refillTokenBucket(task.control.emit_bucket);
          emit_token_quota = peekTokenBucketBytes(task.control.emit_bucket);
          min_emit_payload_bytes = _global_options.min_emit_payload_bytes;
          if (emit_token_quota == 0) {
            break;
          }
	          if (task.http.source) {
	            if (task.http.buffer.buffered_bytes == 0 ||
	                task.http.buffer.chunks.empty()) {
	              no_data = true;
	            } else {
	              const auto buffered = task.http.buffer.buffered_bytes;
	              const bool allow_first_emit = !task.http.first_chunk_emitted;
	              const bool http_stream_finished = !task.http.queued && !task.http.active;
	              const bool allow_tail_emit =
	                  http_stream_finished &&
	                  min_emit_payload_bytes > 0 &&
	                  buffered < min_emit_payload_bytes;
	              if (min_emit_payload_bytes > 0 &&
	                  emit_token_quota < min_emit_payload_bytes &&
	                  !allow_tail_emit &&
	                  !allow_first_emit) {
	                break;
	              }
	              if (min_emit_payload_bytes > 0 &&
	                  buffered < min_emit_payload_bytes &&
	                  !allow_tail_emit &&
	                  !allow_first_emit) {
	                break;
	              }
	              if (allow_tail_emit) {
	                read_len = static_cast<size_t>(std::min<uint64_t>(
	                    _global_options.packet_chunk_bytes, buffered));
	              } else {
                read_len = static_cast<size_t>(std::min<uint64_t>(
                    std::min<uint64_t>(
                        std::min<uint64_t>(quota, _global_options.packet_chunk_bytes),
                        emit_token_quota),
                    buffered));
              }
              if (read_len == 0) {
                no_data = true;
              } else {
                packet_task.task_id = task.task_id;
                packet_task.source.file_name = task.source.file_name;
                packet_task.source.file_size = task.source.file_size;
                packet_header = makePacketHeader(
                    packet_task, EsFilePacketType::FileChunk, offset,
                    static_cast<uint32_t>(read_len), 0, seq, packet_ts);
                packet = buildPacket(packet_task, packet_header, read_len,
                                     &payload_offset);
                size_t copied = 0;
                while (copied < read_len && !task.http.buffer.chunks.empty()) {
                  auto &front_chunk = task.http.buffer.chunks.front();
                  const auto available =
                      front_chunk->size - task.http.buffer.front_chunk_offset;
                  const auto need = read_len - copied;
                  const auto step = std::min(available, need);
                  if (step == 0) {
                    break;
                  }
                  std::memcpy(packet.data() + payload_offset + copied,
                              front_chunk->data.data() +
                                  task.http.buffer.front_chunk_offset,
                              step);
                  copied += step;
                  task.http.buffer.front_chunk_offset += step;
                  task.http.buffer.buffered_bytes -= step;
                  _http_runtime.total_buffered_bytes -= step;
                  if (task.http.buffer.front_chunk_offset >= front_chunk->size) {
                    task.http.buffer.chunks.pop_front();
                    task.http.buffer.front_chunk_offset = 0;
                  }
                }
                if (copied == 0) {
                  no_data = true;
                } else {
                  if (copied < read_len) {
                    read_len = copied;
                    packet_header.payload_len = static_cast<uint32_t>(copied);
                    packet_header.total_len = static_cast<uint32_t>(
                        kEsFileFixedHeaderSize + packet_header.task_id_len +
                        packet_header.file_name_len + packet_header.payload_len);
                    const auto payload_len_pos =
                        static_cast<size_t>(kEsFileCarrierPrefixSize + 24);
                    const auto total_len_pos =
                        static_cast<size_t>(kEsFileCarrierPrefixSize + 40);
                    writeU32BEAt(packet, payload_len_pos, packet_header.payload_len);
                    writeU32BEAt(packet, total_len_pos, packet_header.total_len);
                    packet.resize(payload_offset + copied);
                  }
                  maybeUpdateAllHttpFetchControlsLocked();
                  consumeTokenBucketBytes(task.control.emit_bucket, read_len);
                  task.send.sent_bytes += read_len;
                  task.send.next_seq++;
                  quota = read_len >= quota ? 0 : quota - read_len;
                  buffer_cv = task.http.buffer.cv;
                  can_emit_packet = true;
                  should_try_start_http =
                      !_http_runtime.pending_fetches.empty() &&
                      _http_runtime.active_fetches <
                          _global_options.http_pull_concurrency_limit &&
                      _http_runtime.total_buffered_bytes <
                          _global_options.http_pull_total_buffer_limit_bytes;
                }
              }
            }
          } else {
            if (task.send.sent_bytes >= task.source.file_size) {
              no_data = true;
            } else {
              auto remain = task.source.file_size - task.send.sent_bytes;
              const bool allow_tail_emit =
                  min_emit_payload_bytes > 0 && remain < min_emit_payload_bytes;
              if (min_emit_payload_bytes > 0 &&
                  emit_token_quota < min_emit_payload_bytes &&
                  !allow_tail_emit) {
                break;
              }
              if (allow_tail_emit) {
                read_len = static_cast<size_t>(std::min<uint64_t>(
                    _global_options.packet_chunk_bytes, remain));
              } else {
                read_len = static_cast<size_t>(std::min<uint64_t>(
                    std::min<uint64_t>(
                        std::min<uint64_t>(quota, _global_options.packet_chunk_bytes),
                        emit_token_quota),
                    remain));
              }
              if (task.source.memory_mode) {
                if (task.send.sent_bytes < task.source.memory_payload.size()) {
                  const auto available =
                      task.source.memory_payload.size() -
                      static_cast<size_t>(task.send.sent_bytes);
                  read_len = std::min(read_len, available);
                  if (read_len == 0) {
                    no_data = true;
                  } else {
                    packet_task.task_id = task.task_id;
                    packet_task.source.file_name = task.source.file_name;
                    packet_task.source.file_size = task.source.file_size;
                    packet_header = makePacketHeader(
                        packet_task, EsFilePacketType::FileChunk, offset,
                        static_cast<uint32_t>(read_len), 0, seq, packet_ts);
                    packet = buildPacket(packet_task, packet_header, read_len,
                                         &payload_offset);
                    std::memcpy(packet.data() + payload_offset,
                                task.source.memory_payload.data() +
                                    task.send.sent_bytes,
                                read_len);
                    consumeTokenBucketBytes(task.control.emit_bucket, read_len);
                    task.send.sent_bytes += read_len;
                    task.send.next_seq++;
                    quota = read_len >= quota ? 0 : quota - read_len;
                    can_emit_packet = true;
                  }
                } else {
                  no_data = true;
                }
              } else {
                file_stream = task.source.stream;
              }
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
            auto it = _task_registry.tasks.find(task_id);
            if (it == _task_registry.tasks.end()) {
              break;
            }
            packet_task.task_id = it->second.task_id;
            packet_task.source.file_name = it->second.source.file_name;
            packet_task.source.file_size = it->second.source.file_size;
          }
          packet_header = makePacketHeader(
              packet_task, EsFilePacketType::FileChunk, offset,
              static_cast<uint32_t>(read_len), 0, seq, packet_ts);
          packet =
              buildPacket(packet_task, packet_header, read_len, &payload_offset);
          file_stream->read(reinterpret_cast<char *>(
                                packet.data() + static_cast<long>(payload_offset)),
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
            const auto payload_len_pos =
                static_cast<size_t>(kEsFileCarrierPrefixSize + 24);
            const auto total_len_pos =
                static_cast<size_t>(kEsFileCarrierPrefixSize + 40);
            writeU32BEAt(packet, payload_len_pos, packet_header.payload_len);
            writeU32BEAt(packet, total_len_pos, packet_header.total_len);
            packet.resize(payload_offset + read_size);
          }
          std::lock_guard<std::mutex> lock(_mtx);
          auto it = _task_registry.tasks.find(task_id);
          if (it == _task_registry.tasks.end() || it->second.send.end_sent) {
            break;
          }
          auto &task = it->second;
          if (task.generation != generation || task.send.sent_bytes != offset ||
              task.send.next_seq != seq) {
            continue;
          }
          consumeTokenBucketBytes(task.control.emit_bucket, read_size);
          task.send.sent_bytes += read_size;
          task.send.next_seq++;
          quota = read_size >= quota ? 0 : quota - read_size;
        }
        if (emitPacket(task_id, std::move(packet), packet_header)) {
          ++packet_count;
        if (http_chunk_mode) {
          markHttpFirstChunkEmitted(task_id, generation);
        }
        }
      }

    }
  }

  auto end_ids = collect_ids([](const TaskState &task) {
    return isTaskReadyToEmitEnd(task);
  });
  if (!end_ids.empty()) {
    bool dirty = false;
    for (const auto &task_id : end_ids) {
      uint64_t end_offset = 0;
      uint32_t end_seq = 0;
      TaskState end_task;
      {
        std::lock_guard<std::mutex> lock(_mtx);
        auto it = _task_registry.tasks.find(task_id);
        if (it == _task_registry.tasks.end() || !isTaskReadyToEmitEnd(it->second)) {
          continue;
        }
        end_offset = it->second.send.sent_bytes;
        end_seq = it->second.send.next_seq;
        end_task.task_id = it->second.task_id;
        end_task.source.file_name = it->second.source.file_name;
        end_task.source.file_size = it->second.source.file_size;
        it->second.send.next_seq++;
        it->second.send.end_sent = true;
        it->second.source.stream.reset();
        clearTaskHttpBufferLocked(it->second);
        it->second.source.memory_payload.clear();
        it->second.source.memory_payload.shrink_to_fit();
        dirty = true;
      }
      const auto end_ts = nextRelativeTimestampMs();
      auto end_header =
          makePacketHeader(end_task, EsFilePacketType::FileEnd, end_offset, 0,
                           0, end_seq, end_ts);
      auto end_packet = buildPacket(end_task, end_header, {});
      if (emitPacket(task_id, std::move(end_packet), end_header)) {
        ++packet_count;
      }
    }
    if (dirty) {
      std::lock_guard<std::mutex> lock(_mtx);
      recomputeAllTaskRateProfilesLocked(false);
    }
  }
  return packet_count;
}

// HTTP Fetch Runtime

void EsFileFerryPacker::clearTaskHttpBufferLocked(TaskState &task) {
  if (task.http.buffer.buffered_bytes > 0) {
    _http_runtime.total_buffered_bytes -= task.http.buffer.buffered_bytes;
  }
  task.http.buffer.chunks.clear();
  task.http.buffer.buffered_bytes = 0;
  task.http.buffer.front_chunk_offset = 0;
}

void EsFileFerryPacker::erasePendingHttpFetchLocked(const std::string &task_id,
                                                    uint64_t generation) {
  _http_runtime.pending_fetches.erase(
      std::remove_if(_http_runtime.pending_fetches.begin(), _http_runtime.pending_fetches.end(),
                     [&task_id, generation](const std::pair<std::string, uint64_t> &item) {
                       return item.first == task_id && item.second == generation;
                     }),
      _http_runtime.pending_fetches.end());
}

std::vector<std::pair<std::string, uint64_t>>
EsFileFerryPacker::collectHttpFetchLaunchesLocked() {
  std::vector<std::pair<std::string, uint64_t>> launches;
  bool dirty = false;
  if (_packet_runtime.downstream_congested) {
    return launches;
  }
  const auto now = std::chrono::steady_clock::now();
  size_t granted_active_fetches = 0;
  for (auto &item : _task_registry.tasks) {
    auto &task = item.second;
    if (!task.http.source || !task.http.active) {
      continue;
    }
    dirty = refreshFastStartGrantLocked(task, now) || dirty;
    if (task.http.fast_start_granted) {
      ++granted_active_fetches;
    }
  }
  const auto total_limit = _global_options.http_pull_concurrency_limit;
  const auto fast_start_reserve =
      std::min(_global_options.http_pull_fast_start_slot_reserve, total_limit);
  const auto normal_limit =
      total_limit > fast_start_reserve ? total_limit - fast_start_reserve : 0;
  std::deque<std::pair<std::string, uint64_t>> deferred;
  const auto pending_count = _http_runtime.pending_fetches.size();
  for (size_t i = 0; i < pending_count; ++i) {
    if (_http_runtime.pending_fetches.empty()) {
      break;
    }
    if (_http_runtime.total_buffered_bytes >=
        _global_options.http_pull_total_buffer_limit_bytes) {
      break;
    }
    auto item = _http_runtime.pending_fetches.front();
    _http_runtime.pending_fetches.pop_front();
    auto it = _task_registry.tasks.find(item.first);
    if (it == _task_registry.tasks.end() || it->second.generation != item.second) {
      continue;
    }
    auto &task = it->second;
    if (!task.http.source || !task.http.queued || task.http.active) {
      continue;
    }
    const auto normal_active_fetches =
        _http_runtime.active_fetches > granted_active_fetches
            ? _http_runtime.active_fetches - granted_active_fetches
            : 0;
    const bool can_grant_fast_start =
        task.http.fast_start_candidate && granted_active_fetches < fast_start_reserve &&
        _http_runtime.active_fetches < total_limit;
    const bool can_launch_normal =
        _http_runtime.active_fetches < total_limit &&
        (fast_start_reserve == 0 || normal_active_fetches < normal_limit);
    if (!can_grant_fast_start && !can_launch_normal) {
      deferred.emplace_back(item);
      continue;
    }
    task.http.queued = false;
    task.http.active = true;
    task.http.fetch_paused = false;
    task.http.active_since = now;
    task.http.first_chunk_emitted = false;
    task.http.fast_start_granted = can_grant_fast_start;
    if (!can_grant_fast_start) {
      task.http.fast_start_candidate = false;
    } else {
      ++granted_active_fetches;
    }
    ++_http_runtime.active_fetches;
    maybeUpdateHttpFetchControlLocked(task);
    dirty = true;
    InfoL << "http fast-start launch task_id:" << task.task_id
          << " generation:" << task.generation
          << " granted:" << task.http.fast_start_granted
          << " reserve:" << fast_start_reserve
          << " active_fetches:" << _http_runtime.active_fetches
          << " pending_fetches:" << _http_runtime.pending_fetches.size();
    launches.emplace_back(item);
  }
  while (!deferred.empty()) {
    _http_runtime.pending_fetches.emplace_back(std::move(deferred.front()));
    deferred.pop_front();
  }
  if (dirty) {
    recomputeAllTaskRateProfilesLocked(false);
  }
  return launches;
}

bool EsFileFerryPacker::refreshFastStartGrantLocked(
    TaskState &task, std::chrono::steady_clock::time_point now) {
  if (!task.http.fast_start_granted) {
    return false;
  }
  bool should_release = false;
  const char *reason = nullptr;
  if (task.http.first_chunk_emitted) {
    should_release = true;
    reason = "first_chunk_emitted";
  } else if (!_packet_runtime.callback) {
    should_release = true;
    reason = "packet_callback_missing";
  } else if (_global_options.http_pull_fast_start_protection_ms == 0) {
    should_release = true;
    reason = "protection_disabled";
  } else if (task.http.active_since != std::chrono::steady_clock::time_point{} &&
             std::chrono::duration_cast<std::chrono::milliseconds>(
                 now - task.http.active_since)
                     .count() >=
                 _global_options.http_pull_fast_start_protection_ms) {
    should_release = true;
    reason = "protection_timeout";
  }
  if (!should_release) {
    return false;
  }
  task.http.fast_start_granted = false;
  task.http.fast_start_candidate = false;
  InfoL << "http fast-start release task_id:" << task.task_id
        << " generation:" << task.generation
        << " reason:" << (reason ? reason : "unknown")
        << " headers_ready:" << task.http.headers_ready
        << " first_chunk_emitted:" << task.http.first_chunk_emitted
        << " received_bytes:" << task.http.received_bytes;
  return true;
}

void EsFileFerryPacker::markHttpFirstChunkEmitted(const std::string &task_id,
                                                  uint64_t generation) {
  std::lock_guard<std::mutex> lock(_mtx);
  auto it = _task_registry.tasks.find(task_id);
  if (it == _task_registry.tasks.end() || it->second.generation != generation ||
      !it->second.http.source || it->second.http.first_chunk_emitted) {
    return;
  }
  auto &task = it->second;
  task.http.first_chunk_emitted = true;
  const bool changed = refreshFastStartGrantLocked(task, std::chrono::steady_clock::now());
  if (changed) {
    recomputeAllTaskRateProfilesLocked(false);
  }
}

void EsFileFerryPacker::maybeStartPendingHttpFetches() {
  auto launches = [&]() {
    std::lock_guard<std::mutex> lock(_mtx);
    if (_packet_runtime.packet_thread_exit) {
      return std::vector<std::pair<std::string, uint64_t>>{};
    }
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
    auto it = _task_registry.tasks.find(task_id);
    if (_packet_runtime.packet_thread_exit) {
      if (it != _task_registry.tasks.end() && it->second.generation == generation) {
        it->second.http.active = false;
      }
      if (_http_runtime.active_fetches > 0) {
        --_http_runtime.active_fetches;
      }
      recomputeAllTaskRateProfilesLocked(false);
      return;
    }
    if (it == _task_registry.tasks.end() || it->second.generation != generation) {
      if (_http_runtime.active_fetches > 0) {
        --_http_runtime.active_fetches;
      }
      recomputeAllTaskRateProfilesLocked(false);
      return;
    }
    source = it->second.source.file_path;
    method = it->second.http.method;
    headers = it->second.http.request_headers;
    body = it->second.http.request_body;
  }

  HttpFetchEngine::Request request;
  request.task_id = task_id;
  request.generation = generation;
  request.url = source;
  request.method = method;
  request.headers = headers;
  request.body = body;
  const auto fetch_begin = std::chrono::steady_clock::now();
  request.on_chunk = [this, task_id, generation](const uint8_t *data, size_t size) {
    if (!data || size == 0) {
      return HttpFetchEngine::ChunkConsumeResult::Consumed;
    }
    size_t consumed = 0;
    auto logChunkAbort = [&](const char *reason, uint64_t task_generation,
                             size_t task_buffered_bytes,
                             bool has_task_state) {
      WarnL << "http on_chunk abort, task_id:" << task_id
            << " reason:" << reason
            << " fetch_generation:" << generation
            << " task_generation:" << task_generation
            << " consumed_bytes:" << consumed
            << " incoming_chunk_bytes:" << size
            << " task_buffered_bytes:" << task_buffered_bytes
            << " has_task_state:" << has_task_state;
    };
    auto logChunkPause = [&](const char *reason, uint64_t task_generation,
                             size_t task_buffered_bytes,
                             size_t total_buffered_bytes) {
      InfoL << "http on_chunk pause, task_id:" << task_id
            << " reason:" << reason
            << " fetch_generation:" << generation
            << " task_generation:" << task_generation
            << " consumed_bytes:" << consumed
            << " incoming_chunk_bytes:" << size
            << " task_buffered_bytes:" << task_buffered_bytes
            << " total_buffered_bytes:" << total_buffered_bytes;
    };
    while (consumed < size) {
      size_t copy_len = 0;
      {
        std::unique_lock<std::mutex> lock(_mtx);
        while (true) {
          auto it = _task_registry.tasks.find(task_id);
          if (_packet_runtime.packet_thread_exit) {
            logChunkAbort("packet_thread_exit", 0, 0, false);
            return HttpFetchEngine::ChunkConsumeResult::Abort;
          }
          if (it == _task_registry.tasks.end()) {
            logChunkAbort("task_missing", 0, 0, false);
            return HttpFetchEngine::ChunkConsumeResult::Abort;
          }
          if (it->second.generation != generation) {
            logChunkAbort("generation_mismatch", it->second.generation,
                          it->second.http.buffer.buffered_bytes, true);
            return HttpFetchEngine::ChunkConsumeResult::Abort;
          }
          auto &task = it->second;
          auto buffer_cv = it->second.http.buffer.cv;
          if (!buffer_cv) {
            logChunkAbort("null_buffer_cv", task.generation,
                          task.http.buffer.buffered_bytes, true);
            return HttpFetchEngine::ChunkConsumeResult::Abort;
          }
          if (_packet_runtime.downstream_congested) {
            const bool was_paused = task.http.fetch_paused;
            task.http.fetch_paused = true;
            if (!was_paused) {
              logChunkPause("downstream_congested", task.generation,
                            task.http.buffer.buffered_bytes,
                            _http_runtime.total_buffered_bytes);
            }
            return HttpFetchEngine::ChunkConsumeResult::Pause;
          }
          refillTokenBucket(task.control.fetch_bucket);

          if (task.http.buffer.buffered_bytes >=
                  task.control.max_buffered_bytes &&
              task.http.buffer.buffered_bytes >
                  task.control.resume_buffered_bytes) {
            const bool was_paused = task.http.fetch_paused;
            task.http.fetch_paused = true;
            if (!was_paused) {
              logChunkPause("task_buffer_full", task.generation,
                            task.http.buffer.buffered_bytes,
                            _http_runtime.total_buffered_bytes);
            }
            return HttpFetchEngine::ChunkConsumeResult::Pause;
          }
          if (_http_runtime.total_buffered_bytes >=
              _global_options.http_pull_total_buffer_limit_bytes) {
            const bool was_paused = task.http.fetch_paused;
            task.http.fetch_paused = true;
            if (!was_paused) {
              logChunkPause("total_buffer_full", task.generation,
                            task.http.buffer.buffered_bytes,
                            _http_runtime.total_buffered_bytes);
            }
            return HttpFetchEngine::ChunkConsumeResult::Pause;
          }

          const auto task_room = task.control.max_buffered_bytes >
                                         task.http.buffer.buffered_bytes
                                     ? task.control.max_buffered_bytes -
                                           task.http.buffer.buffered_bytes
                                     : 0;
          const auto total_room =
              _global_options.http_pull_total_buffer_limit_bytes >
                      _http_runtime.total_buffered_bytes
                  ? _global_options.http_pull_total_buffer_limit_bytes -
                        _http_runtime.total_buffered_bytes
                  : 0;
          auto fetch_tokens = peekTokenBucketBytes(task.control.fetch_bucket);
          if (task_room == 0 || total_room == 0) {
            const bool was_paused = task.http.fetch_paused;
            task.http.fetch_paused = true;
            if (!was_paused) {
              logChunkPause("buffer_room_exhausted", task.generation,
                            task.http.buffer.buffered_bytes,
                            _http_runtime.total_buffered_bytes);
            }
            return HttpFetchEngine::ChunkConsumeResult::Pause;
          }
          if (fetch_tokens == 0) {
            const bool was_paused = task.http.fetch_paused;
            task.http.fetch_paused = true;
            if (!was_paused) {
              logChunkPause("fetch_rate_limited", task.generation,
                            task.http.buffer.buffered_bytes,
                            _http_runtime.total_buffered_bytes);
            }
            return HttpFetchEngine::ChunkConsumeResult::Pause;
          }

          auto chunk = _http_chunk_pool.obtain([](HttpChunkBuffer *buffer) {
            buffer->size = 0;
          });
          const auto max_copy = std::min<uint64_t>(
              std::min<uint64_t>(task_room, total_room), fetch_tokens);
          copy_len = static_cast<size_t>(std::min<uint64_t>(
              std::min<uint64_t>(size - consumed, chunk->data.size()),
              max_copy));
          if (copy_len == 0) {
            const bool was_paused = task.http.fetch_paused;
            task.http.fetch_paused = true;
            if (!was_paused) {
              logChunkPause("fetch_copy_zero", task.generation,
                            task.http.buffer.buffered_bytes,
                            _http_runtime.total_buffered_bytes);
            }
            return HttpFetchEngine::ChunkConsumeResult::Pause;
          }
          std::memcpy(chunk->data.data(), data + consumed, copy_len);
          chunk->size = copy_len;
          consumeTokenBucketBytes(task.control.fetch_bucket, copy_len);
          task.http.received_bytes += copy_len;
          task.http.buffer.buffered_bytes += copy_len;
          _http_runtime.total_buffered_bytes += copy_len;
          task.http.buffer.chunks.emplace_back(std::move(chunk));
          maybeUpdateAllHttpFetchControlsLocked();
          break;
        }
      }
      consumed += copy_len;
      _packet_runtime.packet_sem.post();
    }
    return HttpFetchEngine::ChunkConsumeResult::Consumed;
  };
  request.on_headers =
      [this, task_id, generation, source](uint32_t status_code,
                                          const HttpHeaders &headers_in) {
          uint64_t content_length = 0;
          const bool has_content_length =
              tryParseContentLength(headers_in, content_length);
          bool emit_info_now = false;
          std::string file_name;
          uint64_t file_size = 0;
          uint32_t next_seq = 0;
          std::vector<uint8_t> http_meta_payload;
          {
            std::lock_guard<std::mutex> lock(_mtx);
            if (_packet_runtime.packet_thread_exit) {
              return;
            }
            auto it = _task_registry.tasks.find(task_id);
            if (it == _task_registry.tasks.end() || it->second.generation != generation) {
              return;
            }
            auto &task = it->second;
            refreshUnifiedTaskProfileLocked(task, false);
            recomputeAllTaskRateProfilesLocked(false);
            task.http.status_code = status_code;
            task.http.response_meta_payload =
                buildHttpResponseMetaPayload(status_code, headers_in);
            task.http.headers_ready = true;
            if (has_content_length) {
              task.source.file_size = content_length;
              task.http.size_known = true;
            }
            if (!task.send.end_sent && !task.send.info_sent && !task.http.failed) {
              emit_info_now = true;
              file_name = task.source.file_name;
              file_size = task.source.file_size;
              next_seq = task.send.next_seq;
              http_meta_payload = task.http.response_meta_payload;
              task.send.info_sent = true;
              task.send.next_seq++;
            }
            recomputeAllTaskRateProfilesLocked(false);
            maybeUpdateHttpFetchControlLocked(task);
            InfoL << "http fetch headers task_id:" << task_id
                  << " generation:" << generation
                  << " status:" << status_code
                  << " has_content_length:" << has_content_length
                  << " content_length:" << content_length
                  << " response_header_count:" << headers_in.size()
                  << " url:" << source;
          }
          if (emit_info_now) {
            uint16_t info_flags = 0;
            if (!http_meta_payload.empty()) {
              info_flags = kEsFileFlagFileInfoHasHttpResponseHeaders;
            }
            const auto info_ts = nextRelativeTimestampMs();
            TaskState info_task;
            info_task.task_id = task_id;
            info_task.source.file_name = file_name;
            info_task.source.file_size = file_size;
            auto info_header = makePacketHeader(
                info_task, EsFilePacketType::FileInfo, 0,
                static_cast<uint32_t>(http_meta_payload.size()), info_flags,
                next_seq, info_ts);
            auto info_packet =
                buildPacket(info_task, info_header, http_meta_payload);
            emitPacket(task_id, std::move(info_packet), info_header);
          }
          _packet_runtime.packet_sem.post();
        };
  request.on_complete =
      [this, task_id, generation, source, method, fetch_begin](
          bool ok, const HttpHeaders &response_headers,
          uint32_t response_status_code, const std::string &fetch_err,
          const HttpStreamFetcher::TransferDiagnostics &diagnostics) {
    const auto fetch_cost_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - fetch_begin)
            .count();
    uint64_t fetched_bytes = 0;
    uint64_t final_file_size = 0;
    bool final_size_known = false;
    size_t final_buffered_bytes = 0;
    bool packet_runtime_stopped = false;
    std::shared_ptr<std::condition_variable> buffer_cv;
    {
      std::lock_guard<std::mutex> lock(_mtx);
      packet_runtime_stopped = _packet_runtime.packet_thread_exit;
      if (_http_runtime.active_fetches > 0) {
        --_http_runtime.active_fetches;
      }
      auto it = _task_registry.tasks.find(task_id);
      if (it != _task_registry.tasks.end() && it->second.generation == generation) {
        auto &task = it->second;
        fetched_bytes = task.http.received_bytes;
        final_file_size = task.source.file_size;
        final_size_known = task.http.size_known;
        final_buffered_bytes = task.http.buffer.buffered_bytes;
        task.http.active = false;
        task.http.fetch_paused = false;
        task.http.fast_start_granted = false;
        task.http.fast_start_candidate = false;
        task.http.status_code = response_status_code;
        task.http.response_meta_payload =
            buildHttpResponseMetaPayload(response_status_code, response_headers);
        task.http.headers_ready = true;
        if (ok && !task.http.size_known) {
          task.source.file_size = task.http.received_bytes;
          task.http.size_known = true;
          final_file_size = task.source.file_size;
          final_size_known = true;
        }
        buffer_cv = task.http.buffer.cv;
        if (!ok) {
          task.http.failed = true;
          task.http.error = fetch_err.empty() ? "http fetch failed" : fetch_err;
          clearTaskHttpBufferLocked(task);
          final_buffered_bytes = task.http.buffer.buffered_bytes;
        } else {
          task.http.failed = false;
          task.http.error.clear();
        }
      }
      recomputeAllTaskRateProfilesLocked(false);
      maybeUpdateAllHttpFetchControlsLocked();
    }
    if (buffer_cv) {
      buffer_cv->notify_all();
    }
    InfoL << "http fetch task_id:" << task_id << " method:" << method
          << " status:" << response_status_code << " ok:" << ok
          << " bytes:" << fetched_bytes << " file_size:" << final_file_size
          << " size_known:" << final_size_known
          << " buffered_bytes:" << final_buffered_bytes
          << " curl_download_bytes:" << diagnostics.download_bytes
          << " curl_content_length_bytes:" << diagnostics.content_length_bytes
          << " curl_speed_download_bps:" << diagnostics.download_speed_bytes_per_sec
          << " headers_emitted:" << diagnostics.headers_emitted
          << " curl_name_lookup_ms:" << diagnostics.name_lookup_ms
          << " curl_connect_ms:" << diagnostics.connect_ms
          << " curl_app_connect_ms:" << diagnostics.app_connect_ms
          << " curl_pretransfer_ms:" << diagnostics.pretransfer_ms
          << " curl_starttransfer_ms:" << diagnostics.starttransfer_ms
          << " curl_total_ms:" << diagnostics.total_ms
          << " fetch_err:" << fetch_err << " cost_ms:" << fetch_cost_ms
          << " url:" << source;
    if (!packet_runtime_stopped) {
      maybeStartPendingHttpFetches();
      _packet_runtime.packet_sem.post();
    }
    if (!packet_runtime_stopped) {
      _packet_runtime.packet_sem.post();
    }
      };
  InfoL << "http fetch start task_id:" << task_id
        << " generation:" << generation
        << " method:" << method
        << " url:" << source;
  if (_http_fetch_engine) {
    _http_fetch_engine->submit(std::move(request));
  }
}

// Query And Packet Helpers
std::vector<EsFilePackTaskInfo> EsFileFerryPacker::getTaskInfos() const {
  std::lock_guard<std::mutex> lock(_mtx);
  std::vector<EsFilePackTaskInfo> infos;
  infos.reserve(_task_registry.tasks.size());
  for (const auto &it : _task_registry.tasks) {
    EsFilePackTaskInfo info;
    info.task_id = it.second.task_id;
    info.file_path = it.second.source.file_path;
    info.file_name = it.second.source.file_name;
    info.file_size = it.second.source.file_size;
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
  return _packet_runtime.last_error;
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

// Packet Construction Helpers

EsFilePacketHeader EsFileFerryPacker::makePacketHeader(
    const TaskState &task, EsFilePacketType type, uint64_t data_offset,
    uint32_t payload_len, uint16_t flags, uint32_t seq,
    uint32_t timestamp_ms) const {
  EsFilePacketHeader header;
  header.magic = kEsFilePacketMagic;
  header.version = kEsFilePacketVersion;
  header.type = type;
  header.task_id_len = static_cast<uint16_t>(task.task_id.size());
  header.file_name_len = static_cast<uint16_t>(task.source.file_name.size());
  header.flags = flags;
  header.seq = seq;
  header.data_offset = data_offset;
  header.payload_len = payload_len;
  header.file_size = task.source.file_size;
  header.crc32 = 0xFFFFFFFF;
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
  if (!_packet_runtime.ts_started) {
    _packet_runtime.ts_started = true;
    _packet_runtime.ts_start_time = now;
    _packet_runtime.last_ts_ms = 0;
    return _packet_runtime.last_ts_ms;
  }
  const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              now - _packet_runtime.ts_start_time)
                              .count();
  const uint64_t elapsed_u64 =
      elapsed_ms < 0 ? 0 : static_cast<uint64_t>(elapsed_ms);
  const uint32_t current = static_cast<uint32_t>(elapsed_u64 & 0xFFFFFFFFu);
  _packet_runtime.last_ts_ms = current;
  return _packet_runtime.last_ts_ms;
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
  header.file_name_len = static_cast<uint16_t>(task.source.file_name.size());
  header.payload_len = static_cast<uint32_t>(payload_len);
  header.total_len = static_cast<uint32_t>(kEsFileFixedHeaderSize + header.task_id_len +
                                           header.file_name_len + header.payload_len);
  header.magic = kEsFilePacketMagic;
  header.version = kEsFilePacketVersion;
  header.file_size = task.source.file_size;
  const auto total_len = header.total_len;

  std::vector<uint8_t> out;
  out.reserve(total_len + kEsFileCarrierPrefixSize);
  AppendEsFileCarrierPrefix(out);
  AppendEsFilePacketHeader(out, header);
  out.insert(out.end(), task.task_id.begin(), task.task_id.end());
  out.insert(out.end(), task.source.file_name.begin(), task.source.file_name.end());
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
    cb = _packet_runtime.callback;
  }
  if (cb) {
    auto header_out = header;
    if (task_id != kBootstrapTaskId &&
        header_out.type == EsFilePacketType::FileInfo &&
        encodeFileInfoPayloadBase64(packet)) {
      header_out.flags = static_cast<uint16_t>(
          header_out.flags | kEsFileFlagFileInfoPayloadBase64);
      size_t prefix_size = 0;
      if (DetectEsFileCarrierPrefixSize(packet.data(), packet.size(),
                                        prefix_size)) {
        EsFilePacketHeader tmp_header;
        if (DecodeEsFilePacketHeader(packet.data() + prefix_size,
                                     packet.size() - prefix_size, tmp_header)) {
          header_out.payload_len = tmp_header.payload_len;
          header_out.total_len = tmp_header.total_len;
        }
      }
    }
    if (task_id != kBootstrapTaskId &&
        kEnableAnnexBPayloadEscape &&
        maybeEscapeCarrierPacket(packet)) {
      header_out.flags =
          static_cast<uint16_t>(header_out.flags | kEsFileFlagPayloadEscaped);
    }
    if (task_id != kBootstrapTaskId) {
      size_t prefix_size = 0;
      if (DetectEsFileCarrierPrefixSize(packet.data(), packet.size(),
                                        prefix_size)) {
        writeU16BEAt(packet, prefix_size + 10, header_out.flags);
      }
    }
    if (header.type == EsFilePacketType::FileInfo ||
        header.type == EsFilePacketType::FileEnd) {
      DebugL << "emit packet callback, task_id:" << task_id
             << " type:" << EsFilePacketTypeToString(header.type)
             << " seq:" << header.seq
             << " offset:" << header.data_offset
             << " payload_len:" << header.payload_len
             << " file_size:" << header.file_size
             << " total_packet_size:" << packet.size();
    }
    const auto emit_begin = std::chrono::steady_clock::now();
    cb(task_id, std::move(packet), header_out);
    const auto emit_cost_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - emit_begin)
            .count();
    if (emit_cost_ms >= kEmitPacketSlowLogMs) {
      WarnL << "emit packet callback slow, task_id:" << task_id
            << " type:" << EsFilePacketTypeToString(header_out.type)
            << " seq:" << header_out.seq
            << " payload_len:" << header_out.payload_len
            << " cost_ms:" << emit_cost_ms;
    }
  }
  return true;
}

// Task Strategy Helpers

void EsFileFerryPacker::setLastError(const std::string &err) {
  std::lock_guard<std::mutex> lock(_mtx);
  _packet_runtime.last_error = err;
}

void EsFileFerryPacker::refreshUnifiedTaskProfileLocked(TaskState &task,
                                                        bool reset_buckets) {
  const auto now = std::chrono::steady_clock::now();
  task.control.max_buffered_bytes =
      _global_options.http_pull_per_task_buffer_limit_bytes == 0
          ? kDefaultUnifiedBufferedBytes
          : _global_options.http_pull_per_task_buffer_limit_bytes;
  const auto chunk_bytes = static_cast<uint64_t>(
      std::max<size_t>(1, _global_options.packet_chunk_bytes));
  // Leave more room before resuming a paused fetch so small per-task buffers
  // don't oscillate between pause/resume after every emitted chunk.
  const auto desired_resume_room = std::min<uint64_t>(
      task.control.max_buffered_bytes > 0 ? task.control.max_buffered_bytes - 1 : 0,
      std::max<uint64_t>(task.control.max_buffered_bytes / 2,
                         chunk_bytes + chunk_bytes / 2));
  task.control.resume_buffered_bytes =
      task.control.max_buffered_bytes > desired_resume_room
          ? task.control.max_buffered_bytes - desired_resume_room
          : 0;

  const auto fetch_burst =
      inferUnifiedBurstBytes(_global_options.packet_chunk_bytes);
  const auto emit_burst =
      inferUnifiedBurstBytes(_global_options.packet_chunk_bytes);
  task.control.fetch_bucket.unlimited = true;
  task.control.fetch_bucket.rate_bps = 0;
  task.control.fetch_bucket.burst_bytes = fetch_burst;
  task.control.emit_bucket.unlimited = true;
  task.control.emit_bucket.rate_bps = 0;
  task.control.emit_bucket.burst_bytes = emit_burst;

  if (reset_buckets ||
      task.control.fetch_bucket.last_refill ==
          std::chrono::steady_clock::time_point{}) {
    task.control.fetch_bucket.tokens = fetch_burst;
    task.control.fetch_bucket.last_refill = now;
  } else {
    task.control.fetch_bucket.tokens =
        std::min(task.control.fetch_bucket.tokens, fetch_burst);
  }

  if (reset_buckets ||
      task.control.emit_bucket.last_refill ==
          std::chrono::steady_clock::time_point{}) {
    task.control.emit_bucket.tokens = emit_burst;
    task.control.emit_bucket.last_refill = now;
  } else {
    task.control.emit_bucket.tokens =
        std::min(task.control.emit_bucket.tokens, emit_burst);
  }
}

void EsFileFerryPacker::recomputeAllTaskRateProfilesLocked(bool reset_buckets) {
  size_t active_http_fetch_count = 0;
  size_t schedulable_task_count = 0;
  // Dynamic denominators:
  // - fetch side shares total rate across currently active HTTP fetches;
  // - emit side shares total rate across current data-schedulable tasks.
  for (auto &item : _task_registry.tasks) {
    auto &task = item.second;
    if (task.send.end_sent) {
      continue;
    }
    if (task.http.source && task.http.active) {
      ++active_http_fetch_count;
    }
    if (isTaskDataSchedulable(task)) {
      ++schedulable_task_count;
    }
  }
  for (auto &item : _task_registry.tasks) {
    refreshTaskRateBucketsLocked(item.second, active_http_fetch_count,
                                 schedulable_task_count, reset_buckets);
  }
}

void EsFileFerryPacker::refreshTaskRateBucketsLocked(
    TaskState &task, size_t active_http_fetch_count, size_t schedulable_task_count,
    bool reset_buckets) {
  const auto now = std::chrono::steady_clock::now();
  const bool prev_fetch_unlimited = task.control.fetch_bucket.unlimited;
  const bool prev_emit_unlimited = task.control.emit_bucket.unlimited;
  const auto prev_fetch_rate_bps = task.control.fetch_bucket.rate_bps;
  const auto prev_emit_rate_bps = task.control.emit_bucket.rate_bps;
  (void)active_http_fetch_count;
  // The public option still keeps its historical name, but the end-to-end
  // throughput budget is enforced on the emit side. Fetch pacing relies on
  // bounded buffers and pause/resume control so we don't double-throttle the
  // same bytes once during HTTP ingress and again during packet emission.
  const uint64_t total_emit_rate_bytes_per_sec =
      mbpsToBytesPerSec(_global_options.http_pull_total_rate_mbps);
  uint64_t emit_rate_bps = 0;
  const bool rate_limit_enabled = total_emit_rate_bytes_per_sec > 0;

  if (!task.send.end_sent && rate_limit_enabled) {
    if (isTaskDataSchedulable(task) && schedulable_task_count > 0) {
      emit_rate_bps =
          total_emit_rate_bytes_per_sec / schedulable_task_count;
    }
  }

  task.control.fetch_bucket.unlimited = true;
  task.control.fetch_bucket.rate_bps = 0;
  task.control.emit_bucket.unlimited =
      !(rate_limit_enabled && !task.send.end_sent);
  task.control.emit_bucket.rate_bps = emit_rate_bps;
  task.control.fetch_bucket.burst_bytes =
      inferUnifiedBurstBytes(_global_options.packet_chunk_bytes);
  task.control.emit_bucket.burst_bytes =
      inferUnifiedBurstBytes(_global_options.packet_chunk_bytes);

  auto refresh_bucket_tokens =
      [&](TokenBucket &bucket, bool prev_unlimited, uint64_t prev_rate_bps) {
        const bool enabled_now = !bucket.unlimited && bucket.rate_bps > 0;
        const bool became_enabled =
            enabled_now && (prev_unlimited || prev_rate_bps == 0);
        if (reset_buckets ||
            bucket.last_refill == std::chrono::steady_clock::time_point{} ||
            became_enabled) {
          bucket.tokens = bucket.unlimited || enabled_now ? bucket.burst_bytes : 0;
          bucket.last_refill = now;
          return;
        }
        if (!bucket.unlimited && bucket.rate_bps == 0) {
          bucket.tokens = 0;
          bucket.last_refill = now;
          return;
        }
        bucket.tokens = std::min(bucket.tokens, bucket.burst_bytes);
      };
  refresh_bucket_tokens(task.control.fetch_bucket, prev_fetch_unlimited,
                        prev_fetch_rate_bps);
  refresh_bucket_tokens(task.control.emit_bucket, prev_emit_unlimited,
                        prev_emit_rate_bps);
}

void EsFileFerryPacker::refillTokenBucket(TokenBucket &bucket) {
  if (bucket.unlimited || bucket.rate_bps == 0) {
    return;
  }
  const auto now = std::chrono::steady_clock::now();
  if (bucket.last_refill == std::chrono::steady_clock::time_point{}) {
    bucket.last_refill = now;
    bucket.tokens = bucket.burst_bytes;
    return;
  }
  const auto elapsed_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(now -
                                                            bucket.last_refill)
          .count();
  if (elapsed_ms <= 0) {
    return;
  }
  const auto refill_bytes =
      static_cast<uint64_t>((bucket.rate_bps * elapsed_ms) / 1000);
  bucket.tokens =
      std::min(bucket.burst_bytes, bucket.tokens + refill_bytes);
  bucket.last_refill = now;
}

uint64_t EsFileFerryPacker::peekTokenBucketBytes(TokenBucket &bucket) {
  if (bucket.unlimited) {
    return UINT64_MAX;
  }
  return bucket.tokens;
}

void EsFileFerryPacker::consumeTokenBucketBytes(TokenBucket &bucket,
                                                uint64_t bytes) {
  if (bucket.unlimited) {
    return;
  }
  bucket.tokens = bytes >= bucket.tokens ? 0 : bucket.tokens - bytes;
}

// Scheduling Helpers

std::vector<std::string> EsFileFerryPacker::snapshotSchedulableTaskIds() const {
  std::lock_guard<std::mutex> lock(_mtx);
  std::vector<std::string> ids;
  for (const auto &it : _task_registry.tasks) {
    if (isTaskDataSchedulable(it.second)) {
      ids.emplace_back(it.first);
    }
  }
  return ids;
}

std::vector<std::string> EsFileFerryPacker::pickFairRound(
    const std::vector<std::string> &active_ids) {
  if (active_ids.size() <= 1) {
    return active_ids;
  }
  std::vector<std::string> sorted = active_ids;
  std::sort(sorted.begin(), sorted.end());
  if (sorted.empty()) {
    return sorted;
  }
  std::lock_guard<std::mutex> lock(_mtx);
  auto &cursor = _task_registry.rr_cursor;
  cursor %= sorted.size();
  std::vector<std::string> round;
  round.reserve(sorted.size());
  for (size_t i = 0; i < sorted.size(); ++i) {
    round.emplace_back(sorted[(cursor + i) % sorted.size()]);
  }
  cursor = (cursor + 1) % sorted.size();
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
    return isHttpTaskBufferDrained(task);
  }
  return task.send.sent_bytes >= task.source.file_size;
}

bool EsFileFerryPacker::isTaskControlReady(
    const EsFileFerryPacker::TaskState &task) {
  if (task.send.end_sent) {
    return false;
  }
  if (task.http.failed) {
    return true;
  }
  if (!task.send.info_sent) {
    return !task.http.source || task.http.headers_ready;
  }
  return isTaskReadyToEmitEnd(task);
}

bool EsFileFerryPacker::isHttpTaskBufferDrained(
    const EsFileFerryPacker::TaskState &task) {
  return task.http.buffer.chunks.empty() &&
         task.http.buffer.buffered_bytes == 0 &&
         task.http.buffer.front_chunk_offset == 0;
}

bool EsFileFerryPacker::isTaskDataSchedulable(
    const EsFileFerryPacker::TaskState &task) {
  if (task.send.end_sent || !task.send.info_sent || task.http.failed) {
    return false;
  }
  if (task.http.source) {
    return task.http.headers_ready && !task.http.buffer.chunks.empty() &&
           task.http.buffer.buffered_bytes > 0;
  }
  if (task.source.memory_mode) {
    return task.send.sent_bytes < task.source.memory_payload.size();
  }
  return task.send.sent_bytes < task.source.file_size;
}
