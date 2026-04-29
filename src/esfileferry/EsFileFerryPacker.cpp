#include "EsFileFerryPacker.h"
#include "Util/logger.h"
#include "Util/base64.h"
#include <algorithm>
#include <cctype>
#include <cstring>
#include <cstdlib>
#include <thread>
#include <utility>

constexpr size_t EsFileFerryPacker::kDefaultHttpBufferChunkBytes;
constexpr size_t EsFileFerryPacker::kDefaultMaxHttpBufferBlocksPerTask;
constexpr uint32_t EsFileFerryPacker::kDefaultPaceIntervalMs;
constexpr size_t EsFileFerryPacker::kDefaultHttpFastStartSlotReserve;
constexpr size_t EsFileFerryPacker::kMaxPacketsPerTick;
constexpr size_t EsFileFerryPacker::kControlPlaneStagePacketBudget;

EsFileFerryPacker::EsFileFerryPacker()
    : _http_chunk_pool(kDefaultHttpBufferChunkBytes) {
  std::lock_guard<std::mutex> lock(_mtx);
  _global_options = EsFileGlobalOptions{};
  // ===== 初始化令牌桶和保护期池 =====
  _token_refill_time = std::chrono::steady_clock::now();
  _global_protection_tokens_remaining = _global_options.global_protection_token_pool;
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
    http_join_threads.reserve(_http_fetch_threads.size());
    for (auto &thread_state : _http_fetch_threads) {
      if (thread_state && thread_state->thread.joinable()) {
        http_join_threads.emplace_back(std::move(thread_state->thread));
      }
    }
    _http_fetch_threads.clear();
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
constexpr int64_t kHttpBufferWaitSlowLogMs = 1000;
constexpr int64_t kEmitPacketSlowLogMs = 1000;
constexpr int64_t kRateLimitWaitStepMs = 20;
constexpr uint64_t kBitsPerByte = 8;
constexpr uint64_t kBitsPerMegabit = 1024 * 1024;

uint64_t mbpsToBytesPerSec(uint64_t mbps) {
  return (mbps * kBitsPerMegabit) / kBitsPerByte;
}

uint64_t maxBandwidthBytesPerSec(const EsFileGlobalOptions &options) {
  return mbpsToBytesPerSec(options.max_bandwidth_mbps);
}

uint64_t bytesPerSecToMbps(uint64_t bytes_per_sec) {
  return (bytes_per_sec * kBitsPerByte) / kBitsPerMegabit;
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

// ===== 新增核心函数实现 =====
void EsFileFerryPacker::refillTokenBucket(std::chrono::steady_clock::time_point now) {
  std::lock_guard<std::mutex> lock(_mtx);
  const auto max_bandwidth_bytes_per_sec =
      maxBandwidthBytesPerSec(_global_options);
  if (max_bandwidth_bytes_per_sec == 0) {
    // 0 表示不限制，设置为足够大
    _token_bucket_tokens = static_cast<size_t>(-1);
    return;
  }
  const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - _token_refill_time);
  if (elapsed.count() > 0) {
    const auto bytes = (max_bandwidth_bytes_per_sec * elapsed.count()) / 1000;
    _token_bucket_tokens += static_cast<size_t>(bytes);
    // 令牌桶上限设置为1秒的带宽
    const auto max_bucket = max_bandwidth_bytes_per_sec;
    if (_token_bucket_tokens > max_bucket) {
      _token_bucket_tokens = max_bucket;
    }
    _token_refill_time = now;
  }
}

bool EsFileFerryPacker::isTaskInProtection(const TaskState &task, std::chrono::steady_clock::time_point now) const {
  if (!task.protected_period_active || task.protection_tokens_remaining == 0) {
    return false;
  }
  if (now >= task.protection_end_time) {
    return false;
  }
  return true;
}

EsFileFerryPacker::DataPlaneTaskGroups
EsFileFerryPacker::collectDataPlaneTaskGroupsLocked(
    std::chrono::steady_clock::time_point now) {
  DataPlaneTaskGroups groups;
  for (auto &item : _task_registry.tasks) {
    auto &task = item.second;
    if (task.send.end_sent || !isTaskDataSchedulable(task)) {
      continue;
    }
    if (isTaskInProtection(task, now)) {
      groups.protected_task_ids.push_back(task.task_id);
      continue;
    }
    if (task.protected_period_active) {
      _global_protection_tokens_remaining += task.protection_tokens_remaining;
      task.protected_period_active = false;
      task.protection_tokens_remaining = 0;
    }
    groups.normal_task_ids.push_back(task.task_id);
  }
  return groups;
}

std::vector<std::string> EsFileFerryPacker::collectFailedTaskIdsLocked() const {
  std::vector<std::string> ids;
  for (const auto &item : _task_registry.tasks) {
    if (!item.second.send.end_sent && item.second.http.failed) {
      ids.emplace_back(item.first);
    }
  }
  return ids;
}

std::vector<std::string> EsFileFerryPacker::collectReadyInfoTaskIdsLocked() const {
  std::vector<std::string> ids;
  for (const auto &item : _task_registry.tasks) {
    const auto &task = item.second;
    if (task.send.end_sent || task.http.failed || task.send.info_sent) {
      continue;
    }
    if (!task.http.source || task.http.headers_ready) {
      ids.emplace_back(item.first);
    }
  }
  return ids;
}

std::vector<std::string> EsFileFerryPacker::collectReadyEndTaskIdsLocked() const {
  std::vector<std::string> ids;
  for (const auto &item : _task_registry.tasks) {
    if (isTaskReadyToEmitEnd(item.second)) {
      ids.emplace_back(item.first);
    }
  }
  return ids;
}

size_t EsFileFerryPacker::emitTaskPacketsWithProfileRefresh(
    const std::vector<std::string> &task_ids,
    bool (EsFileFerryPacker::*emit_fn)(const std::string &),
    size_t max_packet_count) {
  if (max_packet_count == 0) {
    return 0;
  }
  size_t packet_count = 0;
  bool dirty = false;
  for (const auto &task_id : task_ids) {
    if (packet_count >= max_packet_count) {
      break;
    }
    if ((this->*emit_fn)(task_id)) {
      ++packet_count;
      dirty = true;
    }
  }
  if (dirty) {
    std::lock_guard<std::mutex> lock(_mtx);
    recomputeAllTaskRateProfilesLocked(false);
  }
  return packet_count;
}

size_t EsFileFerryPacker::runControlPlaneStage(
    std::vector<std::string> (EsFileFerryPacker::*collect_fn)() const,
    bool (EsFileFerryPacker::*emit_fn)(const std::string &),
    size_t max_packet_count) {
  if (max_packet_count == 0) {
    return 0;
  }
  std::vector<std::string> task_ids;
  {
    std::lock_guard<std::mutex> lock(_mtx);
    task_ids = (this->*collect_fn)();
  }
  task_ids = pickFairRound(task_ids);
  if (task_ids.empty()) {
    return 0;
  }
  if (task_ids.size() > max_packet_count) {
    task_ids.resize(max_packet_count);
  }
  return emitTaskPacketsWithProfileRefresh(task_ids, emit_fn, max_packet_count);
}

// Config And Lifecycle

void EsFileFerryPacker::setGlobalOptions(const EsFileGlobalOptions &opts) {
  bool should_try_start_http = false;
  {
    std::lock_guard<std::mutex> lock(_mtx);
    const EsFileGlobalOptions default_opts;
    const auto resolved_chunk_size =
        opts.chunk_size_bytes == 0 ? default_opts.chunk_size_bytes
                                   : opts.chunk_size_bytes;
    const auto resolved_protection_period_ms =
        opts.protection_period_ms == 0 ? default_opts.protection_period_ms
                                       : opts.protection_period_ms;
    const auto resolved_bootstrap_interval_ms =
        opts.bootstrap_interval_ms == 0 ? default_opts.bootstrap_interval_ms
                                        : opts.bootstrap_interval_ms;
    const auto resolved_http_concurrency_limit =
        opts.http_concurrency_limit == 0 ? default_opts.http_concurrency_limit
                                         : opts.http_concurrency_limit;
    const auto resolved_http_buffer_limit_bytes =
        opts.http_buffer_limit_bytes == 0 ? default_opts.http_buffer_limit_bytes
                                          : opts.http_buffer_limit_bytes;
    const auto resolved_http_per_task_buffer_limit_bytes =
        opts.http_per_task_buffer_limit_bytes == 0
            ? default_opts.http_per_task_buffer_limit_bytes
            : opts.http_per_task_buffer_limit_bytes;

    _global_options.chunk_size_bytes = resolved_chunk_size;
    _global_options.protection_period_ms = resolved_protection_period_ms;
    _global_options.bootstrap_interval_ms = resolved_bootstrap_interval_ms;
    _global_options.http_concurrency_limit = resolved_http_concurrency_limit;
    _global_options.http_buffer_limit_bytes = resolved_http_buffer_limit_bytes;
    _global_options.http_per_task_buffer_limit_bytes =
        resolved_http_per_task_buffer_limit_bytes;
    _global_options.max_bandwidth_mbps = opts.max_bandwidth_mbps;
    _global_options.initial_task_protection_tokens =
        opts.initial_task_protection_tokens == 0
            ? default_opts.initial_task_protection_tokens
            : opts.initial_task_protection_tokens;
    _global_options.global_protection_token_pool =
        opts.global_protection_token_pool == 0
            ? default_opts.global_protection_token_pool
            : opts.global_protection_token_pool;
    _global_options.protection_bandwidth_percent =
        opts.protection_bandwidth_percent == 0
            ? default_opts.protection_bandwidth_percent
            : opts.protection_bandwidth_percent;
    _global_options.enable_annexb_payload_escape =
        opts.enable_annexb_payload_escape;

    _global_protection_tokens_remaining = _global_options.global_protection_token_pool;
    _token_refill_time = std::chrono::steady_clock::now();
    const auto max_bandwidth_bytes_per_sec =
        maxBandwidthBytesPerSec(_global_options);
    _token_bucket_tokens = max_bandwidth_bytes_per_sec == 0
                               ? static_cast<size_t>(-1)
                               : static_cast<size_t>(max_bandwidth_bytes_per_sec);
    stopBootstrapTimerLocked();
    startBootstrapTimerLocked();

    recomputeAllTaskRateProfilesLocked(false);
    should_try_start_http = !_http_runtime.pending_fetches.empty();
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
  bool should_wake = false;
  {
    std::lock_guard<std::mutex> lock(_mtx);
    if (_packet_runtime.downstream_congested == congested) {
      return;
    }
    _packet_runtime.downstream_congested = congested;
    should_wake = !congested;
  }
  if (should_wake) {
    _packet_runtime.packet_sem.post();
  }
}

bool EsFileFerryPacker::addTask(const std::string &task_id,
                                const std::string &source,
                                const std::string &method,
                                const HttpHeaders &headers,
                                const std::string &body,
                                const std::string &file_name) {

  if (task_id.empty()) {
    WarnL << "reject ferry task: empty task_id";
    return false;
  }
  if (source.empty()) {
    WarnL << "reject ferry task, task_id:" << task_id << " reason: empty source";
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
      WarnL << "reject ferry task, task_id:" << task_id
            << " reason: method must be GET or POST"
            << " method:" << method_upper;
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
      WarnL << "reject ferry task, task_id:" << task_id
            << " reason: open file failed path:" << source;
      return false;
    }
    auto stream = std::make_shared<std::ifstream>(source, std::ios::binary);
    if (!stream->is_open()) {
      WarnL << "reject ferry task, task_id:" << task_id
            << " reason: open stream failed path:" << source;
      return false;
    }
    state.source.file_path = source;
    state.source.file_name = pickFileName(source, file_name);
    state.source.file_size = file_size;
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
      sent_bytes = it->second.send.sent_bytes;
      file_size = it->second.source.file_size;
      
      // 归还保护期token
      if (it->second.protected_period_active && it->second.protection_tokens_remaining > 0) {
        _global_protection_tokens_remaining += it->second.protection_tokens_remaining;
      }
      
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
  size_t task_count = 0;
  size_t pending_fetch_count = 0;
  size_t active_fetch_count = 0;
  uint64_t buffered_bytes = 0;
  {
    std::lock_guard<std::mutex> lock(_mtx);
    task_count = _task_registry.tasks.size();
    pending_fetch_count = _http_runtime.pending_fetches.size();
    active_fetch_count = _http_runtime.active_fetches;
    buffered_bytes = _http_runtime.total_buffered_bytes;
    buffer_cvs.reserve(_task_registry.tasks.size());
    for (auto &item : _task_registry.tasks) {
      if (item.second.http.buffer.cv) {
        buffer_cvs.emplace_back(item.second.http.buffer.cv);
      }
      clearTaskHttpBufferLocked(item.second);
    }
    _task_registry.tasks.clear();
    _http_runtime.pending_fetches.clear();
    _http_runtime.total_buffered_bytes = 0;
    _global_protection_tokens_remaining =
        _global_options.global_protection_token_pool;
    recomputeAllTaskRateProfilesLocked(false);
  }

  for (const auto &buffer_cv : buffer_cvs) {
    buffer_cv->notify_all();
  }
  InfoL << "clear ferry tasks"
        << " task_count:" << task_count
        << " pending_fetches:" << pending_fetch_count
        << " active_fetches:" << active_fetch_count
        << " buffered_bytes:" << buffered_bytes;
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
      static_cast<float>(_global_options.bootstrap_interval_ms) / 1000.0f,
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
    {
      std::lock_guard<std::mutex> lock(_mtx);
      reapCompletedHttpFetchThreadsLocked(http_join_threads);
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
      }
      if (should_emit_bootstrap && bootstrap_cb) {
        emitBootstrapPackets(bootstrap_cb);
      }
      processTickPackets(kMaxPacketsPerTick);
      break;
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

void EsFileFerryPacker::shrinkChunkPacket(EsFilePacketHeader &header,
                                          std::vector<uint8_t> &packet,
                                          size_t payload_offset,
                                          size_t payload_len) {
  header.payload_len = static_cast<uint32_t>(payload_len);
  header.total_len = static_cast<uint32_t>(
      kEsFileFixedHeaderSize + header.task_id_len + header.file_name_len +
      header.payload_len);
  const auto payload_len_pos = static_cast<size_t>(kEsFileCarrierPrefixSize + 24);
  const auto total_len_pos = static_cast<size_t>(kEsFileCarrierPrefixSize + 40);
  writeU32BEAt(packet, payload_len_pos, header.payload_len);
  writeU32BEAt(packet, total_len_pos, header.total_len);
  packet.resize(payload_offset + payload_len);
}

bool EsFileFerryPacker::emitFailedStatusPacket(const std::string &task_id) {
  std::string file_name;
  uint64_t file_size = 0;
  uint32_t next_seq = 0;
  std::string http_error;
  {
    std::lock_guard<std::mutex> lock(_mtx);
    auto it = _task_registry.tasks.find(task_id);
    if (it == _task_registry.tasks.end() || it->second.send.end_sent ||
        !it->second.http.failed) {
      return false;
    }
    file_name = it->second.source.file_name;
    file_size = it->second.source.file_size;
    next_seq = it->second.send.next_seq;
    http_error = it->second.http.error;
  }

  std::vector<uint8_t> status_payload(http_error.begin(), http_error.end());
  TaskState status_task;
  status_task.task_id = task_id;
  status_task.source.file_name = file_name;
  status_task.source.file_size = file_size;
  auto status_header =
      makePacketHeader(status_task, EsFilePacketType::TaskStatus, 0,
                       static_cast<uint32_t>(status_payload.size()), 0,
                       next_seq, nextRelativeTimestampMs());
  auto status_packet = buildPacket(status_task, status_header, status_payload);
  if (!emitPacket(task_id, std::move(status_packet), status_header)) {
    return false;
  }

  std::lock_guard<std::mutex> lock(_mtx);
  auto it = _task_registry.tasks.find(task_id);
  if (it == _task_registry.tasks.end() || it->second.send.end_sent ||
      !it->second.http.failed) {
    return false;
  }
  it->second.send.info_sent = true;
  it->second.send.end_sent = true;
  it->second.send.next_seq++;
  return true;
}

bool EsFileFerryPacker::emitFileInfoPacket(const std::string &task_id) {
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
      return false;
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

  TaskState info_task;
  info_task.task_id = task_id;
  info_task.source.file_name = file_name;
  info_task.source.file_size = file_size;
  auto info_header = makePacketHeader(
      info_task, EsFilePacketType::FileInfo, 0,
      static_cast<uint32_t>(http_meta_payload.size()), info_flags, next_seq,
      nextRelativeTimestampMs());
  auto info_packet = buildPacket(info_task, info_header, http_meta_payload);
  if (!emitPacket(task_id, std::move(info_packet), info_header)) {
    return false;
  }

  std::lock_guard<std::mutex> lock(_mtx);
  auto it = _task_registry.tasks.find(task_id);
  if (it == _task_registry.tasks.end() || it->second.send.end_sent ||
      it->second.send.info_sent) {
    return false;
  }
  it->second.send.info_sent = true;
  it->second.send.next_seq++;

  const auto now = std::chrono::steady_clock::now();
  const auto requested_tokens = _global_options.initial_task_protection_tokens;
  if (requested_tokens > 0 &&
      _global_protection_tokens_remaining >= requested_tokens) {
    it->second.protected_period_active = true;
    it->second.protection_tokens_remaining = requested_tokens;
    it->second.protection_end_time =
        now + std::chrono::milliseconds(_global_options.protection_period_ms);
    _global_protection_tokens_remaining -= requested_tokens;
  }
  return true;
}

bool EsFileFerryPacker::tryEmitTaskChunk(const std::string &task_id,
                                         bool consume_protection_token,
                                         size_t *protected_bandwidth_used) {
  bool should_try_start_http = false;
  bool http_chunk_mode = false;
  bool ready_packet = false;
  uint64_t generation = 0;
  uint64_t offset = 0;
  uint32_t seq = 0;
  size_t read_len = 0;
  size_t payload_offset = 0;
  const auto packet_ts = nextRelativeTimestampMs();
  std::shared_ptr<std::ifstream> file_stream;
  std::shared_ptr<std::condition_variable> buffer_cv;
  TaskState packet_task;
  EsFilePacketHeader packet_header;
  std::vector<uint8_t> packet;

  {
    std::lock_guard<std::mutex> lock(_mtx);
    auto it = _task_registry.tasks.find(task_id);
    if (it == _task_registry.tasks.end() || it->second.send.end_sent ||
        !isTaskDataSchedulable(it->second)) {
      return false;
    }

    auto &task = it->second;
    http_chunk_mode = task.http.source;
    generation = task.generation;
    offset = task.send.sent_bytes;
    seq = task.send.next_seq;

    if (task.http.source) {
      if (task.http.buffer.buffered_bytes == 0 ||
          task.http.buffer.chunks.empty()) {
        return false;
      }

      read_len = static_cast<size_t>(std::min<uint64_t>(
          _global_options.chunk_size_bytes, task.http.buffer.buffered_bytes));
      if (maxBandwidthBytesPerSec(_global_options) > 0) {
        read_len = static_cast<size_t>(
            std::min<uint64_t>(read_len, _token_bucket_tokens));
      }
      if (read_len == 0) {
        return false;
      }

      packet_task.task_id = task.task_id;
      packet_task.source.file_name = task.source.file_name;
      packet_task.source.file_size = task.source.file_size;
      packet_header = makePacketHeader(packet_task, EsFilePacketType::FileChunk,
                                       offset, static_cast<uint32_t>(read_len),
                                       0, seq, packet_ts);
      packet =
          buildPacket(packet_task, packet_header, read_len, &payload_offset);

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
        return false;
      }
      if (copied < read_len) {
        read_len = copied;
        shrinkChunkPacket(packet_header, packet, payload_offset, copied);
      }

      if (maxBandwidthBytesPerSec(_global_options) > 0) {
        _token_bucket_tokens =
            read_len >= _token_bucket_tokens ? 0 : _token_bucket_tokens - read_len;
        if (protected_bandwidth_used) {
          *protected_bandwidth_used += read_len;
        }
      }
      if (consume_protection_token && task.protected_period_active &&
          task.protection_tokens_remaining > 0) {
        task.protection_tokens_remaining--;
      }
      task.send.sent_bytes += read_len;
      task.send.next_seq++;
      buffer_cv = task.http.buffer.cv;
      ready_packet = true;
      should_try_start_http =
          !_http_runtime.pending_fetches.empty() &&
          _http_runtime.active_fetches < _global_options.http_concurrency_limit &&
          _http_runtime.total_buffered_bytes <
              _global_options.http_buffer_limit_bytes;
    } else {
      if (task.send.sent_bytes >= task.source.file_size) {
        return false;
      }

      const auto remain = task.source.file_size - task.send.sent_bytes;
      read_len = static_cast<size_t>(
          std::min<uint64_t>(_global_options.chunk_size_bytes, remain));
      if (maxBandwidthBytesPerSec(_global_options) > 0) {
        read_len = static_cast<size_t>(
            std::min<uint64_t>(read_len, _token_bucket_tokens));
      }
      if (read_len == 0) {
        return false;
      }
      file_stream = task.source.stream;
    }
  }

  if (buffer_cv) {
    buffer_cv->notify_one();
  }
  if (should_try_start_http) {
    maybeStartPendingHttpFetches();
  }

  if (!http_chunk_mode) {
    if (!file_stream) {
      return false;
    }

    {
      std::lock_guard<std::mutex> lock(_mtx);
      auto it = _task_registry.tasks.find(task_id);
      if (it == _task_registry.tasks.end()) {
        return false;
      }
      packet_task.task_id = it->second.task_id;
      packet_task.source.file_name = it->second.source.file_name;
      packet_task.source.file_size = it->second.source.file_size;
    }

    packet_header = makePacketHeader(packet_task, EsFilePacketType::FileChunk,
                                     offset, static_cast<uint32_t>(read_len), 0,
                                     seq, packet_ts);
    packet = buildPacket(packet_task, packet_header, read_len, &payload_offset);
    file_stream->read(
        reinterpret_cast<char *>(packet.data() + static_cast<long>(payload_offset)),
        static_cast<std::streamsize>(read_len));
    const auto read_size = static_cast<size_t>(file_stream->gcount());
    if (read_size == 0) {
      return false;
    }
    if (read_size < read_len) {
      read_len = read_size;
      shrinkChunkPacket(packet_header, packet, payload_offset, read_size);
    }

    std::lock_guard<std::mutex> lock(_mtx);
    auto it = _task_registry.tasks.find(task_id);
    if (it == _task_registry.tasks.end() || it->second.send.end_sent) {
      return false;
    }
    auto &task = it->second;
    if (maxBandwidthBytesPerSec(_global_options) > 0) {
      _token_bucket_tokens =
          read_len >= _token_bucket_tokens ? 0 : _token_bucket_tokens - read_len;
      if (protected_bandwidth_used) {
        *protected_bandwidth_used += read_len;
      }
    }
    if (consume_protection_token && task.protected_period_active &&
        task.protection_tokens_remaining > 0) {
      task.protection_tokens_remaining--;
    }
    task.send.sent_bytes += read_len;
    task.send.next_seq++;
    ready_packet = true;
  }

  if (!ready_packet) {
    return false;
  }

  if (!emitPacket(task_id, std::move(packet), packet_header)) {
    return false;
  }
  if (http_chunk_mode) {
    markHttpFirstChunkEmitted(task_id, generation);
  }
  return true;
}

size_t EsFileFerryPacker::processDataPlaneTasks(
    const std::vector<std::string> &task_ids, bool consume_protection_token,
    size_t max_bandwidth_bytes, size_t *bandwidth_used,
    size_t max_packet_count) {
  if (max_packet_count == 0 || task_ids.empty()) {
    return 0;
  }
  size_t packet_count = 0;

  while (packet_count < max_packet_count) {
    bool emitted_this_round = false;
    for (const auto &task_id : task_ids) {
      if (packet_count >= max_packet_count) {
        break;
      }
      {
        std::lock_guard<std::mutex> lock(_mtx);
        if (maxBandwidthBytesPerSec(_global_options) > 0) {
          if (_token_bucket_tokens == 0) {
            return packet_count;
          }
          if (bandwidth_used && max_bandwidth_bytes > 0 &&
              *bandwidth_used >= max_bandwidth_bytes) {
            return packet_count;
          }
        }
      }
      if (tryEmitTaskChunk(task_id, consume_protection_token, bandwidth_used)) {
        ++packet_count;
        emitted_this_round = true;
      }
    }
    if (!emitted_this_round) {
      break;
    }
  }
  return packet_count;
}

bool EsFileFerryPacker::emitFileEndPacket(const std::string &task_id) {
  uint64_t end_offset = 0;
  uint32_t end_seq = 0;
  TaskState end_task;
  {
    std::lock_guard<std::mutex> lock(_mtx);
    auto it = _task_registry.tasks.find(task_id);
    if (it == _task_registry.tasks.end() || !isTaskReadyToEmitEnd(it->second)) {
      return false;
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
  }

  auto end_header = makePacketHeader(end_task, EsFilePacketType::FileEnd,
                                     end_offset, 0, 0, end_seq,
                                     nextRelativeTimestampMs());
  auto end_packet = buildPacket(end_task, end_header, {});
  return emitPacket(task_id, std::move(end_packet), end_header);
}

size_t EsFileFerryPacker::processTickPackets(size_t max_packet_count) {
  if (max_packet_count == 0) {
    return 0;
  }
  size_t packet_count = 0;
  auto remaining_budget = [&]() {
    return max_packet_count > packet_count ? max_packet_count - packet_count : 0;
  };

  {
    std::lock_guard<std::mutex> lock(_mtx);
    recomputeAllTaskRateProfilesLocked(false);
  }

  // Control plane first: failed/info/end packets bypass the data fair round.
  packet_count +=
      runControlPlaneStage(&EsFileFerryPacker::collectFailedTaskIdsLocked,
                           &EsFileFerryPacker::emitFailedStatusPacket,
                           std::min(kControlPlaneStagePacketBudget,
                                    remaining_budget()));
  packet_count +=
      runControlPlaneStage(&EsFileFerryPacker::collectReadyInfoTaskIdsLocked,
                           &EsFileFerryPacker::emitFileInfoPacket,
                           std::min(kControlPlaneStagePacketBudget,
                                    remaining_budget()));
  if (remaining_budget() == 0) {
    return packet_count;
  }

  {
    std::lock_guard<std::mutex> lock(_mtx);
    if (_packet_runtime.downstream_congested) {
      return packet_count;
    }
  }

  // Data plane: 根据 DESIGN_TokenBucket.md 重构的令牌桶和保护期控制
  const auto now = std::chrono::steady_clock::now();
  refillTokenBucket(now);

  DataPlaneTaskGroups data_plane_groups;
  {
    std::lock_guard<std::mutex> lock(_mtx);
    data_plane_groups = collectDataPlaneTaskGroupsLocked(now);
  }

  data_plane_groups.protected_task_ids =
      pickFairRound(data_plane_groups.protected_task_ids);
  data_plane_groups.normal_task_ids =
      pickFairRound(data_plane_groups.normal_task_ids);

  if (!data_plane_groups.protected_task_ids.empty()) {
    size_t protected_bandwidth_used = 0;
    size_t max_protected_bandwidth = 0;
    {
      std::lock_guard<std::mutex> lock(_mtx);
      const auto max_bandwidth_bytes_per_sec =
          maxBandwidthBytesPerSec(_global_options);
      if (max_bandwidth_bytes_per_sec > 0) {
        max_protected_bandwidth =
            (max_bandwidth_bytes_per_sec *
             _global_options.protection_bandwidth_percent) / 100;
      }
    }

    packet_count += processDataPlaneTasks(data_plane_groups.protected_task_ids,
                                          true, max_protected_bandwidth,
                                          &protected_bandwidth_used,
                                          remaining_budget());
  }

  if (!data_plane_groups.normal_task_ids.empty() && remaining_budget() > 0) {
    packet_count +=
        processDataPlaneTasks(data_plane_groups.normal_task_ids, false, 0,
                              nullptr, remaining_budget());
  }

  packet_count +=
      runControlPlaneStage(&EsFileFerryPacker::collectReadyEndTaskIdsLocked,
                           &EsFileFerryPacker::emitFileEndPacket,
                           std::min(kControlPlaneStagePacketBudget,
                                    remaining_budget()));

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

void EsFileFerryPacker::reapCompletedHttpFetchThreadsLocked(
    std::vector<std::thread> &join_threads) {
  auto it = _http_fetch_threads.begin();
  while (it != _http_fetch_threads.end()) {
    auto &thread_state = *it;
    if (thread_state && thread_state->done &&
        thread_state->done->load(std::memory_order_acquire)) {
      if (thread_state->thread.joinable()) {
        join_threads.emplace_back(std::move(thread_state->thread));
      }
      it = _http_fetch_threads.erase(it);
      continue;
    }
    ++it;
  }
}

std::vector<std::pair<std::string, uint64_t>>
EsFileFerryPacker::collectHttpFetchLaunchesLocked() {
  std::vector<std::pair<std::string, uint64_t>> launches;
  bool dirty = false;
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
  const auto total_limit = _global_options.http_concurrency_limit;
  const auto fast_start_reserve =
      std::min(kDefaultHttpFastStartSlotReserve, total_limit);
  const auto normal_limit =
      total_limit > fast_start_reserve ? total_limit - fast_start_reserve : 0;
  std::deque<std::pair<std::string, uint64_t>> deferred;
  const auto pending_count = _http_runtime.pending_fetches.size();
  for (size_t i = 0; i < pending_count; ++i) {
    if (_http_runtime.pending_fetches.empty()) {
      break;
    }
    if (_http_runtime.total_buffered_bytes >=
        _global_options.http_buffer_limit_bytes) {
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
    task.http.active_since = now;
    task.http.first_chunk_emitted = false;
    task.http.fast_start_granted = can_grant_fast_start;
    if (!can_grant_fast_start) {
      task.http.fast_start_candidate = false;
    } else {
      ++granted_active_fetches;
    }
    ++_http_runtime.active_fetches;
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
  } else if (_global_options.protection_period_ms == 0) {
    should_release = true;
    reason = "protection_disabled";
  } else if (task.http.active_since != std::chrono::steady_clock::time_point{} &&
             std::chrono::duration_cast<std::chrono::milliseconds>(
                 now - task.http.active_since)
                     .count() >=
                 _global_options.protection_period_ms) {
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

  auto fetch_done = std::make_shared<std::atomic_bool>(false);
  std::thread fetch_thread([this, task_id, generation, source, method, headers,
                            body, fetch_done]() {
    const auto fetch_begin = std::chrono::steady_clock::now();
    InfoL << "http fetch start task_id:" << task_id
          << " generation:" << generation
          << " method:" << method
          << " url:" << source;
    HttpHeaders response_headers;
    uint32_t response_status_code = 0;
    std::string fetch_err;
    HttpStreamFetcher::TransferDiagnostics diagnostics;
    const bool ok = HttpStreamFetcher::stream(
        source, method, headers, body,
        [this, task_id, generation](const uint8_t *data, size_t size) {
          if (!data || size == 0) {
            return true;
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
          while (consumed < size) {
            size_t copy_len = 0;
            {
              std::unique_lock<std::mutex> lock(_mtx);
              bool waited_for_buffer = false;
              bool waited_for_rate = false;
              const auto wait_begin = std::chrono::steady_clock::now();
              while (true) {
                auto it = _task_registry.tasks.find(task_id);
                if (_packet_runtime.packet_thread_exit) {
                  logChunkAbort("packet_thread_exit", 0, 0, false);
                  return false;
                }
                if (it == _task_registry.tasks.end()) {
                  logChunkAbort("task_missing", 0, 0, false);
                  return false;
                }
                if (it->second.generation != generation) {
                  logChunkAbort("generation_mismatch", it->second.generation,
                                it->second.http.buffer.buffered_bytes, true);
                  return false;
                }
                auto &task = it->second;
                auto buffer_cv = it->second.http.buffer.cv;
                if (!buffer_cv) {
                  logChunkAbort("null_buffer_cv", task.generation,
                                task.http.buffer.buffered_bytes, true);
                  return false;
                }
                refillTokenBucket(task.control.fetch_bucket);

                if (task.http.buffer.buffered_bytes >=
                        task.control.max_buffered_bytes &&
                    task.http.buffer.buffered_bytes >
                        task.control.resume_buffered_bytes) {
                  waited_for_buffer = true;
                  buffer_cv->wait_for(
                      lock, std::chrono::milliseconds(kRateLimitWaitStepMs));
                  continue;
                }
                if (_http_runtime.total_buffered_bytes >=
                    _global_options.http_buffer_limit_bytes) {
                  waited_for_buffer = true;
                  buffer_cv->wait_for(
                      lock, std::chrono::milliseconds(kRateLimitWaitStepMs));
                  continue;
                }

                const auto task_room = task.control.max_buffered_bytes >
                                               task.http.buffer.buffered_bytes
                                           ? task.control.max_buffered_bytes -
                                                 task.http.buffer.buffered_bytes
                                           : 0;
                const auto total_room =
                    _global_options.http_buffer_limit_bytes >
                            _http_runtime.total_buffered_bytes
                        ? _global_options.http_buffer_limit_bytes -
                              _http_runtime.total_buffered_bytes
                        : 0;
                auto fetch_tokens = peekTokenBucketBytes(task.control.fetch_bucket);
                if (task_room == 0 || total_room == 0) {
                  waited_for_buffer = true;
                  buffer_cv->wait_for(
                      lock, std::chrono::milliseconds(kRateLimitWaitStepMs));
                  continue;
                }
                if (fetch_tokens == 0) {
                  waited_for_rate = true;
                  auto wait_for =
                      estimateTokenWait(task.control.fetch_bucket, 1);
                  if (wait_for.count() <= 0) {
                    wait_for = std::chrono::milliseconds(kRateLimitWaitStepMs);
                  }
                  buffer_cv->wait_for(lock, wait_for);
                  continue;
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
                  waited_for_rate = true;
                  buffer_cv->wait_for(
                      lock, std::chrono::milliseconds(kRateLimitWaitStepMs));
                  continue;
                }
                std::memcpy(chunk->data.data(), data + consumed, copy_len);
                chunk->size = copy_len;
                consumeTokenBucketBytes(task.control.fetch_bucket, copy_len);
                task.http.received_bytes += copy_len;
                task.http.buffer.buffered_bytes += copy_len;
                _http_runtime.total_buffered_bytes += copy_len;
                task.http.buffer.chunks.emplace_back(std::move(chunk));
                break;
              }
              if (waited_for_buffer || waited_for_rate) {
                const auto wait_ms =
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - wait_begin)
                        .count();
                if (wait_ms >= kHttpBufferWaitSlowLogMs) {
                  auto it = _task_registry.tasks.find(task_id);
                  const auto buffered_bytes =
                      it != _task_registry.tasks.end() ? it->second.http.buffer.buffered_bytes : 0;
                  WarnL << "http chunk buffer wait, task_id:" << task_id
                        << " wait_ms:" << wait_ms
                        << " waited_for_rate:" << waited_for_rate
                        << " task_buffered_bytes:" << buffered_bytes
                        << " total_buffered_bytes:" << _http_runtime.total_buffered_bytes
                        << " chunk_block_size:" << kDefaultHttpBufferChunkBytes
                        << " task_buffer_limit:"
                        << (it != _task_registry.tasks.end()
                                ? it->second.control.max_buffered_bytes
                                : 0);
                }
              }
            }
            consumed += copy_len;
            _packet_runtime.packet_sem.post();
          }
          return true;
        },
        [this, task_id, generation, source](uint32_t status_code,
                                            const HttpHeaders &headers_in) {
          uint64_t content_length = 0;
          const bool has_content_length =
              tryParseContentLength(headers_in, content_length);
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
          task.http.response_meta_payload =
              buildHttpResponseMetaPayload(status_code, headers_in);
          task.http.headers_ready = true;
          if (has_content_length) {
            task.source.file_size = content_length;
            task.http.size_known = true;
          }
          InfoL << "http fetch headers task_id:" << task_id
                << " generation:" << generation
                << " status:" << status_code
                << " has_content_length:" << has_content_length
                << " content_length:" << content_length
                << " response_header_count:" << headers_in.size()
                << " url:" << source;
          _packet_runtime.packet_sem.post();
        },
        &response_headers, &response_status_code, fetch_err, &diagnostics);

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
        task.http.fast_start_granted = false;
        task.http.fast_start_candidate = false;
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
    fetch_done->store(true, std::memory_order_release);
    if (!packet_runtime_stopped) {
      _packet_runtime.packet_sem.post();
    }
  });
  std::vector<std::thread> http_join_threads;
  {
    std::lock_guard<std::mutex> lock(_mtx);
    reapCompletedHttpFetchThreadsLocked(http_join_threads);
    _http_fetch_threads.emplace_back(
        std::unique_ptr<HttpFetchThreadState>(
            new HttpFetchThreadState(std::move(fetch_thread), fetch_done)));
  }
  for (auto &thread : http_join_threads) {
    if (thread.joinable()) {
      thread.join();
    }
  }
}

// Query And Packet Helpers

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
    const EsFilePacketHeader &header) {
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
        _global_options.enable_annexb_payload_escape &&
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

void EsFileFerryPacker::refreshUnifiedTaskProfileLocked(TaskState &task,
                                                        bool reset_buckets) {
  const auto now = std::chrono::steady_clock::now();
  task.control.max_buffered_bytes =
      _global_options.http_per_task_buffer_limit_bytes;
  task.control.resume_buffered_bytes = task.control.max_buffered_bytes / 2;

  const auto fetch_burst =
      inferUnifiedBurstBytes(_global_options.chunk_size_bytes);
  const auto emit_burst =
      inferUnifiedBurstBytes(_global_options.chunk_size_bytes);
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
  // - HTTP fetch is only controlled by buffer backpressure;
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
  const uint64_t configured_total_rate_bytes_per_sec =
      maxBandwidthBytesPerSec(_global_options);
  const uint64_t total_emit_rate_bytes_per_sec =
      configured_total_rate_bytes_per_sec;
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
      inferUnifiedBurstBytes(_global_options.chunk_size_bytes);
  task.control.emit_bucket.burst_bytes =
      inferUnifiedBurstBytes(_global_options.chunk_size_bytes);

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

std::chrono::milliseconds EsFileFerryPacker::estimateTokenWait(
    const TokenBucket &bucket, uint64_t min_bytes) {
  if (bucket.unlimited || bucket.tokens >= min_bytes) {
    return std::chrono::milliseconds(0);
  }
  if (bucket.rate_bps == 0) {
    return std::chrono::milliseconds(0);
  }
  const auto deficit = min_bytes - bucket.tokens;
  const auto wait_ms = (deficit * 1000 + bucket.rate_bps - 1) / bucket.rate_bps;
  return std::chrono::milliseconds(wait_ms == 0 ? 1 : wait_ms);
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
  return task.send.sent_bytes < task.source.file_size;
}
