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
constexpr uint32_t EsFileFerryPacker::kDefaultPaceIntervalMs;
constexpr uint64_t EsFileFerryPacker::kDefaultSchedulerRoundBudgetBytes;
constexpr uint64_t EsFileFerryPacker::kDefaultMinEmitPayloadBytes;
constexpr uint64_t EsFileFerryPacker::kDefaultApiBufferedBytes;
constexpr uint64_t EsFileFerryPacker::kDefaultMp4BufferedBytes;
constexpr uint64_t EsFileFerryPacker::kDefaultDownloadBufferedBytes;

EsFileFerryPacker &EsFileFerryPacker::Instance() {
  static std::shared_ptr<EsFileFerryPacker> instance(new EsFileFerryPacker());
  static EsFileFerryPacker &ref = *instance;
  return ref;
}

EsFileFerryPacker::EsFileFerryPacker()
    : _http_chunk_pool(kDefaultHttpBufferChunkBytes) {
  std::lock_guard<std::mutex> lock(_mtx);
  _global_options = EsFileGlobalOptions{};
  _http_chunk_pool.setSize(kDefaultMaxHttpBufferBlocksPerTask * 8);
  startPacketThreadLocked();
  startBootstrapTimerLocked();
  startPaceTimerLocked();
}

EsFileFerryPacker::~EsFileFerryPacker() {
  std::thread join_thread;
  {
    std::lock_guard<std::mutex> lock(_mtx);
    stopPaceTimerLocked();
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
constexpr int64_t kRateLimitWaitStepMs = 20;

bool isHttpUrl(const std::string &value) {
  return value.rfind("http://", 0) == 0 || value.rfind("https://", 0) == 0;
}

size_t priorityToIndex(EsFileTaskPriority priority) {
  return static_cast<size_t>(priority);
}

EsFileTaskPriority inferPriorityFromType(EsFileTaskType type) {
  switch (type) {
  case EsFileTaskType::HttpApiRequest:
    return EsFileTaskPriority::High;
  case EsFileTaskType::HttpMp4Vod:
    return EsFileTaskPriority::Medium;
  case EsFileTaskType::HttpResourceDownload:
  default:
    return EsFileTaskPriority::Low;
  }
}

uint64_t inferDefaultBufferedBytes(EsFileTaskType type) {
  switch (type) {
  case EsFileTaskType::HttpApiRequest:
    return 512 * 1024;
  case EsFileTaskType::HttpMp4Vod:
    return 1024 * 1024;
  case EsFileTaskType::HttpResourceDownload:
  default:
    return 4 * 1024 * 1024;
  }
}

uint64_t inferDefaultBurstBytes(EsFileTaskType type, size_t chunk_size) {
  switch (type) {
  case EsFileTaskType::HttpApiRequest:
    return std::max<uint64_t>(chunk_size, 128 * 1024);
  case EsFileTaskType::HttpMp4Vod:
    return std::max<uint64_t>(chunk_size, 64 * 1024);
  case EsFileTaskType::HttpResourceDownload:
  default:
    return std::max<uint64_t>(chunk_size * 2, 256 * 1024);
  }
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
    _global_options.http_pull_total_rate_bps = opts.http_pull_total_rate_bps;
    _global_options.http_api_rate_share = opts.http_api_rate_share;
    _global_options.http_mp4_rate_share = opts.http_mp4_rate_share;
    _global_options.http_download_rate_share = opts.http_download_rate_share;
    if (_global_options.http_api_rate_share == 0 &&
        _global_options.http_mp4_rate_share == 0 &&
        _global_options.http_download_rate_share == 0) {
      _global_options.http_api_rate_share = 50;
      _global_options.http_mp4_rate_share = 35;
      _global_options.http_download_rate_share = 15;
    }
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
    state.http.method = method_upper;
    state.http.request_headers = headers;
    state.http.request_body = body;
    state.http.buffer.cv = std::make_shared<std::condition_variable>();
    state.control.task_type =
        inferTaskTypeFromRequest(source, method_upper, headers, body);
    uint64_t generation = 0;
    {
      std::lock_guard<std::mutex> lock(_mtx);
      applyTaskProfileLocked(state, state.control.task_type, true);
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
    applyTaskProfileLocked(state, EsFileTaskType::HttpResourceDownload, true);
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
    _task_registry.priority_rr_cursor = {{0, 0, 0}};
    recomputeAllTaskRateProfilesLocked(false);
    should_schedule_http = !_http_runtime.pending_fetches.empty();
  }
  for (const auto &buffer_cv : buffer_cvs) {
    buffer_cv->notify_all();
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
  while (true) {
    _packet_runtime.packet_sem.wait();
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
  auto active_groups = snapshotActiveTasksByPriority();
  if (active_groups[0].empty() && active_groups[1].empty() &&
      active_groups[2].empty()) {
    return packet_count;
  }

  struct TaskSendSnapshot {
    std::string file_name;
    uint64_t file_size = 0;
    uint32_t next_seq = 0;
    bool info_sent = false;
    bool http_fetch_pending = false;
    bool http_failed = false;
    std::string http_error;
    bool http_headers_ready = false;
    std::vector<uint8_t> http_meta_payload;
  };

  for (EsFileTaskPriority priority : {EsFileTaskPriority::High,
                                      EsFileTaskPriority::Medium,
                                      EsFileTaskPriority::Low}) {
    const auto &priority_ids = active_groups[priorityToIndex(priority)];
    if (priority_ids.empty()) {
      continue;
    }
    uint64_t priority_quota =
        computePriorityQuota(total_payload_quota_bytes, priority, active_groups);
    if (priority_quota == 0) {
      continue;
    }
    auto round_ids = pickFairRound(priority_ids, priority);
    const auto active_count = round_ids.size();

    for (size_t i = 0; i < active_count; ++i) {
      const auto &task_id = round_ids[i];
      uint64_t quota = computeTaskQuota(priority_quota, active_count, i);

      TaskSendSnapshot snapshot;
      {
        std::lock_guard<std::mutex> lock(_mtx);
        auto it = _task_registry.tasks.find(task_id);
        if (it == _task_registry.tasks.end() || it->second.send.end_sent) {
          continue;
        }
        const auto &task = it->second;
        snapshot.file_name = task.source.file_name;
        snapshot.file_size = task.source.file_size;
        snapshot.next_seq = task.send.next_seq;
        snapshot.info_sent = task.send.info_sent;
        snapshot.http_fetch_pending =
            task.http.source && (task.http.queued || task.http.active);
        snapshot.http_failed = task.http.failed;
        snapshot.http_error = task.http.error;
        snapshot.http_headers_ready = task.http.headers_ready;
        if (task.http.source && !task.send.info_sent &&
            !task.http.response_meta_payload.empty()) {
          snapshot.http_meta_payload = task.http.response_meta_payload;
        }
      }

      if (snapshot.http_fetch_pending && !snapshot.http_headers_ready) {
        continue;
      }

      if (snapshot.http_failed) {
        std::vector<uint8_t> status_payload(snapshot.http_error.begin(),
                                            snapshot.http_error.end());
        const auto status_ts = nextRelativeTimestampMs();
        TaskState status_task;
        status_task.task_id = task_id;
        status_task.source.file_name = snapshot.file_name;
        status_task.source.file_size = snapshot.file_size;
        auto status_header =
            makePacketHeader(status_task, EsFilePacketType::TaskStatus, 0,
                             static_cast<uint32_t>(status_payload.size()), 0,
                             snapshot.next_seq, status_ts);
        auto status_packet =
            buildPacket(status_task, status_header, status_payload);
        if (!emitPacket(task_id, std::move(status_packet), status_header)) {
          continue;
        }
        ++packet_count;
        std::lock_guard<std::mutex> lock(_mtx);
        auto it = _task_registry.tasks.find(task_id);
        if (it != _task_registry.tasks.end()) {
          it->second.send.info_sent = true;
          it->second.send.end_sent = true;
          it->second.send.next_seq++;
          recomputeAllTaskRateProfilesLocked(false);
        }
        continue;
      }

      if (!snapshot.info_sent) {
        std::vector<uint8_t> info_payload;
        uint16_t info_flags = 0;
        if (!snapshot.http_meta_payload.empty()) {
          info_payload = std::move(snapshot.http_meta_payload);
          info_flags = kEsFileFlagFileInfoHasHttpResponseHeaders;
        }
        const auto info_ts = nextRelativeTimestampMs();
        TaskState info_task;
        info_task.task_id = task_id;
        info_task.source.file_name = snapshot.file_name;
        info_task.source.file_size = snapshot.file_size;
        auto info_header = makePacketHeader(
            info_task, EsFilePacketType::FileInfo, 0,
            static_cast<uint32_t>(info_payload.size()), info_flags,
            snapshot.next_seq, info_ts);
        auto info_packet = buildPacket(info_task, info_header, info_payload);
        if (!emitPacket(task_id, std::move(info_packet), info_header)) {
          continue;
        }
        ++packet_count;
        std::lock_guard<std::mutex> lock(_mtx);
        auto it = _task_registry.tasks.find(task_id);
        if (it != _task_registry.tasks.end()) {
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
          if (it == _task_registry.tasks.end() || it->second.send.end_sent) {
            break;
          }
          auto &task = it->second;
          memory_mode = task.source.memory_mode;
          http_chunk_mode = task.http.source;
          generation = task.generation;
          offset = task.send.sent_bytes;
          seq = task.send.next_seq;
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
              const bool http_stream_finished = !task.http.queued && !task.http.active;
              const bool allow_tail_emit =
                  http_stream_finished &&
                  min_emit_payload_bytes > 0 &&
                  buffered <= min_emit_payload_bytes;
              if (min_emit_payload_bytes > 0 &&
                  emit_token_quota < min_emit_payload_bytes &&
                  !allow_tail_emit) {
                break;
              }
              if (min_emit_payload_bytes > 0 &&
                  buffered < min_emit_payload_bytes &&
                  !allow_tail_emit) {
                break;
              }
              read_len = static_cast<size_t>(std::min<uint64_t>(
                  std::min<uint64_t>(
                      std::min<uint64_t>(quota, _global_options.packet_chunk_bytes),
                      emit_token_quota),
                  buffered));
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
                  consumeTokenBucketBytes(task.control.emit_bucket, read_len);
                  task.send.sent_bytes += read_len;
                  task.send.next_seq++;
                  quota -= read_len;
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
              const bool allow_tail_emit = remain <= min_emit_payload_bytes;
              if (min_emit_payload_bytes > 0 &&
                  emit_token_quota < min_emit_payload_bytes &&
                  !allow_tail_emit) {
                break;
              }
              read_len = static_cast<size_t>(std::min<uint64_t>(
                  std::min<uint64_t>(
                      std::min<uint64_t>(quota, _global_options.packet_chunk_bytes),
                                     emit_token_quota),
                  remain));
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
                    quota -= read_len;
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
        auto it = _task_registry.tasks.find(task_id);
        const bool ready_to_finish =
            it != _task_registry.tasks.end() && isTaskReadyToEmitEnd(it->second);
        if (ready_to_finish) {
          should_end = true;
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
          recomputeAllTaskRateProfilesLocked(false);
        }
      }
      if (should_end) {
        const auto end_ts = nextRelativeTimestampMs();
        auto end_header =
            makePacketHeader(end_task, EsFilePacketType::FileEnd, end_offset,
                             0, 0, end_seq, end_ts);
        auto end_packet = buildPacket(end_task, end_header, {});
        if (emitPacket(task_id, std::move(end_packet), end_header)) {
          ++packet_count;
        }
      }
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
  while (_http_runtime.active_fetches < _global_options.http_pull_concurrency_limit &&
         !_http_runtime.pending_fetches.empty()) {
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
    task.http.queued = false;
    task.http.active = true;
    ++_http_runtime.active_fetches;
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
    auto it = _task_registry.tasks.find(task_id);
    if (it == _task_registry.tasks.end() || it->second.generation != generation) {
      if (_http_runtime.active_fetches > 0) {
        --_http_runtime.active_fetches;
      }
      return;
    }
    source = it->second.source.file_path;
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
              bool waited_for_buffer = false;
              bool waited_for_rate = false;
              const auto wait_begin = std::chrono::steady_clock::now();
              while (true) {
                auto it = _task_registry.tasks.find(task_id);
                if (it == _task_registry.tasks.end() || it->second.generation != generation) {
                  return false;
                }
                auto &task = it->second;
                auto buffer_cv = it->second.http.buffer.cv;
                if (!buffer_cv) {
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
                    _global_options.http_pull_total_buffer_limit_bytes) {
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
                    _global_options.http_pull_total_buffer_limit_bytes >
                            _http_runtime.total_buffered_bytes
                        ? _global_options.http_pull_total_buffer_limit_bytes -
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
        [this, task_id, generation](uint32_t status_code,
                                    const HttpHeaders &headers_in) {
          uint64_t content_length = 0;
          const bool has_content_length =
              tryParseContentLength(headers_in, content_length);
          std::lock_guard<std::mutex> lock(_mtx);
          auto it = _task_registry.tasks.find(task_id);
          if (it == _task_registry.tasks.end() || it->second.generation != generation) {
            return;
          }
          auto &task = it->second;
          const auto inferred_type = inferTaskTypeFromResponse(
              task.control.task_type, task.source.file_path, status_code, headers_in);
          applyTaskProfileLocked(task, inferred_type, false);
        recomputeAllTaskRateProfilesLocked(false);
          task.http.status_code = status_code;
          task.http.response_meta_payload =
              buildHttpResponseMetaPayload(status_code, headers_in);
          task.http.headers_ready = true;
          if (has_content_length) {
            task.source.file_size = content_length;
            task.http.size_known = true;
          }
          _packet_runtime.packet_sem.post();
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
        task.http.status_code = response_status_code;
        task.http.response_meta_payload =
            buildHttpResponseMetaPayload(response_status_code, response_headers);
        task.http.headers_ready = true;
        if (!task.http.size_known) {
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
    _packet_runtime.packet_sem.post();
  }).detach();
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

size_t EsFileFerryPacker::taskTypeToIndex(EsFileTaskType type) {
  return static_cast<size_t>(type);
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

// Task Strategy Helpers

void EsFileFerryPacker::setLastError(const std::string &err) {
  std::lock_guard<std::mutex> lock(_mtx);
  _packet_runtime.last_error = err;
}

EsFileTaskType EsFileFerryPacker::inferTaskTypeFromRequest(
    const std::string &url, const std::string &method_upper,
    const HttpHeaders &headers, const std::string &body) {
  if (method_upper == "POST" || !body.empty()) {
    return EsFileTaskType::HttpApiRequest;
  }
  if (headerValueContains(headers, "range", "bytes=") ||
      headerValueContains(headers, "accept", "video/mp4")) {
    return EsFileTaskType::HttpMp4Vod;
  }
  if (headerValueContains(headers, "accept", "application/json") ||
      headerValueContains(headers, "accept", "text/")) {
    return EsFileTaskType::HttpApiRequest;
  }
  auto lower = toLowerCopy(url);
  if (lower.find(".mp4") != std::string::npos) {
    return EsFileTaskType::HttpMp4Vod;
  }
  return EsFileTaskType::HttpResourceDownload;
}

EsFileTaskType EsFileFerryPacker::inferTaskTypeFromResponse(
    EsFileTaskType current_type, const std::string &url, uint32_t status_code,
    const HttpHeaders &headers) {
  if (headerValueContains(headers, "content-disposition", "attachment")) {
    return EsFileTaskType::HttpResourceDownload;
  }
  if (headerValueContains(headers, "content-type", "application/json") ||
      headerValueContains(headers, "content-type", "text/") ||
      headerValueContains(headers, "content-type", "application/xml")) {
    return EsFileTaskType::HttpApiRequest;
  }
  if (headerValueContains(headers, "content-type", "video/mp4") ||
      headerValueContains(headers, "content-type", "application/mp4") ||
      headerValueContains(headers, "content-range", "bytes") ||
      headerValueContains(headers, "accept-ranges", "bytes") ||
      status_code == 206) {
    return EsFileTaskType::HttpMp4Vod;
  }
  auto lower = toLowerCopy(url);
  if (lower.find(".mp4") != std::string::npos) {
    return EsFileTaskType::HttpMp4Vod;
  }
  return current_type;
}

void EsFileFerryPacker::applyTaskProfileLocked(TaskState &task,
                                               EsFileTaskType task_type,
                                               bool reset_buckets) {
  const auto now = std::chrono::steady_clock::now();
  task.control.task_type = task_type;
  task.control.priority = inferPriorityFromType(task_type);
  task.control.max_buffered_bytes = inferDefaultBufferedBytes(task_type);
  task.control.resume_buffered_bytes = task.control.max_buffered_bytes / 2;

  const auto fetch_burst =
      inferDefaultBurstBytes(task_type, _global_options.packet_chunk_bytes);
  const auto emit_burst =
      inferDefaultBurstBytes(task_type, _global_options.packet_chunk_bytes);
  task.control.fetch_bucket.rate_bps = 0;
  task.control.fetch_bucket.burst_bytes = fetch_burst;
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

uint32_t EsFileFerryPacker::rateShareForTaskType(EsFileTaskType type) const {
  switch (type) {
  case EsFileTaskType::HttpApiRequest:
    return _global_options.http_api_rate_share;
  case EsFileTaskType::HttpMp4Vod:
    return _global_options.http_mp4_rate_share;
  case EsFileTaskType::HttpResourceDownload:
  default:
    return _global_options.http_download_rate_share;
  }
}

void EsFileFerryPacker::recomputeAllTaskRateProfilesLocked(bool reset_buckets) {
  std::array<size_t, 3> active_counts{{0, 0, 0}};
  size_t active_type_count = 0;
  uint32_t active_share_sum = 0;
  for (auto &item : _task_registry.tasks) {
    auto &task = item.second;
    if (task.send.end_sent) {
      continue;
    }
    ++active_counts[taskTypeToIndex(task.control.task_type)];
  }
  for (size_t i = 0; i < active_counts.size(); ++i) {
    if (active_counts[i] == 0) {
      continue;
    }
    ++active_type_count;
    active_share_sum += rateShareForTaskType(static_cast<EsFileTaskType>(i));
  }
  for (auto &item : _task_registry.tasks) {
    refreshTaskRateBucketsLocked(item.second, active_counts, active_type_count,
                                 active_share_sum, reset_buckets);
  }
}

void EsFileFerryPacker::refreshTaskRateBucketsLocked(
    TaskState &task, const std::array<size_t, 3> &active_counts,
    size_t active_type_count, uint32_t active_share_sum, bool reset_buckets) {
  const auto now = std::chrono::steady_clock::now();
  const auto task_type = task.control.task_type;
  const auto type_idx = taskTypeToIndex(task_type);
  const auto active_count = active_counts[type_idx];
  uint64_t task_rate_bps = 0;

  if (!task.send.end_sent && active_count > 0) {
    uint64_t type_rate_bps = 0;
    if (_global_options.http_pull_total_rate_bps > 0) {
      if (active_share_sum > 0) {
        type_rate_bps = (_global_options.http_pull_total_rate_bps *
                         rateShareForTaskType(task_type)) /
                        active_share_sum;
      } else if (active_type_count > 0) {
        type_rate_bps =
            _global_options.http_pull_total_rate_bps / active_type_count;
      }
    }
    task_rate_bps = type_rate_bps / active_count;
  }

  task.control.fetch_bucket.rate_bps = task.http.source ? task_rate_bps : 0;
  task.control.emit_bucket.rate_bps = task_rate_bps;
  task.control.fetch_bucket.burst_bytes =
      inferDefaultBurstBytes(task_type, _global_options.packet_chunk_bytes);
  task.control.emit_bucket.burst_bytes =
      inferDefaultBurstBytes(task_type, _global_options.packet_chunk_bytes);

  if (reset_buckets ||
      task.control.fetch_bucket.last_refill ==
          std::chrono::steady_clock::time_point{}) {
    task.control.fetch_bucket.tokens = task.control.fetch_bucket.burst_bytes;
    task.control.fetch_bucket.last_refill = now;
  } else {
    task.control.fetch_bucket.tokens = std::min(
        task.control.fetch_bucket.tokens, task.control.fetch_bucket.burst_bytes);
  }
  if (reset_buckets ||
      task.control.emit_bucket.last_refill ==
          std::chrono::steady_clock::time_point{}) {
    task.control.emit_bucket.tokens = task.control.emit_bucket.burst_bytes;
    task.control.emit_bucket.last_refill = now;
  } else {
    task.control.emit_bucket.tokens = std::min(
        task.control.emit_bucket.tokens, task.control.emit_bucket.burst_bytes);
  }
}

void EsFileFerryPacker::refillTokenBucket(TokenBucket &bucket) {
  if (bucket.rate_bps == 0) {
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
  if (bucket.rate_bps == 0) {
    return UINT64_MAX;
  }
  return bucket.tokens;
}

void EsFileFerryPacker::consumeTokenBucketBytes(TokenBucket &bucket,
                                                uint64_t bytes) {
  if (bucket.rate_bps == 0) {
    return;
  }
  bucket.tokens = bytes >= bucket.tokens ? 0 : bucket.tokens - bytes;
}

// Scheduling Helpers

std::chrono::milliseconds EsFileFerryPacker::estimateTokenWait(
    const TokenBucket &bucket, uint64_t min_bytes) {
  if (bucket.rate_bps == 0 || bucket.tokens >= min_bytes) {
    return std::chrono::milliseconds(0);
  }
  const auto deficit = min_bytes - bucket.tokens;
  const auto wait_ms = (deficit * 1000 + bucket.rate_bps - 1) / bucket.rate_bps;
  return std::chrono::milliseconds(wait_ms == 0 ? 1 : wait_ms);
}

std::array<std::vector<std::string>, 3>
EsFileFerryPacker::snapshotActiveTasksByPriority() const {
  std::lock_guard<std::mutex> lock(_mtx);
  std::array<std::vector<std::string>, 3> groups;
  for (const auto &it : _task_registry.tasks) {
    if (it.second.send.end_sent) {
      continue;
    }
    groups[priorityToIndex(it.second.control.priority)].emplace_back(it.first);
  }
  return groups;
}

std::vector<std::string> EsFileFerryPacker::pickFairRound(
    const std::vector<std::string> &active_ids, EsFileTaskPriority priority) {
  if (active_ids.size() <= 1) {
    return active_ids;
  }
  std::vector<std::string> sorted = active_ids;
  std::sort(sorted.begin(), sorted.end());
  if (sorted.empty()) {
    return sorted;
  }
  std::lock_guard<std::mutex> lock(_mtx);
  auto &cursor = _task_registry.priority_rr_cursor[priorityToIndex(priority)];
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

uint64_t EsFileFerryPacker::computePriorityQuota(
    uint64_t total_payload_quota_bytes, EsFileTaskPriority priority,
    const std::array<std::vector<std::string>, 3> &active_groups) const {
  uint64_t total_weight = 0;
  for (size_t i = 0; i < active_groups.size(); ++i) {
    if (!active_groups[i].empty()) {
      total_weight += rateShareForTaskType(static_cast<EsFileTaskType>(i));
    }
  }
  if (total_weight == 0) {
    return 0;
  }
  const auto idx = priorityToIndex(priority);
  if (active_groups[idx].empty()) {
    return 0;
  }
  uint64_t quota =
      (total_payload_quota_bytes *
       rateShareForTaskType(static_cast<EsFileTaskType>(idx))) /
      total_weight;
  if (priority == EsFileTaskPriority::High) {
    uint64_t distributed = 0;
    for (size_t i = 0; i < active_groups.size(); ++i) {
      if (!active_groups[i].empty()) {
        distributed +=
            (total_payload_quota_bytes *
             rateShareForTaskType(static_cast<EsFileTaskType>(i))) /
            total_weight;
      }
    }
    quota += total_payload_quota_bytes - distributed;
  }
  return quota;
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
