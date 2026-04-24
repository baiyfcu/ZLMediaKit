#include "HttpFetchEngine.h"
#include <algorithm>
#include <condition_variable>
#include <cstdlib>
#include <curl/curl.h>
#include <deque>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace {

constexpr float kDefaultHttpStreamTimeoutSec = 0.0f;
constexpr long kDefaultHttpConnectTimeoutMs = 30 * 1000;
constexpr long kDefaultCurlBufferSize = 512 * 1024;
constexpr int kCurlMultiWaitTimeoutMs = 100;

std::string toLowerCopy(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(),
                 [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  return value;
}

std::string trimAsciiWhitespace(std::string value) {
  auto is_space = [](unsigned char c) {
    return c == ' ' || c == '\t' || c == '\r' || c == '\n';
  };
  while (!value.empty() && is_space(static_cast<unsigned char>(value.front()))) {
    value.erase(value.begin());
  }
  while (!value.empty() && is_space(static_cast<unsigned char>(value.back()))) {
    value.pop_back();
  }
  return value;
}

bool isBlockedRequestHeader(const std::string &key_lower) {
  static const std::unordered_set<std::string> blocked = {
      "host",              "content-length", "connection", "keep-alive",
      "proxy-connection",  "transfer-encoding", "te",      "trailer",
      "upgrade"};
  return blocked.find(key_lower) != blocked.end();
}

HttpFetchEngine::HttpHeaders makeEffectiveHeaders(
    const HttpFetchEngine::HttpHeaders &headers) {
  HttpFetchEngine::HttpHeaders out;
  out.reserve(headers.size() + 4);
  bool has_user_agent = false;
  bool has_accept = false;
  bool has_accept_encoding = false;
  for (const auto &header : headers) {
    const auto key_lower = toLowerCopy(header.first);
    if (key_lower.empty() || isBlockedRequestHeader(key_lower)) {
      continue;
    }
    if (key_lower == "user-agent") {
      has_user_agent = true;
    } else if (key_lower == "accept") {
      has_accept = true;
    } else if (key_lower == "accept-encoding") {
      has_accept_encoding = true;
    }
    out.emplace_back(header.first, header.second);
  }
  if (!has_user_agent) {
    out.emplace_back("User-Agent", "GbTransfer-Ferry/1.0");
  }
  if (!has_accept) {
    out.emplace_back("Accept", "*/*");
  }
  if (!has_accept_encoding) {
    out.emplace_back("Accept-Encoding", "identity");
  }
  return out;
}

bool isInterimStatusCode(uint32_t status_code) {
  return status_code >= 100 && status_code < 200;
}

struct CurlRequestContext {
  HttpFetchEngine::OnChunk on_chunk;
  HttpFetchEngine::OnHeaders on_headers;
  HttpFetchEngine::HttpHeaders response_headers;
  HttpFetchEngine::HttpHeaders pending_headers;
  uint32_t status_code = 0;
  uint32_t pending_status_code = 0;
  bool headers_emitted = false;
  bool consumer_ok = true;
  std::string consumer_err;

  void resetPendingHeaders(uint32_t status) {
    pending_status_code = status;
    pending_headers.clear();
  }

  void emitHeadersIfReady() {
    if (headers_emitted || pending_status_code == 0 ||
        isInterimStatusCode(pending_status_code)) {
      return;
    }
    status_code = pending_status_code;
    response_headers = pending_headers;
    headers_emitted = true;
    if (on_headers) {
      on_headers(status_code, response_headers);
    }
  }
};

void ensureCurlGlobalInit() {
  static std::once_flag once;
  std::call_once(once, []() { curl_global_init(CURL_GLOBAL_DEFAULT); });
}

size_t onCurlHeader(char *buffer, size_t size, size_t nitems, void *userdata) {
  const auto bytes = size * nitems;
  auto *ctx = static_cast<CurlRequestContext *>(userdata);
  if (!ctx || !buffer || bytes == 0) {
    return bytes;
  }

  std::string line(buffer, bytes);
  const auto trimmed = trimAsciiWhitespace(line);
  if (trimmed.empty()) {
    ctx->emitHeadersIfReady();
    return bytes;
  }

  if (trimmed.rfind("HTTP/", 0) == 0) {
    const auto first_space = trimmed.find(' ');
    if (first_space != std::string::npos) {
      const auto second_space = trimmed.find(' ', first_space + 1);
      const auto code_text =
          trimmed.substr(first_space + 1,
                         second_space == std::string::npos
                             ? std::string::npos
                             : second_space - first_space - 1);
      const auto parsed = std::strtoul(code_text.c_str(), nullptr, 10);
      ctx->resetPendingHeaders(static_cast<uint32_t>(parsed));
    }
    return bytes;
  }

  const auto colon = trimmed.find(':');
  if (colon == std::string::npos) {
    return bytes;
  }
  auto key = trimAsciiWhitespace(trimmed.substr(0, colon));
  if (key.empty()) {
    return bytes;
  }
  auto value = trimAsciiWhitespace(trimmed.substr(colon + 1));
  ctx->pending_headers.emplace_back(std::move(key), std::move(value));
  return bytes;
}

size_t onCurlWrite(char *ptr, size_t size, size_t nmemb, void *userdata) {
  const auto bytes = size * nmemb;
  auto *ctx = static_cast<CurlRequestContext *>(userdata);
  if (!ctx || !ptr || bytes == 0) {
    return bytes;
  }

  ctx->emitHeadersIfReady();
  if (!ctx->on_chunk) {
    return bytes;
  }
  if (!ctx->on_chunk(reinterpret_cast<const uint8_t *>(ptr), bytes)) {
    ctx->consumer_ok = false;
    if (ctx->consumer_err.empty()) {
      ctx->consumer_err = "consume http payload failed";
    }
    return 0;
  }
  return bytes;
}

curl_slist *buildCurlHeaderList(const HttpFetchEngine::HttpHeaders &headers) {
  curl_slist *list = nullptr;
  for (const auto &header : headers) {
    const auto line = header.first + ": " + header.second;
    auto *next = curl_slist_append(list, line.c_str());
    if (!next) {
      curl_slist_free_all(list);
      return nullptr;
    }
    list = next;
  }
  return list;
}

std::string mapCurlError(CURLcode code, const CurlRequestContext &ctx) {
  if (!ctx.consumer_ok && !ctx.consumer_err.empty()) {
    return ctx.consumer_err;
  }
  return curl_easy_strerror(code);
}

double getCurlTimeMs(CURL *curl, CURLINFO info) {
  if (!curl) {
    return 0.0;
  }
  double seconds = 0.0;
  if (curl_easy_getinfo(curl, info, &seconds) != CURLE_OK || seconds < 0.0) {
    return 0.0;
  }
  return seconds * 1000.0;
}

double getCurlDoubleInfo(CURL *curl, CURLINFO info) {
  if (!curl) {
    return 0.0;
  }
  double value = 0.0;
  if (curl_easy_getinfo(curl, info, &value) != CURLE_OK || value < 0.0) {
    return 0.0;
  }
  return value;
}

bool setupCurlRequest(CURL *curl, const std::string &url,
                      const std::string &method, const std::string &body,
                      curl_slist *request_headers, CurlRequestContext *ctx,
                      std::string &err) {
  if (!curl || !ctx) {
    err = "curl init failed";
    return false;
  }

  curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
  curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS,
                   kDefaultHttpConnectTimeoutMs);
  if (kDefaultHttpStreamTimeoutSec > 0.0f) {
    curl_easy_setopt(
        curl, CURLOPT_TIMEOUT_MS,
        static_cast<long>(kDefaultHttpStreamTimeoutSec * 1000.0f));
  }
  curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, &onCurlHeader);
  curl_easy_setopt(curl, CURLOPT_HEADERDATA, ctx);
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, &onCurlWrite);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, ctx);
  curl_easy_setopt(curl, CURLOPT_BUFFERSIZE, kDefaultCurlBufferSize);
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, request_headers);

  if (method == "GET") {
    curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
    return true;
  }
  if (method == "POST") {
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.data());
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE_LARGE,
                     static_cast<curl_off_t>(body.size()));
    return true;
  }

  err = "unsupported http method: " + method;
  return false;
}

struct MultiHandleState {
  explicit MultiHandleState(HttpFetchEngine::Request request_in)
      : request(std::move(request_in)) {}

  HttpFetchEngine::Request request;
  HttpFetchEngine::HttpHeaders effective_headers;
  CurlRequestContext ctx;
  CURL *easy = nullptr;
  curl_slist *request_headers = nullptr;
  bool paused = false;
};

bool requestMatches(const HttpFetchEngine::Request &request,
                    const std::string &task_id, uint64_t generation) {
  return request.task_id == task_id && request.generation == generation;
}

bool handleMatches(const MultiHandleState &state, const std::string &task_id,
                   uint64_t generation) {
  return requestMatches(state.request, task_id, generation);
}

void completeRequestImmediately(HttpFetchEngine::Request &request,
                                const std::string &err) {
  if (!request.on_complete) {
    return;
  }
  HttpFetchEngine::TransferDiagnostics diagnostics;
  request.on_complete(false, HttpFetchEngine::HttpHeaders{}, 0, err, diagnostics);
}

void populateDiagnostics(CURL *easy, const CurlRequestContext &ctx,
                         HttpFetchEngine::TransferDiagnostics &diagnostics) {
  diagnostics.status_code = ctx.status_code;
  diagnostics.headers_emitted = ctx.headers_emitted;
  diagnostics.name_lookup_ms = getCurlTimeMs(easy, CURLINFO_NAMELOOKUP_TIME);
  diagnostics.connect_ms = getCurlTimeMs(easy, CURLINFO_CONNECT_TIME);
  diagnostics.app_connect_ms = getCurlTimeMs(easy, CURLINFO_APPCONNECT_TIME);
  diagnostics.pretransfer_ms = getCurlTimeMs(easy, CURLINFO_PRETRANSFER_TIME);
  diagnostics.starttransfer_ms = getCurlTimeMs(easy, CURLINFO_STARTTRANSFER_TIME);
  diagnostics.total_ms = getCurlTimeMs(easy, CURLINFO_TOTAL_TIME);
  diagnostics.download_bytes = getCurlDoubleInfo(easy, CURLINFO_SIZE_DOWNLOAD);
  diagnostics.content_length_bytes =
      getCurlDoubleInfo(easy, CURLINFO_CONTENT_LENGTH_DOWNLOAD);
  diagnostics.download_speed_bytes_per_sec =
      getCurlDoubleInfo(easy, CURLINFO_SPEED_DOWNLOAD);
}

} // namespace

void ThreadedHttpFetchEngine::submit(Request request) {
  auto done = std::make_shared<std::atomic_bool>(false);
  auto request_holder = std::make_shared<Request>(std::move(request));
  std::thread worker([request_holder, done]() {
    const auto &request = *request_holder;
    HttpHeaders response_headers;
    uint32_t response_status_code = 0;
    std::string fetch_err;
    TransferDiagnostics diagnostics;
    const bool ok = HttpStreamFetcher::stream(
        request.url, request.method, request.headers, request.body,
        request.on_chunk, request.on_headers, &response_headers,
        &response_status_code, fetch_err, &diagnostics);
    if (request.on_complete) {
      request.on_complete(ok, response_headers, response_status_code, fetch_err,
                          diagnostics);
    }
    done->store(true, std::memory_order_release);
  });
  std::lock_guard<std::mutex> lock(_mtx);
  _threads.emplace_back(
      std::unique_ptr<ThreadState>(new ThreadState(std::move(worker), done)));
}

bool ThreadedHttpFetchEngine::pauseTask(const std::string &task_id,
                                        uint64_t generation) {
  (void)task_id;
  (void)generation;
  return false;
}

bool ThreadedHttpFetchEngine::resumeTask(const std::string &task_id,
                                         uint64_t generation) {
  (void)task_id;
  (void)generation;
  return false;
}

bool ThreadedHttpFetchEngine::cancelTask(const std::string &task_id,
                                         uint64_t generation) {
  (void)task_id;
  (void)generation;
  return false;
}

void ThreadedHttpFetchEngine::reapCompleted(std::vector<std::thread> &join_threads) {
  std::lock_guard<std::mutex> lock(_mtx);
  auto it = _threads.begin();
  while (it != _threads.end()) {
    auto &thread_state = *it;
    if (thread_state && thread_state->done &&
        thread_state->done->load(std::memory_order_acquire)) {
      if (thread_state->thread.joinable()) {
        join_threads.emplace_back(std::move(thread_state->thread));
      }
      it = _threads.erase(it);
      continue;
    }
    ++it;
  }
}

void ThreadedHttpFetchEngine::shutdown(std::vector<std::thread> &join_threads) {
  std::lock_guard<std::mutex> lock(_mtx);
  for (auto &thread_state : _threads) {
    if (thread_state && thread_state->thread.joinable()) {
      join_threads.emplace_back(std::move(thread_state->thread));
    }
  }
  _threads.clear();
}

struct CurlMultiHttpFetchEngine::Impl {
  struct ControlAction {
    enum class Type {
      Pause,
      Resume,
      Cancel,
    };

    Type type;
    std::string task_id;
    uint64_t generation = 0;
  };

  struct WorkerState {
    std::mutex mtx;
    std::condition_variable cv;
    std::deque<HttpFetchEngine::Request> pending_requests;
    std::deque<ControlAction> control_actions;
    std::unordered_map<CURL *, std::unique_ptr<MultiHandleState>> active_handles;
    std::thread worker;
    bool worker_started = false;
    bool shutting_down = false;

    void ensureWorkerStarted() {
      if (worker_started) {
        return;
      }
      worker_started = true;
      worker = std::thread([this]() { run(); });
    }

    void run() {
      ensureCurlGlobalInit();
      CURLM *multi = curl_multi_init();
      if (!multi) {
        failPendingRequests("curl_multi_init failed");
        return;
      }

      int running_handles = 0;
      while (true) {
        applyControlActions(multi, running_handles);
        drainPendingRequests(multi, running_handles);
        applyControlActions(multi, running_handles);

        int still_running = 0;
        auto multi_code = curl_multi_perform(multi, &still_running);
        while (multi_code == CURLM_CALL_MULTI_PERFORM) {
          multi_code = curl_multi_perform(multi, &still_running);
        }
        running_handles = still_running;

        collectCompletedHandles(multi, running_handles);

        {
          std::unique_lock<std::mutex> lock(mtx);
          if (shutting_down && pending_requests.empty() &&
              control_actions.empty() && active_handles.empty()) {
            break;
          }
        }

        if (running_handles > 0) {
          int numfds = 0;
          curl_multi_wait(multi, nullptr, 0, kCurlMultiWaitTimeoutMs, &numfds);
        } else {
          std::unique_lock<std::mutex> lock(mtx);
          cv.wait_for(lock, std::chrono::milliseconds(kCurlMultiWaitTimeoutMs),
                      [this]() {
                        return shutting_down || !pending_requests.empty() ||
                               !control_actions.empty() ||
                               !active_handles.empty();
                      });
        }
      }

      cleanupAllHandles(multi);
      curl_multi_cleanup(multi);
    }

    void failPendingRequests(const std::string &err) {
      std::deque<HttpFetchEngine::Request> requests;
      {
        std::lock_guard<std::mutex> lock(mtx);
        requests.swap(pending_requests);
      }
      while (!requests.empty()) {
        auto request = std::move(requests.front());
        requests.pop_front();
        if (request.on_complete) {
          completeRequestImmediately(request, err);
        }
      }
    }

    bool queueControlAction(ControlAction::Type type, const std::string &task_id,
                            uint64_t generation) {
      std::lock_guard<std::mutex> lock(mtx);
      if (shutting_down) {
        return false;
      }

      bool found = false;
      if (type == ControlAction::Type::Cancel) {
        for (const auto &request : pending_requests) {
          if (requestMatches(request, task_id, generation)) {
            found = true;
            break;
          }
        }
      }
      if (!found) {
        for (const auto &entry : active_handles) {
          if (entry.second &&
              handleMatches(*entry.second, task_id, generation)) {
            found = true;
            break;
          }
        }
      }
      if (!found) {
        return false;
      }

      ControlAction action;
      action.type = type;
      action.task_id = task_id;
      action.generation = generation;
      control_actions.push_back(std::move(action));
      cv.notify_one();
      return true;
    }

    void applyControlActions(CURLM *multi, int &running_handles) {
      std::deque<ControlAction> actions;
      {
        std::lock_guard<std::mutex> lock(mtx);
        actions.swap(control_actions);
      }

      while (!actions.empty()) {
        auto action = std::move(actions.front());
        actions.pop_front();
        if (action.type == ControlAction::Type::Cancel) {
          cancelMatchingRequests(multi, action.task_id, action.generation,
                                 running_handles);
          continue;
        }
        updatePauseState(action.type, action.task_id, action.generation);
      }
    }

    void cancelMatchingRequests(CURLM *multi, const std::string &task_id,
                                uint64_t generation, int &running_handles) {
      std::deque<HttpFetchEngine::Request> cancelled_pending;
      std::vector<std::unique_ptr<MultiHandleState>> cancelled_active;
      {
        std::lock_guard<std::mutex> lock(mtx);
        auto request_it = pending_requests.begin();
        while (request_it != pending_requests.end()) {
          if (requestMatches(*request_it, task_id, generation)) {
            cancelled_pending.emplace_back(std::move(*request_it));
            request_it = pending_requests.erase(request_it);
            continue;
          }
          ++request_it;
        }

        for (auto it = active_handles.begin(); it != active_handles.end();) {
          if (it->second && handleMatches(*it->second, task_id, generation)) {
            cancelled_active.emplace_back(std::move(it->second));
            it = active_handles.erase(it);
            if (running_handles > 0) {
              --running_handles;
            }
            continue;
          }
          ++it;
        }
      }

      while (!cancelled_pending.empty()) {
        auto request = std::move(cancelled_pending.front());
        cancelled_pending.pop_front();
        completeRequestImmediately(request, "http fetch cancelled");
      }

      for (auto &state : cancelled_active) {
        finishCancelledRequest(multi, std::move(state));
      }
    }

    void updatePauseState(ControlAction::Type type, const std::string &task_id,
                          uint64_t generation) {
      std::lock_guard<std::mutex> lock(mtx);
      for (auto &entry : active_handles) {
        auto &state = entry.second;
        if (!state || !state->easy ||
            !handleMatches(*state, task_id, generation)) {
          continue;
        }
        if (type == ControlAction::Type::Pause && !state->paused) {
          if (curl_easy_pause(state->easy, CURLPAUSE_RECV) == CURLE_OK) {
            state->paused = true;
          }
        } else if (type == ControlAction::Type::Resume && state->paused) {
          if (curl_easy_pause(state->easy, CURLPAUSE_CONT) == CURLE_OK) {
            state->paused = false;
          }
        }
      }
    }

    void finishCancelledRequest(CURLM *multi,
                                std::unique_ptr<MultiHandleState> state) {
      if (!state) {
        return;
      }
      if (state->easy) {
        curl_multi_remove_handle(multi, state->easy);
      }
      if (state->request.on_complete) {
        HttpFetchEngine::TransferDiagnostics diagnostics;
        if (state->easy) {
          populateDiagnostics(state->easy, state->ctx, diagnostics);
        }
        state->request.on_complete(false, state->ctx.response_headers,
                                   state->ctx.status_code,
                                   "http fetch cancelled", diagnostics);
      }
      if (state->request_headers) {
        curl_slist_free_all(state->request_headers);
        state->request_headers = nullptr;
      }
      if (state->easy) {
        curl_easy_cleanup(state->easy);
        state->easy = nullptr;
      }
    }

    void drainPendingRequests(CURLM *multi, int &running_handles) {
      std::deque<HttpFetchEngine::Request> requests;
      {
        std::lock_guard<std::mutex> lock(mtx);
        requests.swap(pending_requests);
      }
      while (!requests.empty()) {
        auto request = std::move(requests.front());
        requests.pop_front();

        std::unique_ptr<MultiHandleState> state(
            new MultiHandleState(std::move(request)));
        state->effective_headers = makeEffectiveHeaders(state->request.headers);
        state->ctx.on_chunk = state->request.on_chunk;
        state->ctx.on_headers = state->request.on_headers;
        state->easy = curl_easy_init();
        if (!state->easy) {
          completeSetupFailure(std::move(state), "curl_easy_init failed");
          continue;
        }
        state->request_headers = buildCurlHeaderList(state->effective_headers);
        if (!state->effective_headers.empty() && !state->request_headers) {
          completeSetupFailure(std::move(state), "build curl headers failed");
          continue;
        }

        std::string err;
        if (!setupCurlRequest(state->easy, state->request.url,
                              state->request.method, state->request.body,
                              state->request_headers, &state->ctx, err)) {
          completeSetupFailure(std::move(state), err);
          continue;
        }

        curl_easy_setopt(state->easy, CURLOPT_PRIVATE, state.get());
        const auto add_code = curl_multi_add_handle(multi, state->easy);
        if (add_code != CURLM_OK) {
          completeSetupFailure(std::move(state), curl_multi_strerror(add_code));
          continue;
        }

        {
          std::lock_guard<std::mutex> lock(mtx);
          active_handles.emplace(state->easy, std::move(state));
        }
        ++running_handles;
      }
    }

    void completeSetupFailure(std::unique_ptr<MultiHandleState> state,
                              const std::string &err) {
      if (!state) {
        return;
      }
      if (state->request.on_complete) {
        completeRequestImmediately(state->request, err);
      }
      if (state->request_headers) {
        curl_slist_free_all(state->request_headers);
        state->request_headers = nullptr;
      }
      if (state->easy) {
        curl_easy_cleanup(state->easy);
        state->easy = nullptr;
      }
    }

    void collectCompletedHandles(CURLM *multi, int &running_handles) {
      int pending = 0;
      CURLMsg *msg = nullptr;
      while ((msg = curl_multi_info_read(multi, &pending)) != nullptr) {
        if (msg->msg != CURLMSG_DONE || !msg->easy_handle) {
          continue;
        }

        std::unique_ptr<MultiHandleState> state;
        {
          std::lock_guard<std::mutex> lock(mtx);
          auto it = active_handles.find(msg->easy_handle);
          if (it != active_handles.end()) {
            state = std::move(it->second);
            active_handles.erase(it);
          }
        }
        if (!state) {
          curl_multi_remove_handle(multi, msg->easy_handle);
          if (running_handles > 0) {
            --running_handles;
          }
          continue;
        }

        finishRequest(multi, msg->data.result, state.get());
        if (state->request_headers) {
          curl_slist_free_all(state->request_headers);
          state->request_headers = nullptr;
        }
        if (state->easy) {
          curl_easy_cleanup(state->easy);
          state->easy = nullptr;
        }
        if (running_handles > 0) {
          --running_handles;
        }
      }
    }

    void finishRequest(CURLM *multi, CURLcode code, MultiHandleState *state) {
      if (!state || !state->easy) {
        return;
      }

      curl_multi_remove_handle(multi, state->easy);

      long response_code = 0;
      curl_easy_getinfo(state->easy, CURLINFO_RESPONSE_CODE, &response_code);
      if (response_code > 0 && state->ctx.pending_status_code == 0) {
        state->ctx.pending_status_code = static_cast<uint32_t>(response_code);
      }
      state->ctx.emitHeadersIfReady();
      if (state->ctx.status_code == 0 && response_code > 0) {
        state->ctx.status_code = static_cast<uint32_t>(response_code);
      }

      HttpFetchEngine::TransferDiagnostics diagnostics;
      populateDiagnostics(state->easy, state->ctx, diagnostics);

      std::string fetch_err;
      bool ok = true;
      if (!state->ctx.consumer_ok) {
        fetch_err = state->ctx.consumer_err.empty()
                        ? "consume http payload failed"
                        : state->ctx.consumer_err;
        ok = false;
      } else if (code != CURLE_OK) {
        fetch_err = mapCurlError(code, state->ctx);
        ok = false;
      } else if (state->ctx.status_code < 200 ||
                 state->ctx.status_code >= 300) {
        fetch_err = "http status " + std::to_string(state->ctx.status_code);
        ok = false;
      }

      if (state->request.on_complete) {
        state->request.on_complete(ok, state->ctx.response_headers,
                                   state->ctx.status_code, fetch_err,
                                   diagnostics);
      }
    }

    void cleanupAllHandles(CURLM *multi) {
      std::unordered_map<CURL *, std::unique_ptr<MultiHandleState>> handles;
      {
        std::lock_guard<std::mutex> lock(mtx);
        handles.swap(active_handles);
      }
      for (auto &entry : handles) {
        auto &state = entry.second;
        if (!state) {
          continue;
        }
        if (state->easy) {
          curl_multi_remove_handle(multi, state->easy);
        }
        completeSetupFailure(std::move(state), "http fetch engine shutdown");
      }
    }
  };

  std::vector<std::unique_ptr<WorkerState>> workers;

  Impl() {
    const auto worker_count = defaultWorkerCount();
    workers.reserve(worker_count);
    for (size_t i = 0; i < worker_count; ++i) {
      workers.emplace_back(new WorkerState());
    }
  }

  static size_t defaultWorkerCount() {
    const auto hw = std::thread::hardware_concurrency();
    if (hw == 0) {
      return 4;
    }
    const auto half = static_cast<size_t>((hw + 1) / 2);
    return std::max<size_t>(2, std::min<size_t>(8, half));
  }

  static size_t shardIndex(const std::string &task_id, uint64_t generation,
                           size_t shard_count) {
    if (shard_count <= 1) {
      return 0;
    }
    const auto key_hash = std::hash<std::string>{}(task_id);
    const auto generation_hash = std::hash<uint64_t>{}(generation);
    const auto mixed =
        key_hash ^ (generation_hash + 0x9e3779b97f4a7c15ULL +
                    (key_hash << 6) + (key_hash >> 2));
    return mixed % shard_count;
  }

  WorkerState *findWorker(const std::string &task_id, uint64_t generation) {
    if (workers.empty()) {
      return nullptr;
    }
    return workers[shardIndex(task_id, generation, workers.size())].get();
  }

  void submit(HttpFetchEngine::Request request) {
    auto *worker = findWorker(request.task_id, request.generation);
    if (!worker) {
      return;
    }
    {
      std::lock_guard<std::mutex> lock(worker->mtx);
      if (worker->shutting_down) {
        return;
      }
      worker->ensureWorkerStarted();
      worker->pending_requests.emplace_back(std::move(request));
    }
    worker->cv.notify_one();
  }

  bool pauseTask(const std::string &task_id, uint64_t generation) {
    auto *worker = findWorker(task_id, generation);
    return worker ? worker->queueControlAction(ControlAction::Type::Pause,
                                               task_id, generation)
                  : false;
  }

  bool resumeTask(const std::string &task_id, uint64_t generation) {
    auto *worker = findWorker(task_id, generation);
    return worker ? worker->queueControlAction(ControlAction::Type::Resume,
                                               task_id, generation)
                  : false;
  }

  bool cancelTask(const std::string &task_id, uint64_t generation) {
    auto *worker = findWorker(task_id, generation);
    return worker ? worker->queueControlAction(ControlAction::Type::Cancel,
                                               task_id, generation)
                  : false;
  }

  void shutdown(std::vector<std::thread> &join_threads) {
    for (auto &worker : workers) {
      if (!worker) {
        continue;
      }
      {
        std::lock_guard<std::mutex> lock(worker->mtx);
        worker->shutting_down = true;
      }
      worker->cv.notify_all();
      if (worker->worker.joinable()) {
        join_threads.emplace_back(std::move(worker->worker));
      }
    }
  }
};

CurlMultiHttpFetchEngine::CurlMultiHttpFetchEngine()
    : _impl(new Impl()) {}

CurlMultiHttpFetchEngine::~CurlMultiHttpFetchEngine() = default;

void CurlMultiHttpFetchEngine::submit(Request request) {
  if (!_impl) {
    return;
  }
  _impl->submit(std::move(request));
}

bool CurlMultiHttpFetchEngine::pauseTask(const std::string &task_id,
                                         uint64_t generation) {
  if (!_impl) {
    return false;
  }
  return _impl->pauseTask(task_id, generation);
}

bool CurlMultiHttpFetchEngine::resumeTask(const std::string &task_id,
                                          uint64_t generation) {
  if (!_impl) {
    return false;
  }
  return _impl->resumeTask(task_id, generation);
}

bool CurlMultiHttpFetchEngine::cancelTask(const std::string &task_id,
                                          uint64_t generation) {
  if (!_impl) {
    return false;
  }
  return _impl->cancelTask(task_id, generation);
}

void CurlMultiHttpFetchEngine::reapCompleted(std::vector<std::thread> &join_threads) {
  (void)join_threads;
}

void CurlMultiHttpFetchEngine::shutdown(std::vector<std::thread> &join_threads) {
  if (!_impl) {
    return;
  }
  _impl->shutdown(join_threads);
}
