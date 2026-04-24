#include "HttpFetchEngine.h"
#include <utility>

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
