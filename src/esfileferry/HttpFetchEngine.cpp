#include "HttpFetchEngine.h"
#include <utility>

namespace {
void detachIfSelfJoin(std::thread &thread) {
  if (!thread.joinable()) {
    return;
  }
  if (thread.get_id() == std::this_thread::get_id()) {
    thread.detach();
  }
}
} // namespace

void ThreadedHttpFetchEngine::submit(Request request) {
  auto done = std::make_shared<std::atomic_bool>(false);
  auto cancelled = std::make_shared<std::atomic_bool>(false);
  const auto task_id = request.task_id;
  const auto generation = request.generation;
  request.should_abort = [cancelled, should_abort = request.should_abort]() {
    if (cancelled->load(std::memory_order_acquire)) {
      return true;
    }
    return should_abort && should_abort();
  };
  auto request_holder = std::make_shared<Request>(std::move(request));
  std::thread worker([request_holder, done]() {
    const auto &request = *request_holder;
    HttpHeaders response_headers;
    uint32_t response_status_code = 0;
    std::string fetch_err;
    TransferDiagnostics diagnostics;
    const bool ok = HttpStreamFetcher::stream(
        request.url, request.method, request.headers, request.body,
        request.on_chunk, request.on_headers, request.should_abort, &response_headers,
        &response_status_code, fetch_err, &diagnostics);
    if (request.on_complete) {
      request.on_complete(ok, response_headers, response_status_code, fetch_err,
                          diagnostics);
    }
    done->store(true, std::memory_order_release);
  });
  std::lock_guard<std::mutex> lock(_mtx);
  _threads.emplace_back(
      std::unique_ptr<ThreadState>(new ThreadState(
          std::move(worker), done, cancelled, task_id, generation)));
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
  bool cancelled = false;
  std::lock_guard<std::mutex> lock(_mtx);
  for (auto &thread_state : _threads) {
    if (!thread_state || !thread_state->cancelled || !thread_state->done) {
      continue;
    }
    if (thread_state->task_id != task_id || thread_state->generation != generation) {
      continue;
    }
    if (thread_state->done->load(std::memory_order_acquire)) {
      continue;
    }
    thread_state->cancelled->store(true, std::memory_order_release);
    cancelled = true;
  }
  return cancelled;
}

void ThreadedHttpFetchEngine::reapCompleted(std::vector<std::thread> &join_threads) {
  std::lock_guard<std::mutex> lock(_mtx);
  auto it = _threads.begin();
  while (it != _threads.end()) {
    auto &thread_state = *it;
    if (thread_state && thread_state->done &&
        thread_state->done->load(std::memory_order_acquire)) {
      if (thread_state->thread.joinable()) {
        if (thread_state->thread.get_id() == std::this_thread::get_id()) {
          detachIfSelfJoin(thread_state->thread);
        } else {
          join_threads.emplace_back(std::move(thread_state->thread));
        }
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
      if (thread_state->thread.get_id() == std::this_thread::get_id()) {
        detachIfSelfJoin(thread_state->thread);
      } else {
        join_threads.emplace_back(std::move(thread_state->thread));
      }
    }
  }
  _threads.clear();
}
