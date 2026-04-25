#pragma once

#include "HttpStreamFetcher.h"
#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

class HttpFetchEngine {
public:
    using HttpHeaders = HttpStreamFetcher::HttpHeaders;
    using TransferDiagnostics = HttpStreamFetcher::TransferDiagnostics;
    using OnChunk = HttpStreamFetcher::OnChunk;
    using OnHeaders = HttpStreamFetcher::OnHeaders;
    using ShouldAbort = HttpStreamFetcher::ShouldAbort;

    struct Request {
        std::string task_id;
        uint64_t generation = 0;
        std::string url;
        std::string method;
        HttpHeaders headers;
        std::string body;
        OnChunk on_chunk;
        OnHeaders on_headers;
        ShouldAbort should_abort;
        std::function<void(bool ok,
                           const HttpHeaders &response_headers,
                           uint32_t response_status_code,
                           const std::string &fetch_err,
                           const TransferDiagnostics &diagnostics)> on_complete;
    };

    virtual ~HttpFetchEngine() = default;

    virtual void submit(Request request) = 0;
    virtual bool pauseTask(const std::string &task_id, uint64_t generation) = 0;
    virtual bool resumeTask(const std::string &task_id, uint64_t generation) = 0;
    virtual bool cancelTask(const std::string &task_id, uint64_t generation) = 0;
    virtual void reapCompleted(std::vector<std::thread> &join_threads) = 0;
    virtual void shutdown(std::vector<std::thread> &join_threads) = 0;
};

class ThreadedHttpFetchEngine final : public HttpFetchEngine {
public:
    void submit(Request request) override;
    bool pauseTask(const std::string &task_id, uint64_t generation) override;
    bool resumeTask(const std::string &task_id, uint64_t generation) override;
    bool cancelTask(const std::string &task_id, uint64_t generation) override;
    void reapCompleted(std::vector<std::thread> &join_threads) override;
    void shutdown(std::vector<std::thread> &join_threads) override;

private:
    struct ThreadState {
        ThreadState(std::thread &&thread_in,
                    std::shared_ptr<std::atomic_bool> done_in,
                    std::shared_ptr<std::atomic_bool> cancelled_in,
                    std::string task_id_in,
                    uint64_t generation_in)
            : thread(std::move(thread_in)),
              done(std::move(done_in)),
              cancelled(std::move(cancelled_in)),
              task_id(std::move(task_id_in)),
              generation(generation_in) {}
        std::thread thread;
        std::shared_ptr<std::atomic_bool> done;
        std::shared_ptr<std::atomic_bool> cancelled;
        std::string task_id;
        uint64_t generation = 0;
    };

private:
    std::mutex _mtx;
    std::vector<std::unique_ptr<ThreadState>> _threads;
};

class CurlMultiHttpFetchEngine final : public HttpFetchEngine {
public:
    CurlMultiHttpFetchEngine();
    ~CurlMultiHttpFetchEngine() override;

    void submit(Request request) override;
    bool pauseTask(const std::string &task_id, uint64_t generation) override;
    bool resumeTask(const std::string &task_id, uint64_t generation) override;
    bool cancelTask(const std::string &task_id, uint64_t generation) override;
    void reapCompleted(std::vector<std::thread> &join_threads) override;
    void shutdown(std::vector<std::thread> &join_threads) override;

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;
};
