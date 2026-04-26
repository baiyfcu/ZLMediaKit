#pragma once

#include "HttpStreamFetcher.h"
#include "HttpFetchEngine.h"
#include "EsFilePayloadProtocol.h"
#include "Poller/Timer.h"
#include "Util/ResourcePool.h"
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <fstream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

struct EsFilePackTaskInfo {
    // 任务 ID
    std::string task_id;
    // 数据源路径（本地路径或 URL）
    std::string file_path;
    // 文件名
    std::string file_name;
    // 文件总大小
    uint64_t file_size = 0;
    // 已发送字节数
    uint64_t sent_bytes = 0;
    // 下一个包序号
    uint32_t next_seq = 0;
    // FileInfo 是否已发送
    bool info_sent = false;
    // 任务是否完成
    bool completed = false;
};

struct EsFileGlobalOptions {
    // 单个 FileChunk 的默认业务分片大小。
    // 取 128KB：兼顾多路并发时的平滑度与包数量。
    size_t packet_chunk_bytes = 128 * 1024;
    // 单轮调度的默认发送预算。
    // 该值只控制单轮调度粒度与线程占用时长，不承担总体限速语义。
    uint64_t scheduler_round_budget_bytes = 4 * 1024 * 1024;
    // FileChunk 的最小发送门限（仅对 FileChunk 生效）。
    // emit token 小于该值时延后发送，等待 token 累积；尾包可放行。
    uint64_t min_emit_payload_bytes = 32 * 1024;
    // HTTP 回源的默认最大并发窗口。
    // 取 150：适合作为 150~200 路混合任务的默认保护值。
    size_t http_pull_concurrency_limit = 150;
    // HTTP 回源的默认全局缓冲上限。
    // 取 128MB：与默认并发窗口一起控制瞬时内存占用。
    uint64_t http_pull_total_buffer_limit_bytes = 128 * 1024 * 1024;
    // 单任务 HTTP 回源缓冲上限。
    // 默认保持 512KB：100 路都需保活时建议压到 512KB-1MB。
    uint64_t http_pull_per_task_buffer_limit_bytes = 512 * 1024;
    // 当前配置名沿用 HTTP pull 语义，
    // 但一阶段重构后也作为统一发送面的总体速率上限使用。
    // 单位：Mb/s，按 1 Mbps = 1024 * 1024 bit/s 换算。
    uint64_t http_pull_total_rate_mbps = 450;
    // 为新 HTTP 任务预留的 fast-start active 槽位。
    size_t http_pull_fast_start_slot_reserve = 2;
    // fast-start granted 的首包保护窗口。
    uint32_t http_pull_fast_start_protection_ms = 2000;
};

class EsFileFerryPacker {
public:
    static constexpr const char *kBootstrapTaskId = "__bootstrap__";
    // 一阶段主调度面说明：
    // 1. 控制面优先推进：TaskStatus / FileInfo / FileEnd 独立于数据面公平轮转；
    // 2. 数据面统一等权：所有可调度任务进入同一公平轮转集合；
    // 3. 运行时 profile 统一：buffer / burst / emit/fetch rate 不再按旧分类分叉。
    // 同步发包回调，运行在 Packer 调度线程。
    // 如果第三方调用方需要异步转发，建议在回调内部使用有界队列；
    // 当队列达到上限时阻塞回调线程，让背压自然回传到 Packer/HTTP 拉取线程，
    // 避免下游无限缓存导致内存放大。
    using PacketCallback = std::function<void(const std::string &task_id, std::vector<uint8_t> &&packet, const EsFilePacketHeader &header)>;
    using HttpHeaders = HttpStreamFetcher::HttpHeaders;

    EsFileFerryPacker();
    // 单例入口
    static EsFileFerryPacker &Instance();
    ~EsFileFerryPacker();

    // 设置单个 FileChunk 的目标 payload 大小，0 表示恢复默认值。
    // 用法：下游回调/发送链路较快时可适当调大以减少包数；
    // 多路并发较高、希望调度更平滑时可适当调小。
    void setChunkSize(size_t chunk_size);
    // 设置单轮调度的全局发送预算，0 表示恢复默认值。
    // 用法：用于限制单次调度循环内的总发送量，避免单轮发送占用线程过久。
    void setSchedulerRoundBudgetBytes(uint64_t budget_bytes);
    // 设置 FileChunk 最小发送门限，0 表示关闭门限。
    void setMinEmitPayloadBytes(uint64_t min_payload_bytes);
    // 设置 HTTP 拉取最大并发数，0 表示恢复默认值。
    // 用法：该值限制同时处于拉取态的 HTTP 任务数，不建议直接设为总任务数。
    void setMaxHttpFetchConcurrency(size_t max_concurrency);
    // 设置 HTTP 全局缓冲上限，0 表示恢复默认值。
    // 用法：与单任务缓冲上限、HTTP 并发数共同决定进程的瞬时内存占用。
    void setMaxTotalHttpBufferedBytes(uint64_t max_buffered_bytes);
    // 批量设置全局保护参数。
    void setGlobalOptions(const EsFileGlobalOptions &opts);

    // 设置发包回调
    void setPacketCallback(PacketCallback cb);
    // 下游消费拥塞时抑制普通 FileChunk 的 fetch/emit；控制面包仍继续推进。
    void setDownstreamCongested(bool congested);
    // 添加本地文件任务
    bool addFileTask(const std::string &task_id, const std::string &file_path, const std::string &file_name = "");
    // 添加 HTTP 源任务。
    // 适用于视频文件下载，也适用于 GET/POST 返回大 JSON、大文本或二进制数据。
    bool addHttpTask(const std::string &task_id, const std::string &url, const std::string &method = "GET", const HttpHeaders &headers = {}, const std::string &body = "", const std::string &file_name = "");
    // 统一添加任务入口（自动识别本地/HTTP）
    bool addTask(const std::string &task_id, const std::string &source, const std::string &method = "GET", const HttpHeaders &headers = {}, const std::string &body = "", const std::string &file_name = "");
    // 移除单个任务
    void removeTask(const std::string &task_id);
    // 清空全部任务
    void clearTasks();

    // 获取任务快照信息
    std::vector<EsFilePackTaskInfo> getTaskInfos() const;
    // 获取最近一次错误信息
    std::string getLastError() const;
    std::vector<std::string> testPickFairRound(const std::vector<std::string> &active_ids) {
        return pickFairRound(active_ids);
    }

    uint64_t testComputeTaskQuota(uint64_t total_payload_quota_bytes, size_t active_count, size_t index) const {
        return computeTaskQuota(total_payload_quota_bytes, active_count, index);
    }

    uint64_t testGetTotalHttpBufferedBytes() const {
        std::lock_guard<std::mutex> lock(_mtx);
        return _http_runtime.total_buffered_bytes;
    }

    static constexpr size_t testDefaultHttpFetchConcurrency() {
        return kDefaultHttpFetchConcurrency;
    }

    static constexpr uint64_t testDefaultMaxHttpBufferedBytesPerTask() {
        return kDefaultMaxHttpBufferedBytesPerTask;
    }

    static constexpr uint64_t testDefaultMaxHttpBufferedBytes() {
        return kDefaultMaxHttpBufferedBytes;
    }

    static constexpr size_t testDefaultHttpBufferChunkBytes() {
        return kDefaultHttpBufferChunkBytes;
    }

    uint64_t testGetTaskFetchRateBps(const std::string &task_id) const {
        std::lock_guard<std::mutex> lock(_mtx);
        auto it = _task_registry.tasks.find(task_id);
        return it != _task_registry.tasks.end() ? it->second.control.fetch_bucket.rate_bps : 0;
    }

    uint64_t testGetTaskEmitRateBps(const std::string &task_id) const {
        std::lock_guard<std::mutex> lock(_mtx);
        auto it = _task_registry.tasks.find(task_id);
        return it != _task_registry.tasks.end() ? it->second.control.emit_bucket.rate_bps : 0;
    }

private:
    struct HttpChunkBuffer {
        explicit HttpChunkBuffer(size_t capacity) : data(capacity) {}
        std::vector<uint8_t> data;
        size_t size = 0;
    };
    using HttpChunkPool = toolkit::ResourcePool<HttpChunkBuffer>;
    using HttpChunkBufferPtr = HttpChunkPool::ValuePtr;

    struct TaskSendState {
        uint64_t sent_bytes = 0;
        uint32_t next_seq = 0;
        bool info_sent = false;
        bool end_sent = false;
    };

    struct TaskHttpBufferState {
        uint64_t buffered_bytes = 0;
        size_t front_chunk_offset = 0;
        std::deque<HttpChunkBufferPtr> chunks;
        std::shared_ptr<std::condition_variable> cv;
    };

    struct TokenBucket {
        bool unlimited = true;
        uint64_t rate_bps = 0;
        uint64_t burst_bytes = 0;
        uint64_t tokens = 0;
        std::chrono::steady_clock::time_point last_refill;
    };

    struct TaskControlState {
        uint64_t max_buffered_bytes = 0;
        uint64_t resume_buffered_bytes = 0;
        TokenBucket fetch_bucket;
        TokenBucket emit_bucket;
    };

    struct TaskHttpRuntimeState {
        bool source = false;
        bool queued = false;
        bool active = false;
        bool fetch_paused = false;
        bool failed = false;
        bool headers_ready = false;
        bool size_known = false;
        uint32_t status_code = 0;
        uint64_t received_bytes = 0;
        std::string error;
        std::string method;
        HttpHeaders request_headers;
        std::string request_body;
        std::vector<uint8_t> response_meta_payload;
        TaskHttpBufferState buffer;
        bool fast_start_candidate = false;
        bool fast_start_granted = false;
        bool first_chunk_emitted = false;
        uint32_t retry_attempt = 0;
        std::chrono::steady_clock::time_point buffer_nonempty_since;
        std::chrono::steady_clock::time_point last_chunk_emit_at;
        std::chrono::steady_clock::time_point queued_since;
        std::chrono::steady_clock::time_point active_since;
        std::chrono::steady_clock::time_point retry_not_before;
    };

    struct TaskSourceState {
        // 数据源路径（本地路径或 URL）
        std::string file_path;
        // 文件名
        std::string file_name;
        // 文件总大小
        uint64_t file_size = 0;
        // 是否为内存分片模式
        bool memory_mode = false;
        // 内存模式下的完整数据
        std::vector<uint8_t> memory_payload;
        // 文件流句柄
        std::shared_ptr<std::ifstream> stream;
    };

    // TaskState 只描述源数据读取与协议分片发送状态。
    // 何时开始 HTTP 响应、客户端是否提前关闭等上层语义由 GtApi 负责。
    struct TaskState {
        // 任务 ID
        std::string task_id;
        // 任务代际版本
        uint64_t generation = 0;
        // 源数据态
        TaskSourceState source;
        // 发送态
        TaskSendState send;
        // HTTP 拉取态
        TaskHttpRuntimeState http;
        // 任务级流控态
        TaskControlState control;
    };

    struct TaskRegistryState {
        std::unordered_map<std::string, TaskState> tasks;
        uint64_t generation = 0;
        size_t rr_cursor = 0;
    };

    struct HttpFetchRuntimeState {
        std::deque<std::pair<std::string, uint64_t>> pending_fetches;
        size_t active_fetches = 0;
        uint64_t total_buffered_bytes = 0;
    };

    struct PacketRuntimeState {
        PacketCallback callback;
        std::string last_error;
        bool downstream_congested = false;
        bool ts_started = false;
        std::chrono::steady_clock::time_point ts_start_time;
        uint32_t last_ts_ms = 0;
        bool bootstrap_due = false;
        toolkit::Timer::Ptr bootstrap_timer;
        toolkit::Timer::Ptr pace_timer;
        std::thread packet_thread;
        bool packet_thread_running = false;
        bool packet_thread_exit = false;
        toolkit::semaphore packet_sem;
    };

    EsFileFerryPacker(const EsFileFerryPacker &) = delete;
    EsFileFerryPacker &operator=(const EsFileFerryPacker &) = delete;

    // 视频专网默认单包 payload 大小。
    // 作用：决定本地文件、视频 HTTP 下载、GET/POST 大 JSON 响应在协议层拆成多大的 FileChunk。
    // 默认取 128KB：兼顾多路并发下的平滑度、吞吐与包数量。
    static constexpr size_t kDefaultPacketChunkBytes = 128 * 1024;
    // HTTP 拉取侧内部缓冲块大小。
    // 作用：视频流、大 JSON、普通文件等 HTTP 响应写入内部缓冲队列时使用的块尺寸。
    // 默认取 512KB：作为内部拉取缓冲块，减少额外拷贝和碎片。
    static constexpr size_t kDefaultHttpBufferChunkBytes = 512 * 1024;
    // 单个 HTTP 任务允许累计的最大缓冲块数。
    // 作用：发送端跟不上时，对单任务施加背压，避免某一路长期占满内存。
    static constexpr size_t kDefaultMaxHttpBufferBlocksPerTask = 8;
    // 单个 HTTP 任务的默认最大缓冲字节数。
    // 默认取 512KB：与 EsFileGlobalOptions 保持一致，保证 0 回退路径语义一致。
    static constexpr uint64_t kDefaultMaxHttpBufferedBytesPerTask =
        512 * 1024;
    // 视频专网默认 HTTP 拉取并发窗口。
    // 作用：限制同时处于 HTTP 拉取态的任务数量，默认与 EsFileGlobalOptions 保持一致。
    static constexpr size_t kDefaultHttpFetchConcurrency = 150;
    // 视频专网默认 HTTP 全局缓冲上限。
    // 默认取 128MB：与 EsFileGlobalOptions 保持一致，避免 reset/构造默认值漂移。
    static constexpr uint64_t kDefaultMaxHttpBufferedBytes =
        128 * 1024 * 1024;
    // Bootstrap 发送间隔。
    // 作用：在建立传输后周期性发送引导 NAL，帮助接收端快速进入可解码态。
    static constexpr uint32_t kDefaultBootstrapIntervalMs = 2000;
    // 速率整形的基础唤醒间隔，帮助令牌桶按较平滑的节拍恢复发送。
    static constexpr uint32_t kDefaultPaceIntervalMs = 20;
    // 视频专网默认的单轮调度发送预算。
    // 作用：限制单轮调度的总出流量，保障多路公平轮转而不是被大视频或大 JSON 长时间独占。
    // 默认取 4MB：与 EsFileGlobalOptions 保持一致。
    static constexpr uint64_t kDefaultSchedulerRoundBudgetBytes =
        4 * 1024 * 1024;
    // 单任务 HTTP 默认缓冲上限统一收敛为单一值，不再按旧业务分类拆分。
    static constexpr uint64_t kDefaultUnifiedBufferedBytes =
        kDefaultMaxHttpBufferedBytesPerTask;

    // 获取本地文件大小
    static bool getFileSize(const std::string &file_path, uint64_t &size);
    // 选择输出文件名（优先显式传入）
    static std::string pickFileName(const std::string &file_path, const std::string &file_name);
    // 组装协议头
    EsFilePacketHeader makePacketHeader(const TaskState &task, EsFilePacketType type, uint64_t data_offset, uint32_t payload_len, uint16_t flags, uint32_t seq, uint32_t timestamp_ms) const;
    // 输出 bootstrap NAL 包
    void emitBootstrapPackets(const PacketCallback &cb) const;
    // 生成相对毫秒时间戳
    uint32_t nextRelativeTimestampMs();
    // 启动发包线程（调用方需已持锁）
    void startPacketThreadLocked();
    // 停止发包线程（调用方需已持锁）
    void stopPacketThreadLocked(std::thread &join_thread);
    // 发包线程主循环
    void packetThreadLoop();
    // bootstrap 定时器回调
    bool onBootstrapTimer();
    // 速率整形定时器回调
    bool onPaceTimer();
    // 启动 bootstrap 定时器（调用方需已持锁）
    void startBootstrapTimerLocked();
    // 停止 bootstrap 定时器（调用方需已持锁）
    void stopBootstrapTimerLocked();
    // 启动速率整形定时器（调用方需已持锁）
    void startPaceTimerLocked();
    // 停止速率整形定时器（调用方需已持锁）
    void stopPaceTimerLocked();

    // 组装完整协议包（含起始码 + 固定头 + 变长字段）
    std::vector<uint8_t> buildPacket(const TaskState &task, EsFilePacketHeader &header, const std::vector<uint8_t> &payload) const;
    std::vector<uint8_t> buildPacket(const TaskState &task, EsFilePacketHeader &header, size_t payload_len, size_t *payload_offset) const;
    // tick 下的任务调度与发包主流程
    size_t processTickPackets(uint64_t total_payload_quota_bytes);
    // 向上游发出一个完整包
    bool emitPacket(const std::string &task_id, std::vector<uint8_t> &&packet, const EsFilePacketHeader &header) const;
    // 更新最近一次错误信息
    void setLastError(const std::string &err);
    // 以下为一阶段真实主调度面：
    // - snapshotSchedulableTaskIds / pickFairRound / computeTaskQuota
    //   共同决定数据面统一公平轮转；
    // - isTaskControlReady / isTaskDataSchedulable / isTaskReadyToEmitEnd
    //   负责 control plane 与 data plane 的边界判断；
    // - refreshUnifiedTaskProfileLocked / recomputeAllTaskRateProfilesLocked /
    //   refreshTaskRateBucketsLocked 负责统一运行时 profile 与动态分母重算。
    // 获取当前轮次的数据面可调度任务列表
    std::vector<std::string> snapshotSchedulableTaskIds() const;
    // 单集合公平轮转顺序计算
    std::vector<std::string> pickFairRound(const std::vector<std::string> &active_ids);
    // 计算单任务本 tick 负载预算
    uint64_t computeTaskQuota(uint64_t total_payload_quota_bytes, size_t active_count, size_t index) const;
    // 判断任务是否已具备发送 FileEnd 的条件
    static bool isTaskReadyToEmitEnd(const TaskState &task);
    // 判断任务是否存在待立即推进的控制包
    static bool isTaskControlReady(const TaskState &task);
    // 判断 HTTP 任务内部缓冲是否已完全排空
    static bool isHttpTaskBufferDrained(const TaskState &task);
    // 判断任务是否具备参与当前数据面公平轮转的条件
    static bool isTaskDataSchedulable(const TaskState &task);
    // 刷新统一运行时 profile（调用方需已持锁）。
    // 一阶段重构后 profile 不再按 task_type 分叉。
    void refreshUnifiedTaskProfileLocked(TaskState &task, bool reset_buckets);
    // 根据当前任务状态重新分配全局共享速率（调用方需已持锁）
    void recomputeAllTaskRateProfilesLocked(bool reset_buckets);
    // 基于统一调度口径刷新单任务令牌桶（调用方需已持锁）
    void refreshTaskRateBucketsLocked(
        TaskState &task,
        size_t active_http_fetch_count,
        size_t schedulable_task_count,
        bool reset_buckets);
    // 更新令牌桶
    static void refillTokenBucket(TokenBucket &bucket);
    // 当前令牌桶可用的字节数
    static uint64_t peekTokenBucketBytes(TokenBucket &bucket);
    // 从令牌桶消费字节数
    static void consumeTokenBucketBytes(TokenBucket &bucket, uint64_t bytes);
    // 估算至少获取指定令牌所需等待时间
    static std::chrono::milliseconds estimateTokenWait(const TokenBucket &bucket,
                                                       uint64_t min_bytes);
    // 清理任务 HTTP 缓冲并同步更新全局缓冲统计（调用方需已持锁）
    void clearTaskHttpBufferLocked(TaskState &task);
    // 清理待启动 HTTP 拉取队列中的指定任务（调用方需已持锁）
    void erasePendingHttpFetchLocked(const std::string &task_id, uint64_t generation);
    // 尝试启动待处理的 HTTP 拉取任务
    void maybeStartPendingHttpFetches();
    // 收集可启动的 HTTP 拉取任务（调用方需已持锁）
    std::vector<std::pair<std::string, uint64_t>> collectHttpFetchLaunchesLocked();
    // 根据当前水位/拥塞状态更新单任务 fetch pause/resume（调用方需已持锁）
    void maybeUpdateHttpFetchControlLocked(TaskState &task);
    // 根据当前全局状态刷新全部活跃 HTTP 任务的 fetch pause/resume（调用方需已持锁）
    void maybeUpdateAllHttpFetchControlsLocked();
    // 释放已完成或超时的 fast-start granted 资格（调用方需已持锁）
    bool refreshFastStartGrantLocked(TaskState &task,
                                     std::chrono::steady_clock::time_point now);
    // 标记 HTTP 任务已完成首个 FileChunk 发出
    void markHttpChunkEmitted(const std::string &task_id, uint64_t generation);
    // 启动单个 HTTP 拉取任务
    void launchHttpFetchTask(const std::string &task_id, uint64_t generation);

private:
    // 全局互斥锁
    mutable std::mutex _mtx;
    // 全局业务配置
    EsFileGlobalOptions _global_options;
    HttpChunkPool _http_chunk_pool;
    TaskRegistryState _task_registry;
    HttpFetchRuntimeState _http_runtime;
    PacketRuntimeState _packet_runtime;
    std::unique_ptr<HttpFetchEngine> _http_fetch_engine;
};
