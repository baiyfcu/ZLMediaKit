#pragma once

#include "HttpStreamFetcher.h"
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

struct EsFileGlobalOptions {
    // ===== 文档定义的主配置 =====
    size_t chunk_size_bytes = 128 * 1024;
    uint32_t protection_period_ms = 2000;
    uint32_t bootstrap_interval_ms = 2000;
    size_t http_concurrency_limit = 250;
    uint64_t http_buffer_limit_bytes = 128 * 1024 * 1024;
    uint64_t http_per_task_buffer_limit_bytes = 2 * 1024 * 1024;

    // ===== 令牌桶与保护期配置 =====
    uint64_t max_bandwidth_mbps = 0; // 0 表示不限制
    uint32_t initial_task_protection_tokens = 10;
    size_t global_protection_token_pool = 100;
    uint32_t protection_bandwidth_percent = 70;
    bool enable_annexb_payload_escape = true;
};

class EsFileFerryPacker {
public:
    static constexpr const char *kBootstrapTaskId = "__bootstrap__";
    using PacketCallback = std::function<void(const std::string &task_id, std::vector<uint8_t> &&packet, const EsFilePacketHeader &header)>;
    using HttpHeaders = HttpStreamFetcher::HttpHeaders;

    EsFileFerryPacker();
    ~EsFileFerryPacker();

    // 统一配置入口。
    void setGlobalOptions(const EsFileGlobalOptions &opts);
    void setPacketCallback(PacketCallback cb);
    void setDownstreamCongested(bool congested);
    bool addTask(const std::string &task_id, const std::string &source, const std::string &method = "GET", const HttpHeaders &headers = {}, const std::string &body = "", const std::string &file_name = "");
    void removeTask(const std::string &task_id);
    void clearTasks();

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
        bool failed = false;
        bool headers_ready = false;
        bool size_known = false;
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
        std::chrono::steady_clock::time_point active_since;
    };

    struct TaskSourceState {
        // 数据源路径（本地路径或 URL）
        std::string file_path;
        // 文件名
        std::string file_name;
        // 文件总大小
        uint64_t file_size = 0;
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
        bool protected_period_active = false;
        uint32_t protection_tokens_remaining = 0;
        std::chrono::steady_clock::time_point protection_end_time;
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

    struct HttpFetchThreadState {
        HttpFetchThreadState(std::thread &&thread_in,
                             std::shared_ptr<std::atomic_bool> done_in)
            : thread(std::move(thread_in)), done(std::move(done_in)) {}
        std::thread thread;
        std::shared_ptr<std::atomic_bool> done;
    };

    struct PacketRuntimeState {
        PacketCallback callback;
        bool ts_started = false;
        std::chrono::steady_clock::time_point ts_start_time;
        uint32_t last_ts_ms = 0;
        bool bootstrap_due = false;
        toolkit::Timer::Ptr bootstrap_timer;
        toolkit::Timer::Ptr pace_timer;
        std::thread packet_thread;
        bool packet_thread_running = false;
        bool packet_thread_exit = false;
        bool downstream_congested = false;
        toolkit::semaphore packet_sem;
    };

    EsFileFerryPacker(const EsFileFerryPacker &) = delete;
    EsFileFerryPacker &operator=(const EsFileFerryPacker &) = delete;

    // HTTP 拉取侧内部缓冲块大小。
    // 作用：视频流、大 JSON、普通文件等 HTTP 响应写入内部缓冲队列时使用的块尺寸。
    // 默认取 512KB：作为内部拉取缓冲块，减少额外拷贝和碎片。
    static constexpr size_t kDefaultHttpBufferChunkBytes = 512 * 1024;
    // 单个 HTTP 任务允许累计的最大缓冲块数。
    // 作用：发送端跟不上时，对单任务施加背压，避免某一路长期占满内存。
    static constexpr size_t kDefaultMaxHttpBufferBlocksPerTask = 8;
    // 速率整形的基础唤醒间隔，帮助令牌桶按较平滑的节拍恢复发送。
    static constexpr uint32_t kDefaultPaceIntervalMs = 20;
    // HTTP fast-start 保留槽位仅作为内部调度参数，不再暴露到公共配置接口。
    static constexpr size_t kDefaultHttpFastStartSlotReserve = 2;
    // 单次 packet 线程唤醒最多发出的包数，避免长时间自旋占满线程。
    static constexpr size_t kMaxPacketsPerTick = 64;
    // 单个控制面阶段每 tick 最多发出的包数，避免 FileInfo/Status 饿死数据面。
    static constexpr size_t kControlPlaneStagePacketBudget = 8;

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
    size_t processTickPackets(size_t max_packet_count);
    // 向上游发出一个完整包
    bool emitPacket(const std::string &task_id, std::vector<uint8_t> &&packet, const EsFilePacketHeader &header);
    // 在已组包后缩短 payload，并回写协议头长度字段
    static void shrinkChunkPacket(EsFilePacketHeader &header,
                                  std::vector<uint8_t> &packet,
                                  size_t payload_offset,
                                  size_t payload_len);
    // 发出失败状态包，并同步把任务推进到结束态
    bool emitFailedStatusPacket(const std::string &task_id);
    // 发出 FileInfo 包，并按需激活新任务保护期
    bool emitFileInfoPacket(const std::string &task_id);
    // 尝试为单个任务发出一个 FileChunk
    bool tryEmitTaskChunk(const std::string &task_id,
                          bool consume_protection_token,
                          size_t *protected_bandwidth_used);
    // 处理一组数据面任务，返回实际发出的包数量
    size_t processDataPlaneTasks(const std::vector<std::string> &task_ids,
                                 bool consume_protection_token,
                                 size_t max_bandwidth_bytes,
                                 size_t *bandwidth_used,
                                 size_t max_packet_count);
    // 发出 FileEnd 包，并清理任务残余缓冲
    bool emitFileEndPacket(const std::string &task_id);
    // 收集失败任务，供控制面优先发送 TaskStatus
    std::vector<std::string> collectFailedTaskIdsLocked() const;
    // 收集已具备发送 FileInfo 条件的任务
    std::vector<std::string> collectReadyInfoTaskIdsLocked() const;
    // 收集已具备发送 FileEnd 条件的任务
    std::vector<std::string> collectReadyEndTaskIdsLocked() const;
    // 按 task_id 批量发送控制面包，若任务状态有推进则同步重算 profile
    size_t emitTaskPacketsWithProfileRefresh(
        const std::vector<std::string> &task_ids,
        bool (EsFileFerryPacker::*emit_fn)(const std::string &),
        size_t max_packet_count);
    // 执行一个控制面阶段：收集候选任务、做公平轮转、批量发包
    size_t runControlPlaneStage(
        std::vector<std::string> (EsFileFerryPacker::*collect_fn)() const,
        bool (EsFileFerryPacker::*emit_fn)(const std::string &),
        size_t max_packet_count);
    // 以下为一阶段真实主调度面：
    // - pickFairRound
    //   决定数据面统一公平轮转；
    // - isTaskDataSchedulable / isTaskReadyToEmitEnd
    //   负责 control plane 与 data plane 的边界判断；
    // - refreshUnifiedTaskProfileLocked / recomputeAllTaskRateProfilesLocked /
    //   refreshTaskRateBucketsLocked 负责统一运行时 profile 与动态分母重算。
    // 单集合公平轮转顺序计算
    std::vector<std::string> pickFairRound(const std::vector<std::string> &active_ids);
    // 判断任务是否已具备发送 FileEnd 的条件
    static bool isTaskReadyToEmitEnd(const TaskState &task);
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
    // 释放已完成或超时的 fast-start granted 资格（调用方需已持锁）
    bool refreshFastStartGrantLocked(TaskState &task,
                                     std::chrono::steady_clock::time_point now);
    // 标记 HTTP 任务已完成首个 FileChunk 发出
    void markHttpFirstChunkEmitted(const std::string &task_id, uint64_t generation);
    // 启动单个 HTTP 拉取任务
    void launchHttpFetchTask(const std::string &task_id, uint64_t generation);
    // 回收已结束的 HTTP 拉取线程（调用方需已持锁）
    void reapCompletedHttpFetchThreadsLocked(std::vector<std::thread> &join_threads);
    
private:
    struct DataPlaneTaskGroups {
        std::vector<std::string> protected_task_ids;
        std::vector<std::string> normal_task_ids;
    };

    // 全局互斥锁
    mutable std::mutex _mtx;
    // 全局业务配置
    EsFileGlobalOptions _global_options;
    HttpChunkPool _http_chunk_pool;
    TaskRegistryState _task_registry;
    HttpFetchRuntimeState _http_runtime;
    PacketRuntimeState _packet_runtime;
    std::vector<std::unique_ptr<HttpFetchThreadState>> _http_fetch_threads;
    
    // 全局令牌桶与保护期运行态
    size_t _token_bucket_tokens = 0; // 当前令牌桶可用字节数
    std::chrono::steady_clock::time_point _token_refill_time; // 上次填充时间
    size_t _global_protection_tokens_remaining = 0; // 全局保护期token池
    // 填充令牌桶
    void refillTokenBucket(std::chrono::steady_clock::time_point now);
    // 判断任务是否在保护期
    bool isTaskInProtection(const TaskState &task, std::chrono::steady_clock::time_point now) const;
    // 收集当前 tick 的数据面任务分组，并在保护期结束时归还 token
    DataPlaneTaskGroups collectDataPlaneTaskGroupsLocked(
        std::chrono::steady_clock::time_point now);
};
