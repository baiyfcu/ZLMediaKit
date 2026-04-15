#pragma once

#include "HttpStreamFetcher.h"
#include "EsFilePayloadProtocol.h"
#include "Poller/Timer.h"
#include "Util/ResourcePool.h"
#include "Util/util.h"
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

class EsFileFerryPacker {
public:
    static constexpr const char *kBootstrapTaskId = "__bootstrap__";
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
    // 设置每个 tick 的全局发送预算，0 表示恢复默认值。
    // 用法：用于限制单次调度周期内的总发送量，避免单轮发送占用线程过久。
    void setTickPayloadQuotaBytes(uint64_t quota_bytes);
    // 设置 HTTP 拉取最大并发数，0 表示恢复默认值。
    // 用法：该值限制同时处于拉取态的 HTTP 任务数，不建议直接设为总任务数。
    void setMaxHttpFetchConcurrency(size_t max_concurrency);
    // 设置 HTTP 全局缓冲上限，0 表示恢复默认值。
    // 用法：与单任务缓冲上限、HTTP 并发数共同决定进程的瞬时内存占用。
    void setMaxTotalHttpBufferedBytes(uint64_t max_buffered_bytes);

    // 设置发包回调
    void setPacketCallback(PacketCallback cb);
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

    struct TaskHttpState {
        bool source = false;
        bool queued = false;
        bool active = false;
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
    };

    // TaskState 只描述源数据读取与协议分片发送状态。
    // 何时开始 HTTP 响应、客户端是否提前关闭等上层语义由 GtApi 负责。
    struct TaskState {
        // 任务 ID
        std::string task_id;
        // 数据源路径（本地路径或 URL）
        std::string file_path;
        // 文件名
        std::string file_name;
        // 文件总大小
        uint64_t file_size = 0;
        // 任务代际版本
        uint64_t generation = 0;
        // 是否为内存分片模式
        bool memory_mode = false;
        // 内存模式下的完整数据
        std::vector<uint8_t> memory_payload;
        // 文件流句柄
        std::shared_ptr<std::ifstream> stream;
        // 发送态
        TaskSendState send;
        // HTTP 拉取态
        TaskHttpState http;
        // HTTP 缓冲态
        TaskHttpBufferState http_buffer;
    };

    EsFileFerryPacker(const EsFileFerryPacker &) = delete;
    EsFileFerryPacker &operator=(const EsFileFerryPacker &) = delete;

    // 视频专网默认单包 payload 大小。
    // 作用：决定本地文件、视频 HTTP 下载、GET/POST 大 JSON 响应在协议层拆成多大的 FileChunk。
    // 默认取 512KB：兼顾 50 路以上并发时的吞吐、调度公平性与包数。
    static constexpr size_t kDefaultPacketChunkBytes = 512 * 1024;
    // HTTP 拉取侧内部缓冲块大小。
    // 作用：视频流、大 JSON、普通文件等 HTTP 响应写入内部缓冲队列时使用的块尺寸。
    // 默认与协议分片同为 512KB，减少额外拷贝和碎片。
    static constexpr size_t kDefaultHttpBufferChunkBytes = 512 * 1024;
    // 单个 HTTP 任务允许累计的最大缓冲块数。
    // 作用：发送端跟不上时，对单任务施加背压，避免某一路长期占满内存。
    static constexpr size_t kDefaultMaxHttpBufferBlocksPerTask = 8;
    // 单个 HTTP 任务的默认最大缓冲字节数。
    // 推导关系：chunk_bytes * blocks_per_task。
    static constexpr uint64_t kDefaultMaxHttpBufferedBytesPerTask =
        static_cast<uint64_t>(kDefaultHttpBufferChunkBytes) *
        kDefaultMaxHttpBufferBlocksPerTask;
    // 视频专网默认 HTTP 拉取并发窗口。
    // 作用：限制同时处于 HTTP 拉取态的任务数量，避免 50 路任务同时拉取放大连接、源站压力和内存。
    // 默认取 6：适合作为 50 路以上视频/大 JSON 混合下载场景的均衡基线。
    static constexpr size_t kDefaultHttpFetchConcurrency = 6;
    // 视频专网默认 HTTP 全局缓冲上限。
    // 推导关系：单任务缓冲上限 * HTTP 并发窗口 = 4MB * 6 = 24MB。
    // 用法：提高 HTTP 并发时通常也要同步评估该值，避免瞬时内存放大。
    static constexpr uint64_t kDefaultMaxHttpBufferedBytes =
        kDefaultMaxHttpBufferedBytesPerTask * kDefaultHttpFetchConcurrency;
    // Bootstrap 发送间隔。
    // 作用：在建立传输后周期性发送引导 NAL，帮助接收端快速进入可解码态。
    static constexpr uint32_t kDefaultBootstrapIntervalMs = 2000;
    // 视频专网每个 tick 的默认全局发送预算。
    // 作用：限制单轮调度的总出流量，保障多路公平轮转而不是被大视频或大 JSON 长时间独占。
    // 默认取 12MB：适合作为 50 路以上并发场景的吞吐/公平折中值。
    static constexpr uint64_t kDefaultTickPayloadBudgetBytes = 12 * 1024 * 1024;

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
    // 启动 bootstrap 定时器（调用方需已持锁）
    void startBootstrapTimerLocked();
    // 停止 bootstrap 定时器（调用方需已持锁）
    void stopBootstrapTimerLocked();

    // 组装完整协议包（含起始码 + 固定头 + 变长字段）
    std::vector<uint8_t> buildPacket(const TaskState &task, EsFilePacketHeader &header, const std::vector<uint8_t> &payload) const;
    std::vector<uint8_t> buildPacket(const TaskState &task, EsFilePacketHeader &header, size_t payload_len, size_t *payload_offset) const;
    // tick 下的任务调度与发包主流程
    size_t processTickPackets(uint64_t total_payload_quota_bytes);
    // 向上游发出一个完整包
    bool emitPacket(const std::string &task_id, std::vector<uint8_t> &&packet, const EsFilePacketHeader &header) const;
    // 更新最近一次错误信息
    void setLastError(const std::string &err);
    // 获取仍活跃任务列表
    std::vector<std::string> snapshotActiveTasks() const;
    // 任务公平轮转顺序计算
    std::vector<std::string> pickFairRound(const std::vector<std::string> &active_ids);
    // 计算单任务本 tick 负载预算
    uint64_t computeTaskQuota(uint64_t total_payload_quota_bytes, size_t active_count, size_t index) const;
    // 判断任务是否已具备发送 FileEnd 的条件
    static bool isTaskReadyToEmitEnd(const TaskState &task);
    // 清理任务 HTTP 缓冲并同步更新全局缓冲统计（调用方需已持锁）
    void clearTaskHttpBufferLocked(TaskState &task);
    // 清理待启动 HTTP 拉取队列中的指定任务（调用方需已持锁）
    void erasePendingHttpFetchLocked(const std::string &task_id, uint64_t generation);
    // 尝试启动待处理的 HTTP 拉取任务
    void maybeStartPendingHttpFetches();
    // 收集可启动的 HTTP 拉取任务（调用方需已持锁）
    std::vector<std::pair<std::string, uint64_t>> collectHttpFetchLaunchesLocked();
    // 启动单个 HTTP 拉取任务
    void launchHttpFetchTask(const std::string &task_id, uint64_t generation);

private:
    // 全局互斥锁
    mutable std::mutex _mtx;
    // 当前分片大小
    size_t _chunk_size = kDefaultPacketChunkBytes;
    HttpChunkPool _http_chunk_pool;
    // 包输出回调
    PacketCallback _packet_cb;
    // task_id -> 任务状态
    std::unordered_map<std::string, TaskState> _tasks;
    // 任务代际计数器
    uint64_t _task_generation = 0;
    // 轮转游标
    size_t _rr_cursor = 0;
    // 待启动的 HTTP 拉取任务队列(task_id, generation)
    std::deque<std::pair<std::string, uint64_t>> _pending_http_fetches;
    // 当前活跃的 HTTP 拉取任务数
    size_t _active_http_fetches = 0;
    // 当前 HTTP 全局缓冲字节数
    uint64_t _total_http_buffered_bytes = 0;
    // HTTP 拉取最大并发数
    size_t _max_http_fetch_concurrency = kDefaultHttpFetchConcurrency;
    // HTTP 全局缓冲上限
    uint64_t _max_total_http_buffered_bytes = kDefaultMaxHttpBufferedBytes;
    // 最近一次错误信息
    std::string _last_error;
    // 每个 tick 的全局发送预算
    uint64_t _tick_payload_quota_bytes = kDefaultTickPayloadBudgetBytes;
    // 相对时间戳是否已初始化
    bool _ts_started = false;
    // 相对时间戳起始时刻
    std::chrono::steady_clock::time_point _ts_start_time;
    // 最近一次相对时间戳
    uint32_t _last_ts_ms = 0;
    // bootstrap 是否待发送
    bool _bootstrap_due = false;
    // bootstrap 定时器
    toolkit::Timer::Ptr _bootstrap_timer;
    // 发包线程
    std::thread _packet_thread;
    // 发包线程是否在运行
    bool _packet_thread_running = false;
    // 发包线程退出标记
    bool _packet_thread_exit = false;
    // 发包事件信号
    toolkit::semaphore _packet_sem;
};
