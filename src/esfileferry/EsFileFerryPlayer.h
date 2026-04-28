#pragma once

#include "EsFilePayloadProtocol.h"
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

struct EsTaskDataEvent {
    // 协议包类型
    EsFilePacketType type = EsFilePacketType::Unknown;
    // 任务 ID
    std::string task_id;
    // 文件名
    std::string file_name;
    // 文件总大小
    uint64_t file_size = 0;
    // 当前包偏移
    uint64_t offset = 0;
    // 当前包序号
    uint32_t seq = 0;
    // 扩展标记位
    uint16_t flags = 0;
    // 累计接收字节数
    uint64_t received_size = 0;
    // 接收进度
    double progress = 0;
    // 是否已完成
    bool completed = false;
    // 原始负载
    std::vector<uint8_t> payload;
    // 文本状态
    std::string status;
};

enum class EsFileUnpackErrorType {
    Unknown = 0,
    FrameTooSmall,
    RawPacketDecodeFailed,
    BufferedPacketInvalid,
    MissingTask,
};

struct EsFileUnpackErrorEvent {
    EsFileUnpackErrorType type = EsFileUnpackErrorType::Unknown;
    std::string message;
    std::string task_id;
    EsFilePacketType packet_type = EsFilePacketType::Unknown;
    uint32_t seq = 0;
    uint32_t payload_len = 0;
    uint64_t file_size = 0;
    size_t miss_count = 0;
    size_t frame_size = 0;
};

class EsFileFerryUnPacker {
public:
    using OnTaskData = std::function<void(const EsTaskDataEvent &)>;
    using OnError = std::function<void(const EsFileUnpackErrorEvent &)>;
    using LegacyOnError = std::function<void(const std::string &)>;

    // 单例入口
    static EsFileFerryUnPacker &Instance();

    // 注册/更新 task 数据回调，cb 为空表示移除
    void setTaskCallback(const std::string &task_id, OnTaskData cb);
    // 按 task_id 移除任务回调与运行态
    void removeTask(const std::string &task_id);
    // 清空所有任务回调与运行态
    void clearTasks();
    // 获取当前已注册 task_id 列表
    std::vector<std::string> getTaskIds() const;
    // 获取最近一次错误信息
    std::string getLastError() const;

    // 输入原始帧数据
    bool inputFrame(const uint8_t *data, size_t size);

    // 设置统一错误回调
    void setOnError(OnError cb);
    // 兼容旧版仅字符串错误描述回调
    void setOnError(LegacyOnError cb);

private:
    struct TaskRuntimeState {
        // 匹配到的协议包数量
        uint64_t matched_packet_count = 0;
        // 匹配到的有效载荷总字节数
        uint64_t matched_bytes = 0;
        // 当前已接收文件字节数
        uint64_t received_size = 0;
        // 文件总大小
        uint64_t file_size = 0;
        // 是否已完成
        bool completed = false;
        // 是否已记录过序号
        bool has_seq = false;
        // 最近一次序号
        uint32_t last_seq = 0;
        // 重复序号计数
        uint64_t duplicate_seq_count = 0;
        // 乱序序号计数
        uint64_t out_of_order_seq_count = 0;
    };

    EsFileFerryUnPacker();
    EsFileFerryUnPacker(const EsFileFerryUnPacker &) = delete;
    EsFileFerryUnPacker &operator=(const EsFileFerryUnPacker &) = delete;

    // 循环解析缓冲区中的完整协议包
    void parseBuffer(const uint8_t *data, size_t size);
    // 从内部缓冲区解析一个完整协议包
    bool parseOnePacket(EsFilePacket &packet);
    // 从外部原始字节解析一个完整协议包（不修改内部缓冲）
    bool parseOnePacketFromRaw(const uint8_t *data, size_t size, EsFilePacket &packet, size_t &consumed);
    // 分发协议包到对应 task 回调并更新运行态
    void dispatchPacket(EsFilePacket packet,const uint8_t *data, size_t size);
    // 更新最近错误信息
    void setLastError(const std::string &err);
    // 触发错误回调
    void emitError(EsFileUnpackErrorEvent event);

private:
    void appendToBufferLocked(const uint8_t *data, size_t size);
    void resetBufferIfFullyConsumedLocked();
    void compactBufferLocked();

private:
    static constexpr size_t kMaxPacketSize = 2 * 1024 * 1024;
    static constexpr size_t kInitialBufferReserveBytes = 256 * 1024;
    static constexpr size_t kCompactThresholdBytes = 64 * 1024;
    // 控制流 task_id
    static constexpr const char *kBootstrapTaskId = "__bootstrap__";

    // 原始字节缓冲区互斥锁
    mutable std::mutex _buffer_mtx;
    // task 回调/运行态/错误信息互斥锁
    mutable std::mutex _task_mtx;
    // 最近一次错误信息
    std::string _last_error;
    // 待解析字节缓冲区
    std::vector<uint8_t> _buffer;
    size_t _buffer_start = 0;
    // task_id -> 数据回调
    std::unordered_map<std::string, OnTaskData> _task_callbacks;
    // task_id -> 运行态状态
    std::unordered_map<std::string, TaskRuntimeState> _task_states;
    // 错误回调
    OnError _on_error;

};
