# esfileferry 模块集成说明

## 1. 模块定位

`esfileferry` 提供“文件/HTTP 数据源 -> ES 负载包 -> 媒体承载传输 -> 协议解包事件”的完整基础能力，适合给上层业务模块做文件摆渡、HTTP 资源摆渡、跨节点回传。

核心职责分为四块：

- `EsFileFerryPacker`：发送侧，负责任务管理、分片、打包、调度与发包回调。
- `EsFileFerryUnPacker`：接收侧，负责从字节流容错解包并按 `task_id` 分发事件。
- `EsFileFerryPuller`：拉流侧，负责播放拉流、轨道接入、帧注入 UnPacker、断流重试。
- `EsFilePayloadProtocol`：协议模型与常量定义。

## 2. 目录结构

```text
esfileferry/
├── CMakeLists.txt
├── EsFileFerryPacker.h/.cpp
├── EsFileFerryPlayer.h/.cpp      # UnPacker 实现
├── EsFileFerryPuller.h/.cpp
├── EsFilePayloadProtocol.h/.cpp
└── tests/
    ├── test_packer.cpp
    └── test_unpacker.cpp
```

## 3. 依赖与构建

### 3.1 依赖

- C++ 工具链（项目同级）
- `libcurl`
- `zlmediakit`
- `zltoolkit`

`CMakeLists.txt` 中 `esfileferry` 为静态库，默认开启测试目标：

- `esfileferry_packer_tests`
- `esfileferry_unpacker_tests`

### 3.2 集成到 CMake

```cmake
add_subdirectory(esfileferry)
target_link_libraries(your_target PRIVATE esfileferry)
```

## 4. 协议与传输格式

### 4.1 包类型

- `FileInfo`：任务元信息包
- `FileChunk`：分片数据包
- `FileEnd`：结束包
- `TaskStatus`：任务状态/错误包

### 4.2 固定头与载体

- 固定头长度：`48` 字节（大端）
- 传输载体前缀长度：`5` 字节（`00 00 00 01` + `kEsFileCarrierNalHeader`）
- 实际发送字节布局：
  - `5 字节载体前缀`
  - `48 字节固定头`
  - `task_id`
  - `file_name`
  - `payload`

### 4.3 关键常量

- `kEsFilePacketMagic = 0x47544659`
- `kEsFilePacketVersion = 1`
- `kEsFileCarrierPrefixSize = 5`
- `kEsFileFlagFileInfoHasHttpResponseHeaders = 0x0001`

当 `FileInfo.flags` 包含 `kEsFileFlagFileInfoHasHttpResponseHeaders` 时，`FileInfo.payload` 携带 HTTP 响应头元数据（用于上层回写源站状态码/响应头）。

## 5. 发送侧集成（EsFileFerryPacker）

### 5.1 典型流程

1. 获取单例并配置 `PacketCallback`
2. 可选配置 `setChunkSize`
3. 调用 `addFileTask` / `addHttpTask` / `addTask`
4. 在回调里将 `packet` 注入你的媒体发送链路
5. 按需 `removeTask` 或 `clearTasks`

### 5.2 最小示例

```cpp
auto &packer = EsFileFerryPacker::Instance();

packer.setPacketCallback(
    [](const std::string &task_id,
       std::vector<uint8_t> &&packet,
       const EsFilePacketHeader &header) {
      if (task_id == EsFileFerryPacker::kBootstrapTaskId) {
        return;
      }
      send_to_media_channel(task_id, std::move(packet), header);
    });

packer.setChunkSize(512 * 1024);
packer.addFileTask("task_local_1", "/data/a.mp4", "a.mp4");
```

### 5.3 HTTP 任务行为

- 仅支持 `GET/POST`，其他方法会失败并写入 `getLastError()`
- HTTP 拉取线程会边拉边写入内存缓冲 `memory_payload`
- 发包线程并行消费内存缓冲，不依赖临时文件
- `CURLOPT_BUFFERSIZE` 使用 `512KB`
- 当响应头到达后，会优先产出携带 HTTP 元数据的 `FileInfo`
- 拉取失败时会发送 `TaskStatus` 包，`payload` 为错误文本

### 5.4 调度特性

- 默认分片：`128KB`
- 每轮全局 payload 配额：`8MB`
- 活跃任务公平轮转，单任务按配额裁剪
- 同一任务发送顺序：`FileInfo -> FileChunk* -> FileEnd`

### 5.5 API 迁移说明

本轮公平调度重构接受一次破坏性 API 清理：公开头文件不再提供 `EsFileTaskType`、`EsFileTaskPriority`、`http_api_rate_share`、`http_mp4_rate_share`、`http_download_rate_share`。

迁移方式：

- 删除外部调用方对旧任务类型、旧优先级和三类权重字段的引用。
- 使用 `http_pull_total_rate_mbps` 配置统一总速率；当前该字段同时约束 HTTP fetch 和统一 emit 发送面。
- 使用 `http_pull_concurrency_limit`、`http_pull_total_buffer_limit_bytes`、`packet_chunk_bytes`、`scheduler_round_budget_bytes`、`min_emit_payload_bytes` 做高并发调参。
- 如果外部代码只调用 `addFileTask`、`addHttpTask`、`addTask`、`setGlobalOptions`，且没有引用旧分类符号，一般不需要业务改动。

### 5.6 线程模型

- `PacketCallback` 在 Packer 发包线程触发
- 回调中不要做阻塞 I/O 与重计算
- 任务增删与发包并发受内部互斥保护

### 5.7 高并发 HTTP 下载配置建议

面向浏览器 MP4 下载、录像文件下载等场景时，应把“任务总数”和“同时 active HTTP 拉取数”分开理解。`EsFileFerryPacker` 可以接收 100/200+ 个 HTTP 任务，但不建议把 `http_pull_concurrency_limit` 直接设置为任务总数。

推荐模型：

- 200+ 个任务可以同时进入 Packer，由内部 pending 队列排队。
- `http_pull_concurrency_limit` 只控制当前同时通过 curl 拉取上游 HTTP 的 active 任务数。
- pending 任务不会占用 active fetch 的限速分母，也不会持续堆积 HTTP body 缓冲。
- active 任务通过每任务缓冲和全局缓冲上限形成背压，避免下游消费慢时内存无界增长。

200+ 任务的推荐起步配置：

```cpp
EsFileGlobalOptions opts;

// 先不要直接设为 200。建议从 16 开始，稳定后再试 24/32。
opts.http_pull_concurrency_limit = 16;

// 按 active 并发估算，而不是按总任务数估算。
// 当前默认单 active HTTP 任务缓冲上限约 512KB，
// 并发 24 左右时可先给 96MB，全链路稳定后再调整。
opts.http_pull_total_buffer_limit_bytes = 96 * 1024 * 1024;

// 单任务 HTTP 回源缓冲上限。
// 如果业务要求 100 路都持续有进度，建议先压到 512KB-1MB，
// 避免 100 路同时 active 时每路都堆到过大的回源缓冲。
opts.http_pull_per_task_buffer_limit_bytes = 512 * 1024;

// 统一速率上限。建议设置为机器/源站/下游链路安全吞吐的 60%-80%，
// 避免一开始使用 0 不限速造成源站、发送队列或内存压力突刺。
opts.http_pull_total_rate_mbps = 640;

// 单个 FileChunk 的目标 payload 大小。
// 默认 128KB 通常适合多路公平调度；如果下游包开销偏高，可压测 256KB。
opts.packet_chunk_bytes = 128 * 1024;

// 单轮调度预算，通常先保留默认 4MB。
opts.scheduler_round_budget_bytes = 4 * 1024 * 1024;

// 最小 payload 成形门限，降低高并发下的小包比例。
opts.min_emit_payload_bytes = 16 * 1024;

packer.setGlobalOptions(opts);
```

建议按下面三档压测：

| 档位 | active HTTP 并发 | 全局 HTTP buffer | 适用场景 |
|---|---:|---:|---|
| 保守 | 12-16 | 48-64MB | 先验证 200+ 任务排队稳定性 |
| 均衡 | 24-32 | 96-128MB | 推荐生产起点 |
| 激进 | 48-64 | 192-256MB | 机器、源站、下游发送队列都确认可承载后再试 |

调参原则：

- `http_pull_concurrency_limit` 是源站连接数、线程数、内存和下游发送压力的核心旋钮，不等于浏览器任务总数。
- `http_pull_total_buffer_limit_bytes` 应随 active 并发同步评估；可按 `active 并发 * 单任务缓冲上限` 估算初值，再结合压测调整。
- `http_pull_total_rate_mbps` 同时约束 HTTP fetch 与统一 emit 发送面；配置为 `0` 表示不限速，生产高并发场景不建议默认不限速。
- `PacketCallback` 必须快速返回。若上层要写浏览器 socket 或跨线程发送，建议进入有界队列；队列满时阻塞回调，让背压回传到 Packer/HTTP 拉取侧。
- 浏览器下载进度不需要由 Packer 暴露额外字段。Packer 只输出 `FileInfo/FileChunk/FileEnd/TaskStatus`，HTTP 响应层负责还原响应头、`Content-Length`、`Range/206` 等浏览器语义。
- 如果业务目标是真正 200 路同时 active 且每路都高速下载，当前“一路 active HTTP 一个 fetch 线程”的模型会有线程数和上下文切换压力，应考虑后续演进为 `libcurl multi` 或 HTTP 拉取线程池模型。

## 6. 接收侧集成（EsFileFerryUnPacker）

### 6.1 典型流程

1. 为关心的 `task_id` 注册 `setTaskCallback`
2. 把收到的原始字节流持续喂给 `inputFrame(data, size)`
3. 在 `OnTaskData` 中处理 `FileInfo/FileChunk/FileEnd/TaskStatus`
4. 任务结束后调用 `removeTask(task_id)` 清理状态

### 6.2 最小示例

```cpp
auto &unpacker = EsFileFerryUnPacker::Instance();

unpacker.setTaskCallback("task_local_1", [](const EsTaskDataEvent &event) {
  if (event.type == EsFilePacketType::FileChunk && !event.payload.empty()) {
    append_file_bytes(event.payload.data(), event.payload.size());
  }
  if (event.type == EsFilePacketType::FileEnd || event.completed) {
    finalize_file();
  }
});

unpacker.setOnError([](const std::string &err) {
  on_unpack_error(err);
});
```

### 6.3 解包容错行为

- 支持在噪声字节流中扫描载体前缀与魔数
- 支持拆包/粘包场景
- 不完整包进入内部缓冲等待后续字节
- 非法数据采用滑动前进策略继续扫描

### 6.4 UnPacker 限流与保护策略

`EsFileFerryUnPacker` 当前没有“按字节速率/包速率主动限流”的硬限制，策略重点是避免异常流量、噪声帧和错误日志把 CPU 与日志系统拖垮。实际保护行为如下：

- 分层处理顺序：
  - 第一层是输入门禁：先过滤明显不可能成为协议包的超小帧，尽量在 `inputFrame()` 入口快速返回。
  - 第二层是原始帧直解：优先尝试“单帧就是完整协议包”的快路径，命中后直接分发，减少共享缓冲参与。
  - 第三层是缓冲重组：只有快路径未命中时，才把数据写入内部缓冲并进入拆包/粘包解析。
  - 第四层是异常恢复：对非法候选按字节滑动前进，并通过采样日志保留排障信息，避免持续卡死在坏数据上。
- 按权重的资源倾斜：
  - 高权重放在“让正常流量尽快通过”，因此完整包直解优先级最高，尽量避免所有流量都进入缓冲区慢路径。
  - 中权重放在“让异常流量可恢复”，因此对坏包采取滑动扫描而不是整段丢弃，减少误伤后续有效数据。
  - 低权重放在“错误可观测性”，因此错误日志、缺失任务告警都做采样和降频，优先保证吞吐而不是日志完整性。
- 分层后的效果：
  - 正常完整帧主要消耗在快路径，CPU 开销最低。
  - 拆包/粘包流量主要消耗在内部缓冲与重组逻辑，属于次优先层。
  - 噪声流、错流、未注册任务流量被压到最低优先级处理，只保留必要的诊断信息。

- 小帧快速忽略：
  - `inputFrame()` 对 `size <= 48` 的帧直接返回，不进入解包主流程。
  - 对明显不是摆渡协议前缀的短帧，仅做采样告警，避免高频打印。
- 原始帧解析失败日志采样：
  - 单帧直解失败时，仅对命中候选特征的大帧/带前缀帧输出调试日志。
  - 采样策略为“前 5 次必打，后续每 200 次打 1 次”。
- 缓冲区解析失败日志采样：
  - 内部缓冲解析出错时，同样按“前 5 次必打，后续每 200 次打 1 次”输出告警。
  - 避免噪声流或错流场景下日志风暴。
- 未注册任务 `task_id` 降频：
  - 收到未注册 `task_id` 的协议包时，不会每包都报错。
  - `MissingTask` 错误事件按每 `10000` 次采样一次，降低无效回调和日志压力。
- 非法包滑动跳过：
  - 对缓冲中的非法包，不会整段反复重试，而是仅前进 1 字节继续扫描下一个候选起点。
  - 这样能在混杂噪声字节流中持续前进，避免卡死在同一坏包上。
- 缓冲区按需扩容与压缩：
  - 初始预留 `256KB`。
  - 数据不足时先尝试 `compact`，把已消费区间前移复用。
  - 仅在容量不够时扩容，扩容策略为按需增长或倍增，减少频繁 realloc。
- 缓冲区压缩门限：
  - `_buffer_start < 64KB` 且剩余数据不小于已消费数据时，不立即搬移内存。
  - 这是一种“延迟压缩”策略，用更少的 `memmove` 换吞吐稳定性。
- 满缓冲自动清空已消费态：
  - 当内部缓冲已全部消费后，直接 `clear + reset start`，避免空转保留脏状态。

对接建议：

- `OnTaskData` 回调里不要做阻塞 I/O 或重计算，否则会把解包线程的吞吐问题误判成“UnPacker 限流”。
- 若上游存在大量短小噪声帧，优先在进入 `inputFrame()` 前做一次业务层过滤。
- 若需要真正的速率治理，应在 `EsFileFerryPuller` 或上游媒体输入侧做背压/丢弃策略，而不是依赖 `UnPacker`。
- 若后续要继续增强 `UnPacker`，建议仍保持“快路径优先、缓冲重组次之、日志观测最后”的分层原则，不要把重日志或复杂统计放到第一层。

## 7. 拉流集成（EsFileFerryPuller）

`EsFileFerryPuller` 负责把媒体帧接到 `EsFileFerryUnPacker::inputFrame`，适合“从 RTSP 直接收摆渡流”的模块。

### 7.1 最小示例

```cpp
auto &puller = EsFileFerryPuller::Instance();
puller.setOnError([](const std::string &err) {
  on_pull_error(err);
});
puller.startPull("rtsp://127.0.0.1/live/esferry", 0);
```

### 7.2 行为说明

- `startPull(url, rtp_type)` 会重建播放器并开始拉流
- 拉流失败或断流后，默认 1 秒自动重试
- 仅视频轨数据进入 UnPacker，音频轨会被忽略
- `stopPull()` 会停止播放并清理 delegate/运行状态

## 8. 与业务模块对接建议

### 8.1 任务标识约定

- 业务层必须保证 `task_id` 唯一
- 同一 `task_id` 重复添加会覆盖旧任务状态
- 控制流 `task_id` 固定为 `__bootstrap__`，业务层应忽略

### 8.2 结束判定

建议以以下条件之一作为完成：

- `event.type == EsFilePacketType::FileEnd`
- `event.completed == true`

### 8.3 错误处理

- 发送侧错误：`EsFileFerryPacker::getLastError()`
- 接收侧错误：`EsFileFerryUnPacker::getLastError()` 与 `setOnError`
- 拉流侧错误：`EsFileFerryPuller::getLastError()` 与 `setOnError`

### 8.4 API/mp4 点播与下载业务分层

结合当前 `api/mp4`、播放串点播、文件下载链路，建议按下面的层次理解 `esfileferry` 的职责边界：

- HTTP 入口层：
  - 负责接收浏览器、播放器或下载器发起的 `mp4` 点播/下载请求。
  - 典型承载点是 `HttpFileManager` 的路由入口，以及 `HttpBody` 对外暴露的 HTTP 响应语义。
  - 这一层只关心“收到什么 URL、回什么 HTTP 头和 body”，不直接处理摆渡协议细节。
- 业务编排层：
  - 负责把一次 HTTP 访问转换成“新增摆渡任务 / 删除摆渡任务”。
  - 典型实现是 `PlayChannelRequestContext`、`GtApi`、`WebHook`、`ferry_task_event(add/del)`。
  - 这一层决定任务 `task_id`、目标源地址、请求头透传、超时窗口和取消时机。
- 数据摆渡发送层：
  - 由 `EsFileFerryPacker` 负责把文件源或 HTTP 源转换成 `FileInfo/FileChunk/FileEnd/TaskStatus` 协议包。
  - 这一层只负责“如何分片、如何打包、如何按任务发送”，不关心浏览器、下载器或 `mp4` 播放器的 HTTP 语义。
- 媒体承载层：
  - 负责把协议包挂到 RTP/H264/PS 等媒体承载链路上传输。
  - 这一层是摆渡数据跨节点传输的通道层，不负责业务路由和 HTTP 收尾判定。
- 数据摆渡接收层：
  - 由 `EsFileFerryUnPacker` 负责从媒体字节流中恢复协议包，并输出任务事件。
  - 输出的是 `FileInfo/FileChunk/FileEnd/TaskStatus` 事件，而不是直接输出 HTTP 响应。
- HTTP 响应恢复层：
  - 由 `PlayChannelClientImp` 一类适配层把 `UnPacker` 输出事件重新还原为 HTTP 语义。
  - `FileInfo` 对应响应头准备，`FileChunk` 对应 body 数据，`FileEnd` 或 `completed=true` 对应完成，`TaskStatus` 对应失败。
  - 历史上的关键要求是：这一层要处理“header/body 乱序容忍”“99%+ 未收到 FileEnd 时的收尾”“客户端断开后的任务取消”。

按职责分工，几个容易混淆但应明确分开的点如下：

- `api/mp4` 点播/下载是否成功，不能只看 `Packer` 是否还在拉源，还要看 HTTP 响应层是否正确收到了 header、body 与 complete。
- `UnPacker` 不负责业务层超时、取消、路由映射，也不负责决定何时发 `add/del` 回调。
- `Packer` 不负责浏览器或下载器的 HTTP 兼容细节，例如响应头恢复、header 先后顺序、客户端断开回收。
- 路由映射问题（例如虚拟路径、`/changeprotocol=...`、静态资源路径）属于 HTTP 入口层，不属于 `esfileferry` 协议层。
- 如果出现“下载到 99%+ 未完成”，应优先检查“HTTP 响应恢复层和业务超时层”，其次才看 `Packer/UnPacker` 是否漏发或漏收 `FileEnd`。

## 9. 性能与稳定性建议

- 多路并发默认优先使用 `128KB` 分片；若下游包开销较高，可压测 `256KB/512KB`
- 业务回调只做轻量逻辑，重任务异步转移
- 任务完成后及时 `removeTask`，避免状态长期堆积
- 高并发场景优先监控：任务总数、active HTTP fetch 数、pending 队列长度、`PacketCallback` 耗时、全局 HTTP buffer、进程线程数、源站连接数、重试次数

## 10. 测试与验证

可直接复用模块内测试：

```bash
cmake --build <build_dir> --target esfileferry_packer_tests esfileferry_unpacker_tests -j4
```

运行测试二进制后可验证：

- 本地文件打包/解包完整性
- HTTP 源任务打包能力
- 随机分段输入下的解包稳定性
- 序号顺序与完成判定

## 11. 相关文档

- `../docs/es_file_payload_protocol.md`
- `../docs/esfileferry_packer_player_responsibility.md`
- `../docs/ferry_webhook_interface.md`
