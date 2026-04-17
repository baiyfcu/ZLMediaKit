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

- 默认分片：`512KB`
- 每轮全局 payload 配额：`8MB`
- 活跃任务公平轮转，单任务按配额裁剪
- 同一任务发送顺序：`FileInfo -> FileChunk* -> FileEnd`

### 5.5 线程模型

- `PacketCallback` 在 Packer 发包线程触发
- 回调中不要做阻塞 I/O 与重计算
- 任务增删与发包并发受内部互斥保护

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

## 9. 性能与稳定性建议

- 分片大小建议不小于 `512KB`
- 业务回调只做轻量逻辑，重任务异步转移
- 任务完成后及时 `removeTask`，避免状态长期堆积
- 高并发场景优先监控：任务数、回调耗时、内存占用、重试次数

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
