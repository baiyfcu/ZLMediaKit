# esfileferry tests

这个目录提供 `esfileferry` 相关的轻量测试工程，适合做：

- 单独编译 `packer/http_fetcher/unpacker` 测试
- 在 macOS 上快速跑 `sample`
- 作为后续 profiling 和压测的固定入口

## 测试目标

- `esfileferry_packer_tests`
- `esfileferry_http_fetcher_tests`
- `esfileferry_unpacker_tests`
- `esfileferry_tests_all`

## 独立构建

在当前目录单独生成一个最小构建树：

```bash
cmake -S . -B build
cmake --build build --target esfileferry_tests_all -j4
```

单独编某一个测试：

```bash
cmake --build build --target esfileferry_packer_tests -j4
```

## 主工程内构建

如果已经在 `ZLMediaKit` 根目录配置工程，并打开了：

- `ENABLE_FERRY=ON`
- `ENABLE_TESTS=ON`
- `ESFILEFERRY_BUILD_TESTS=ON`

则可以直接构建：

```bash
cmake --build <zlm-build-dir> --target esfileferry_tests_all -j4
```

## 快速采样

推荐优先从 `packer` 测试开始：

```bash
./run_profile.sh
```

默认行为：

- 自动确保 `build` 目录存在
- 编译 `esfileferry_packer_tests`
- 启动测试进程后，使用 macOS `sample` 对其 pid 采样 5 秒
- 输出到 `/tmp/esfileferry_packer_tests.sample.txt`

如果想采样 `http_fetcher`：

```bash
./run_profile.sh --target esfileferry_http_fetcher_tests
```

如果想延长采样时间：

```bash
./run_profile.sh --seconds 10
```

## 推荐观察点

做定点优化时，优先看这些热点：

- `EsFileFerryPacker::processTickPackets()`
- `EsFileFerryPacker::emitPacket()`
- `EsFileFerryPacker::launchHttpFetchTask()`
- `_mtx` 持锁时间
- HTTP 缓冲满时的等待占比

## Instruments

如果需要更完整的火焰图和线程视图，建议直接对测试二进制使用：

- `Time Profiler`
- `System Trace`

独立工程默认生成的测试二进制位于：

```text
tests/build/esfileferry_packer_tests
tests/build/esfileferry_http_fetcher_tests
tests/build/esfileferry_unpacker_tests
```
