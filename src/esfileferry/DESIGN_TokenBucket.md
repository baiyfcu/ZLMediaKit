# EsFileFerry 令牌桶和保护期设计方案

## 1. 方案概述

本方案解决：
1. **令牌桶带宽限制：控制发送给下游的最大带宽
2. **全局保护期token池：控制新任务保护期并发数
3. **保护期带宽配额：保护老任务不被饿死

## 2. 问题背景

### 2.1 为什么需要这个方案？

| 问题 | 说明 |
|------|------|
| 无带宽限制 | 有数据就发，可能造成下游网络拥塞 |
| 100路并发保护期 | 新任务占满带宽，老任务饿死 |
| 无保护期控制 | 新任务启动慢，用户体验差 |

### 2.2 方案定位

```
网络架构：
上游HTTP源 --(全速拉取，buffer控制)--> Packer --(令牌桶限速 + 保护期)--> 下游播放器
```

## 3. 核心设计

### 3.1 配置项 (EsFileGlobalOptions)

```cpp
struct EsFileGlobalOptions {
    // ===== 原配置 =====
    size_t chunk_size_bytes = 128 * 1024;
    uint32_t protection_period_ms = 2000;
    uint32_t bootstrap_interval_ms = 2000;
    size_t http_concurrency_limit = 50;
    uint64_t http_buffer_limit_bytes = 128 * 1024 * 1024;
    uint64_t http_per_task_buffer_limit_bytes = 2 * 1024 * 1024;
    
    // ===== 新增带宽控制配置 =====
    size_t max_bandwidth_bytes_per_sec = 0; // 最大发送带宽，0表示不限制
    
    // ===== 新增保护期配置 =====
    uint32_t initial_task_protection_tokens = 10; // 新任务初始保护期token数
    size_t global_protection_token_pool = 100;   // 全局保护期token池上限
    uint32_t protection_bandwidth_percent = 70;  // 保护期任务最多占总带宽百分比(%)
};
```

### 3.2 TaskState 新增字段

```cpp
struct TaskState {
    // ===== 原字段 =====
    // ... 原有字段 ...
    
    // ===== 新增字段 =====
    uint32_t protection_tokens_remaining = 0; // 剩余保护期token
    std::chrono::steady_clock::time_point protection_end_time; // 保护期结束时间
};
```

### 3.3 EsFileFerryPacker 新增成员

```cpp
    // ===== 令牌桶成员 =====
    size_t _token_bucket_tokens = 0; // 当前令牌桶可用字节数
    std::chrono::steady_clock::time_point _token_refill_time; // 上次填充时间
    size_t _global_protection_tokens_remaining = 0; // 全局保护期token池
```

## 4. 工作流程

### 4.1 addTask() 流程

```
1. 初始化任务
   ├─ 初始化原字段
   └─ 尝试申请保护期token
      ├─ 检查全局token池是否足够
      ├─ 是 → 设置保护期token、开始/结束时间
      └─ 否 → 不进保护期
```

### 4.2 processTickPackets() 流程

```
1. 填充令牌桶 (refillTokenBucket)

2. 任务分类
   ├─ protected_task_ids  → 保护期任务（高优先级）
   ├─ normal_task_ids     → 普通任务
   └─ end_task_ids        → 结束任务

3. 处理保护期任务
   ├─ 检查保护期配额（保护期带宽百分比）
   ├─ 检查令牌桶
   ├─ 消耗保护期token
   └─ 发送

4. 处理普通任务
   ├─ 检查令牌桶
   └─ 发送

5. 处理结束任务
```

### 4.3 保护期判断逻辑

```
任务在保护期内的条件：
task.protected_period_active = true
AND
task.protection_tokens_remaining > 0
AND
now < task.protection_end_time
```

## 5. 核心函数

### 5.1 refillTokenBucket

填充令牌桶

```cpp
void EsFileFerryPacker::refillTokenBucket(const std::chrono::steady_clock::time_point &now)
```

### 5.2 isTaskInProtection (内部函数）

判断任务是否在保护期

## 6. 并发控制策略

### 6.1 全局保护期token池

| 配置 | 默认值 | 说明 |
|------|--------|------|
| global_protection_token_pool | 100 | 全局最多100个保护期token |
| initial_task_protection_tokens | 10 | 新任务分配10个token |

### 6.2 保护期带宽配额

| 配置 | 默认值 | 说明 |
|------|--------|------|
| protection_bandwidth_percent | 70% | 保护期任务最多占总带宽70%，30%给老任务 |

## 7. 使用示例

### 7.1 设置带宽限制

```cpp
EsFileGlobalOptions opts;
opts.max_bandwidth_bytes_per_sec = 50 * 1024 * 1024; // 50MB/s
packer.setGlobalOptions(opts);
```

### 7.2 设置保护期

```cpp
EsFileGlobalOptions opts;
opts.initial_task_protection_tokens = 5; // 每个新任务5个token
opts.global_protection_token_pool = 50; // 全局最多50个保护期token
opts.protection_bandwidth_percent = 60; // 保护期最多占60%带宽
packer.setGlobalOptions(opts);
```

## 8. 向后兼容说明

✅ 默认配置下行为：
- max_bandwidth_bytes_per_sec = 0  -> 不限制带宽
- global_protection_token_pool = 100  -> 默认保护期池
- protection_bandwidth_percent = 70%  -> 保护期占70%

如果想恢复原行为（无保护期无带宽限制）：

```cpp
EsFileGlobalOptions opts;
opts.max_bandwidth_bytes_per_sec = 0;
opts.global_protection_token_pool = 0; // 或 initial_task_protection_tokens = 0
packer.setGlobalOptions(opts);
```

## 9. 流程图

```
令牌桶 refill 逻辑
├─ 计算上次 refill 到现在过了多久
├─ 按时间计算要加多少token
└─ 加满到桶容量（1秒的带宽）
```

