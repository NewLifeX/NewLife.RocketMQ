# NewLife.RocketMQ 更新日志 2026

## v3.0.2026.0504 (2026-05-04)

### 新特性：5.x 协议增强批次（F052~F058）
* **F052** 消息压缩可插拔接口：新增 `IMessageCompressor` 接口与 `ZlibMessageCompressor` 内置实现，`MessageCompressorRegistry` 全局注册表；`MessageExt` 解压与 `Producer` 压缩均改用注册表调度，支持 ZLIB/LZ4/ZSTD 按需扩展
* **F053** 优先级消息：`GrpcSystemProperties` 新增 `Priority` 字段（Proto field 20，RIP-80），`GrpcMessagingService.SendMessageAsync` 支持传递优先级
* **F054** ACL 2.0 权限模型：`AclProvider` 新增 `AclEnabled`/`ResourceType`/`ResourceName` 属性，`ClusterClient.SetSignature` 自动注入 `aclEnabled`/`resourceType`/`resourceName` 请求头，与 ACL 1.x 完全兼容
* **F055** ChangeInvisibleDuration 不递增 ReconsumeTimes：`ChangeInvisibleTimeAsync` 新增 `incrementReconsumeTimes = true` 可选参数，`false` 时请求头附加 `reconsumeTimes = -1`
* **F056** LMQ 轻量消息队列：`Message` 类新增 `PROPERTY_INNER_MULTI_DISPATCH`/`PROPERTY_INNER_CONSUMER_QUEUE` 常量及 `SetLmqDestination`/`GetLmqDestination` 辅助方法
* **F058** gRPC PushConsumer：新增 `GrpcPushConsumer` 类（netstandard2.1+），内置长轮询线程、信号量并发控制，支持 `OnMessage` 回调式消费，自动 Ack/延迟重试

### 新增单元测试（+42 个）
* `CompressionTests`（12 个）：ZLIB 压缩/解压、RFC1950 头部检测、RAW DEFLATE 兼容、注册表、未知类型抛异常
* `ProtoTests`（5 个）：Priority 字段序列化与反序列化
* `SupportApacheAclTest`（5 个）：ACL 2.0 请求头注入验证
* `PopConsumeTests`（5 个）：ChangeInvisibleTime incrementReconsumeTimes 双模式
* `MessageTests`（6 个）：LMQ 属性设置与读取
* `GrpcPushConsumerTests`（9 个）：属性验证、回调路径、Ack/重试路径

---

## v3.0.2026.0304 (2026-03-04)

### 架构优化
* 重构 RocketMQ gRPC 协议为 SpanReader/SpanWriter 实现，提升性能
* 重构架构文档，优化编解码器实现
* 优化项目文件描述及标题信息

### 文档完善
* 文档体系重构：新增架构与需求文档，优化 Readme
* 新增架构设计文档和功能分析文档

### 依赖升级
* 升级 NewLife.Core 依赖到最新版本

---

## v3.0.2026.0228 (2026-02-28)

### 问题修复
* 修复逻辑缺陷并补充单元测试
* 新增客户端拉取超时机制，防止 RocketMQ 4.9.8 消费者线程卡死

---

## v3.0.2026.0216 (2026-02-16)

### 重大更新：v3.0 云适配重构
* 云适配重构与功能全面单元测试覆盖
* 完善兼容性梳理与优化进展

### 新增特性
* 新增 VIP 通道支持
* 新增批量消息确认机制
* 新增 5.x MsgId 支持
* 新增 gRPC Telemetry 遥测支持
* 完整实现**事务消息**功能（PR #108）
  - 增加 RocketMQ 事务消息发布与结束接口
  - 完善事务消息实现并通过审查与安全扫描
* 完整实现**请求-应答模式** (Request-Reply) 功能（PR #107）
  - 完善 RocketMQ 5.0 特性支持
  - 通过单元测试和文档验证

### 文档完善
* 新增功能分析与架构设计文档
* Phase 6 文档总结：兼容性梳理与优化进展

### 依赖升级
* 升级 NuGet 依赖包

---

## v3.0.2026.0211 (2026-02-11)

### 依赖升级
* 升级 NewLife.Core 依赖到最新版本
