# 更新日志

## v3.1.2026.0601 (2026-06-01)

### RocketMQ 5.x 协议增强（F052~F058）
- **消息压缩可插拔**：实现 ZLIB/LZ4/ZSTD 压缩接口，支持按需注册压缩算法
- **gRPC 优先级消息**：gRPC 通道支持消息优先级发送
- **ACL 2.0 权限模型**：新增 ACL 2.0 权限验证支持，对齐 RocketMQ 最新安全规范
- **Pop 不递增重试次数**：修正 Pop 消费时错误累加重试次数的行为，精确控制消费重试语义
- **LMQ 轻量队列**：支持 Light Message Queue（LMQ），适用于百万级设备场景
- **gRPC PushConsumer**：新增 gRPC PushConsumer 主流程与注册表机制

### Lite Topic（F057）
- **Lite Topic 支持**：新增 `TopicMessageType` 枚举，`MqBase.CreateTopic` 增加 `topicMessageType` 参数，支持 RocketMQ 5.5.0+ Lite Topic 动态创建，兼容旧版 Broker

### 消息钩子机制
- **MessageTrace Hook**：`Producer`/`Consumer` 新增消息钩子注册方法，支持消息轨迹与拦截扩展
- **一键安装脚本**：新增 RocketMQ Windows 一键安装启动脚本，自动下载 JDK/RocketMQ 并配置环境，便于本地集成测试

### Bug 修复
- **[fix]** 修复 `MessageExt` 解压 Body 时的错误
- **[fix]** 修正批量消息与 IPv6 `SysFlag` 标志位，严格对齐协议规范
- **[fix]** 补充 `Command.Error` 实现和 `NameClient` 空指针检查
- **[fix]** 修复 Pull Request 拉取目标查找逻辑

### 测试与质量
- 新增 42 个单元测试（F052~F058 覆盖），测试用例总数达 519
- 完善端到端集成测试与 Request-Reply 机制验证
- 重构测试目录（`Consumer` → `Consumers`，`Producer` → `Producers`）与命名空间对齐

---

## v3.0.2026.0501 (2026-05-01)

### 问题修复
- **[fix]** 修复 Pop/Ack/ChangeInvisibleTime 操作缺少 `queueId` 参数导致服务端处理异常的问题
- **便利方法**：`MessageExt` 新增多个便利访问方法，简化消息属性读取

### 依赖更新
- 升级 NewLife.Core 依赖包到最新版本（2026-04-xx）

---

## v3.0.2026.0305 (2026-03-05)

### 云适配重构（重大版本）
- **架构重构**：全面升级为 v3.0 云适配架构，新增 `ICloudProvider` 接口统一阿里云、华为云、腾讯云适配
- **事务消息**：新增 RocketMQ 事务消息发布与回查接口，支持分布式事务场景
- **请求-应答模式**：新增 Request-Reply 同步调用模式，支持消息级 RPC

### gRPC 协议支持
- **gRPC 5.x Proxy**：新增 gRPC 协议支持，零依赖不引入第三方 Protobuf/gRPC 库
- **SpanReader/SpanWriter 重构**：将 gRPC 协议编解码器重构为基于 `SpanReader`/`SpanWriter` 的零分配实现，提升性能
- **gRPC Telemetry**：新增 gRPC Telemetry 链路追踪支持

### 新增功能
- **VIP 通道**：支持 VIP Channel 高优先级消息通道
- **批量确认**：支持批量 Ack 操作，减少网络往返
- **5.x MsgId**：支持 RocketMQ 5.x 消息 ID 格式生成与解析
- **客户端拉取超时**：新增 `Consumer.PullTimeout` 客户端侧应用层超时保护，防止 4.9.8 无响应导致消费线程永久阻塞

### 测试覆盖
- 新增 152 个单元测试，覆盖协议层、模型层、工具类等核心组件
- 完善架构文档与需求文档

---

## v2.7.2026.0301 (2026-03-01)

### 问题修复
- 新增`Consumer.PullTimeout`属性，默认值0表示自动取`SuspendTimeout+10_000ms`，作为客户端拉取消息的应用层超时保护，防止RocketMQ 4.9.8在SuspendTimeout后无响应导致消费线程永久阻塞

## v2.7.2026.0201 (2026-02-01)

### 依赖更新
- 升级 NewLife.Core 依赖包到最新版本（2026-01-24）
- 升级 NewLife.Core 依赖包（2026-01-14）
- 升级 NewLife.Core 依赖包（2026-01-12）

## v2.7.2026.0102 (2026-01-03)

初始发布版本
