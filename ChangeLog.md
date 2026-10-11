# 更新日志

## v3.2.2026.0802 (2026-08-02)

### gRPC 协议增强与健壮性
- **MessageConverter 双协议转换助手**：新增公共 API，支持 Remoting（Message/MessageExt）与 gRPC（GrpcMessage）消息模型双向转换，消除调用方手写胶水代码
- **gRPC 解析与并发健壮性**：修复枚举偏移、字段编号错误，增强 Protobuf 解析容错与并发安全，提升 PushConsumer 稳定性
- **性能与架构优化**：优化 gRPC 编解码热点路径与整体架构

### 云适配优化
- **CloudProvider 统一**：统一阿里云、华为云、腾讯云及 Apache ACL 适配逻辑，升级依赖并优化多项细节

### Bug 修复
- **[fix]** 修复 WriteLog 空引用问题，Topic 名为空时兜底输出
- **[fix]** 补充 TOPIC_NOT_EXIST 异常场景日志，使用 range slice 优化

### 测试与质量
- 新增 17 个云厂商单元测试，覆盖四家 Provider 适配器逻辑
- 新增 gRPC 黄金字节样本、健壮性及 MessageConverter 单元测试
- 修复 4 个 CI 测试失败（空 Broker 列表、消费初始化等场景）
- 集成测试支持自动启动本地 RocketMQ 并硬失败

### 其他优化
- 重构 GitHub Actions RocketMQ 配置生成与下载方式，增强可读性和可维护性
- 新增竞品分析文档，重构需求/功能清单/架构三件套文档体系并统一格式

### 依赖升级与 NativeAOT 支持
- **NewLife.Core v12 测试版**：升级到 12.0.2026.1009-beta0916，适配 v12 消息栈（IMessageCodec/MessagePump）与 JSON 源生成要求
- **NativeAOT**：net10.0 目标开启 IsAotCompatible，库内序列化改走源生成成员访问器与元数据根，去除反射路径
- **AOT 冒烟**：新增 AotSmoke 控制台工程（26 用例：帧编解码/请求头/JSON/云厂商适配/双协议转换/端到端收发/gRPC 链路含死信转发与 FIFO 顺序消费），CI 增加 linux-x64 发布运行步骤
- **发布基线与监控**：新增 scripts/AotBaseline.cs 采集发布体积、启动开销与 CPU 吞吐并支持阈值判定，CI 输出流水线摘要并归档 JSON；win-x64 基线：可执行文件 9.97 MB、启动 46 ms
- **版本策略**：AssemblyVersion 由 `3.2.*` 通配符改为 `3.2.0.0`（通配符与确定性构建冲突，且程序集版本宜只在发布时递增，FileVersion/PackageVersion 仍用完整日期版本）

### RocketMQ 5.x gRPC 链路与消费语义
- **Proxy 链路打通**：修复明文 h2c 未强制 HTTP/2、发送缺 MessageId、Telemetry 一次性上报后被注销、缺 grpc-timeout、PushConsumer 缺客户端标识等缺陷；新增 GrpcTelemetrySession 资源上报长连接（周期重建自愈，暴露 ConnectCount/ErrorCount）
- **消费重试**：失败消息由 Broker 写入 %RETRY%{group}_{topic} 并按 popFromRetryProbability 概率并回原主题的拉取响应，客户端只需消费原主题；不可见时间下限收敛为 10 秒
- **死信处理**：新增 GrpcPushConsumer.MaxDeliveryAttempts（默认16）达限转 %DLQ%{group}，以及 Consumer.ForwardToDeadLetterQueueViaGrpcAsync、GetGrpcDlqTopic 与死信重投工具 scripts/DlqTool.cs
- **事务消息**：修正 EndTransaction 的 resolution/source 字段编号错位（此前回滚被当作提交处理），实测提交可见、回滚不可见
- **FIFO 顺序消费**：新增 Message.MessageGroup 与 GrpcPushConsumer.Orderly，实测并发度 20 下仍严格按发送顺序消费
- **主题类型**：CreateTopic 的 topicMessageType 改为写入 attributes 的 message.type，FIFO/事务主题可正常创建
- **ACL 2.0 鉴权**：修正 gRPC 签名输出为十六进制（Proxy 侧先 hex→base64 再比对）；实测无凭据、非管理员、错误密钥均被拒，管理员账户可正常收发。Proxy 需在 rmq-proxy.json 配置 `enableACL` 与 `enableAclRpcHookForClusterMode`，且开启前应预建内部主题 DefaultHeartBeatSyncerTopic（否则 Proxy 因无凭据建主题失败而启动不了）
- **同步发送**：Producer.Publish/PublishDelay 在 gRPC 模式下直接经 Proxy 发送（此前会因本地队列选择抛异常），延迟等级按 Broker 默认 messageDelayLevel 换算为投递时间戳；PublishOneway 在 gRPC 下没有单向语义，改为抛出 NotSupportedException
- **系统保留键**：发送时过滤 TAGS/KEYS/DELAY/TRAN_MSG/PGROUP，避免 Proxy 以 ILLEGAL_MESSAGE_PROPERTY_KEY 拒绝
- **GrpcPushConsumer 凭据**：新增 AccessKey/SecretKey，Proxy 启用 ACL 时可直接传入
- **本地环境**：Broker 启动加 -XX:MaxDirectMemorySize=1g 避免 POP 直接内存耗尽导致重试停滞；启动脚本与 CI 自动创建 Proxy 内部主题 DefaultHeartBeatSyncerTopic

### 测试与长稳
- gRPC 集成用例增至 9 个（路由/发送/接收确认/不可见重投/死信转发/推送重投/并发批量转死信/事务提交回滚/FIFO 顺序）
- 长稳脚本新增多主题多消费者与资源采样（句柄/线程/工作集/GC/长连接重建计数）：4 分钟 108 条、30 分钟 702 条均全确认零异常，长连接重建 405 次零异常、无泄漏
- GitHub Actions 增加测试前预热步骤（规避冷启动时序）与夜间长稳流水线（注入 Proxy 重启）
- 新增 AOT/JIT 性能对照基准（Benchmark 工程 + AotSmoke --bench）：AOT 吞吐约为 JIT 的 57%~74%，换取启动 46 ms 与自包含 9.97 MB

---

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
