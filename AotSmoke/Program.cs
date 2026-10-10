// RocketMQ NativeAOT 冒烟测试
//
// 用途：以 NativeAOT（PublishAot=true）发布为原生可执行文件后运行，验证裁剪环境下以下链路仍然可用：
//   帧编解码、请求头属性字典、JSON 序列化（含源生成成员访问器）、云厂商适配、双协议消息转换、
//   消息体对象序列化、埋点标签序列化，以及端到端收发（普通/对象体/批量/延迟、消费、偏移查询、顺序锁、POP 消费）。
// 环境变量 ROCKETMQ_NAMESERVER（如 127.0.0.1:9876）可选；设置后追加真实收发用例，未设置时跳过。
// 环境变量 ROCKETMQ_GRPC_PROXY（如 http://127.0.0.1:8081）可选；设置后追加 gRPC 5.x 链路用例（路由/收发/确认/推送消费/死信转发/FIFO 顺序消费）。
//
// 本地：dotnet publish AotSmoke -c Release -r win-x64 -p:PublishAot=true && ./AotSmoke/bin/Release/net10.0/win-x64/publish/AotSmoke.exe
// CI  ：dotnet publish AotSmoke -c Release -r linux-x64 -p:PublishAot=true && ./AotSmoke/bin/Release/net10.0/linux-x64/publish/AotSmoke

using System.Buffers;
using System.Diagnostics;
using NewLife;
using NewLife.Data;
using NewLife.Log;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Grpc;
using NewLife.RocketMQ.Models;
using NewLife.RocketMQ.Protocol;
using NewLife.Reflection;
using NewLife.Serialization;

var failures = new List<String>();
var count = 0;

void Check(String name, Action action)
{
    count++;
    try
    {
        action();
        Console.WriteLine($"[PASS] {name}");
    }
    catch (Exception ex)
    {
        failures.Add($"{name} => {ex.Message}");
        Console.WriteLine($"[FAIL] {name} => {ex.Message}");
    }
}

async Task CheckAsync(String name, Func<Task> action)
{
    count++;
    try
    {
        await action();
        Console.WriteLine($"[PASS] {name}");
    }
    catch (Exception ex)
    {
        failures.Add($"{name} => {ex.Message}");
        Console.WriteLine($"[FAIL] {name} => {ex.Message}");
    }
}

void Assert(Boolean condition, String message)
{
    if (!condition) throw new Exception(message);
}

// 主题不存在时先创建，再等待 NameServer 路由注册与客户端路由缓存生效（全新 Broker 上首次运行必需）
async Task<Boolean> EnsureTopicReadyAsync(String nameServer, String topic, Int32 seconds = 45)
{
    using (var warm = new Producer { Topic = topic, NameServerAddress = nameServer })
    {
        warm.Start();
        warm.CreateTopic(topic, 4);
    }

    for (var i = 0; i < seconds; i++)
    {
        try
        {
            using var probe = new Producer { Topic = topic, NameServerAddress = nameServer };
            probe.Start();
            var sent = await probe.PublishAsync("aot-smoke-warm");
            if (sent.Status == NewLife.RocketMQ.Protocol.SendStatus.SendOK) return true;
        }
        catch
        {
            // 路由尚未就绪，重试
        }

        await Task.Delay(1000);
    }

    return false;
}

// 基准模式：--probe 打印版本后立即退出（供外部测量启动开销），--bench 运行 CPU 微基准并输出 BENCH|名称|ops/s|毫秒
void Benchmark(String name, Int32 iterations, Action action)
{
    // 预热，避免首次调用的一次性开销计入结果
    for (var i = 0; i < iterations / 10; i++) action();

    var watch = Stopwatch.StartNew();
    for (var i = 0; i < iterations; i++) action();
    watch.Stop();

    var ops = iterations / watch.Elapsed.TotalSeconds;
    Console.WriteLine($"BENCH|{name}|{ops:n0}|{watch.Elapsed.TotalMilliseconds:n0}");
}

if (args.Contains("--probe"))
{
    Console.WriteLine($"AOT PROBE {Environment.Version} {Environment.ProcessId}");
    return 0;
}

if (args.Contains("--bench"))
{
    var benchCodec = new MqCodec();
    var benchCommand = new Command
    {
        Header = new Header
        {
            Code = (Int32)RequestCode.SEND_MESSAGE_V2,
            Opaque = 1234,
            Language = LanguageCode.DOTNET + "",
            Version = MQVersion.V4_8_0,
            Remark = "bench",
        },
    };
    benchCommand.SetBody(new ArrayPacket("hello aot benchmark".GetBytes()));

    using (var built = benchCodec.Build(benchCommand)!) { }
    var frameData = benchCodec.Build(benchCommand)!.ReadBytes();

    Benchmark("帧编解码·构建", 200_000, () =>
    {
        using var f = benchCodec.Build(benchCommand)!;
    });

    Benchmark("帧编解码·解析", 200_000, () =>
    {
        var rs = benchCodec.TryParse(new ReadOnlySequence<Byte>(frameData));
        if (rs == null || rs.Value.Invalid) throw new Exception("解析失败");
    });

    var benchExt = new MessageExt { Topic = "bench-topic", Body = "hello aot benchmark".GetBytes(), Tags = "tag" };

    Benchmark("双协议·消息转换", 200_000, () =>
    {
        var g = benchExt.ToGrpcMessage();
        _ = g.ToMessage();
    });

    var benchHeader = new Header { Code = 10, Opaque = 5, Language = "DOTNET" };
    benchHeader.GetExtFields()["a"] = "pg";
    var benchJson = benchHeader.ToJson();

    Benchmark("JSON·序列化", 100_000, () => _ = benchHeader.ToJson());

    Benchmark("JSON·反序列化", 100_000, () => _ = benchJson.ToJsonEntity<Header>());

    return 0;
}

Console.WriteLine($"AOT 冒烟测试启动，运行时版本 {Environment.Version}");

Check("帧编解码·整帧往返", () =>
{
    var codec = new MqCodec();
    var cmd = new Command
    {
        Header = new Header
        {
            Code = (Int32)RequestCode.SEND_MESSAGE_V2,
            Opaque = 1234,
            Language = LanguageCode.DOTNET + "",
            Version = MQVersion.V4_8_0,
            Remark = "AOT",
        },
    };
    cmd.SetBody(new ArrayPacket("hello aot".GetBytes()));

    using var frame = codec.Build(cmd)!;
    var data = frame.ReadBytes();

    var rs = codec.TryParse(new ReadOnlySequence<Byte>(data));
    Assert(rs != null && !rs.Value.Invalid, "解析结果无效");

    var parsed = rs!.Value.Message as Command;
    Assert(parsed?.Header != null, "未解析出请求头");
    Assert(parsed!.Header!.Code == (Int32)RequestCode.SEND_MESSAGE_V2, "Code 不一致");
    Assert(parsed.Header.Opaque == 1234, "Opaque 不一致");
    Assert(parsed.Header.Remark == "AOT", "Remark 不一致");
    Assert(rs.Value.BodyLength == "hello aot".GetBytes().Length, "BodyLength 不一致");
    Assert(rs.Value.HeaderSize + rs.Value.BodyLength == data.Length, "帧长不一致");
});

Check("帧编解码·头部构建与流式体", () =>
{
    var codec = new MqCodec();
    var cmd = new Command
    {
        Header = new Header { Code = 11, Opaque = 88 },
    };

    using var head = codec.BuildHeader(cmd, 4096)!;
    var bytes = head.ReadBytes();

    var rs = codec.TryParse(new ReadOnlySequence<Byte>(bytes));
    Assert(rs != null && !rs.Value.Invalid, "解析结果无效");
    Assert(rs!.Value.HeaderSize == bytes.Length, "HeaderSize 应为头部长度");
    Assert(rs.Value.BodyLength == 4096, "BodyLength 应为声明值");
});

Check("帧编解码·坏帧上报与响应配对", () =>
{
    var codec = new MqCodec();

    var bad = codec.TryParse(new ReadOnlySequence<Byte>(new Byte[] { 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x10 }));
    Assert(bad != null && bad.Value.Invalid, "坏帧未上报 Invalid");

    var req = new Command { Header = new Header { Code = 1, Opaque = 66 } };
    var res = new Command { Reply = true, Header = new Header { Code = 0, Opaque = 66 } };
    var push = new Command { Header = new Header { Code = 2, Opaque = 66 } };
    Assert(codec.Match(req, res), "同 Opaque 应答未配对");
    Assert(!codec.Match(req, push), "服务端推送不参与配对");
});

Check("请求头·属性字典", () =>
{
    var send = new SendMessageRequestHeader
    {
        ProducerGroup = "pg",
        Topic = "tp",
        DefaultTopicQueueNums = 4,
        QueueId = 3,
        Batch = true,
        UnitMode = false,
    };
    var sendDic = send.GetProperties();
    Assert(sendDic.Count == 14, $"SendMessageRequestHeader 属性数应为 14，实际 {sendDic.Count}");
    Assert(sendDic["a"] + "" == "pg", "a=ProducerGroup");
    Assert(sendDic["k"] is Boolean, "k 应为布尔原值");
    Assert(sendDic["m"] + "" == "True", "m=Batch");

    var pull = new PullMessageRequestHeader
    {
        ConsumerGroup = "cg",
        Topic = "tp",
        QueueId = 2,
        QueueOffset = 100,
        SuspendTimeoutMillis = 20_000,
    };
    var pullDic = pull.GetProperties();
    Assert(pullDic.Count == 11, $"PullMessageRequestHeader 属性数应为 11，实际 {pullDic.Count}");
    Assert(pullDic["consumerGroup"] + "" == "cg", "consumerGroup");
    Assert(pullDic["queueOffset"] + "" == "100", "queueOffset");
    Assert(pullDic.Keys.First() + "" == "commitOffset", "键应按序排序");

    var end = new EndTransactionRequestHeader
    {
        ProducerGroup = "pg",
        MsgId = "m1",
        CommitOrRollback = 1,
    };
    var endDic = end.GetProperties();
    Assert(endDic.Count == 7, $"EndTransactionRequestHeader 属性数应为 7，实际 {endDic.Count}");
    Assert(endDic["msgId"] + "" == "m1", "msgId");
    Assert(endDic["commitOrRollback"] + "" == "1", "commitOrRollback");
});

Check("JSON·模型与字典序列化", () =>
{
    // 先确认源生成的成员访问器已注册，且裁剪后成员仍可反射枚举（NewLife 序列化依赖两者）
    var getter = MemberAccessorTable.TryGetGetter(typeof(HeartbeatData), "ClientID");
    Assert(getter != null, "HeartbeatData.ClientID 未注册成员访问器");

    var props = typeof(HeartbeatData).GetProperties().Length;
    Assert(props >= 3, $"HeartbeatData 反射可见属性数={props}，成员可能被裁剪");

    var header = new Header { Code = 10, Opaque = 5, Language = "DOTNET" };
    header.GetExtFields()["a"] = "pg";
    var json = header.ToJson();
    Assert(json.Contains("\"code\":10"), $"Header JSON 缺少 code：{json}");

    var back = json.ToJsonEntity<Header>();
    Assert(back != null && back.Code == 10 && back.Opaque == 5, "Header JSON 往返失败");

    var heartbeat = new HeartbeatData { ClientID = "cid" };
    heartbeat.ConsumerDataSet = [new ConsumerData { GroupName = "g1", ConsumeType = "CONSUME_PASSIVELY" }];
    var hbJson = heartbeat.ToJson(false, false, true);
    Assert(hbJson.Contains("\"clientID\":\"cid\""), $"心跳 JSON 缺少 clientID：{hbJson}");
    Assert(hbJson.Contains("\"groupName\":\"g1\""), $"心跳 JSON 缺少 groupName：{hbJson}");

    var dic = new Dictionary<String, Object> { ["topic"] = "tp", ["maxNum"] = 32, ["isForce"] = false };
    var dicJson = dic.ToJson(false, false, false);
    // NewLife 的 JSON 写入器忽略布尔默认值 false（匿名对象同样如此），此处仅保证键值可用
    Assert(dicJson.Contains("\"topic\":\"tp\"") && dicJson.Contains("\"maxNum\":32"), $"字典 JSON 不一致：{dicJson}");

    var queue = new MessageQueue { Topic = "tp", BrokerName = "bn", QueueId = 1 };
    Assert(JsonHelper.Default.Write(queue, false, false, true).Contains("\"queueId\":1"), "MessageQueue JSON 不一致");
});

Check("消息体·对象体序列化", () =>
{
    var msg = new Message();
    msg.SetBody(new Dictionary<String, Object> { ["x"] = 1, ["s"] = "v" });
    Assert(msg.BodyString == "{\"x\":1,\"s\":\"v\"}", $"对象体 JSON 不一致：{msg.BodyString}");

    msg.SetBody("plain");
    Assert(msg.BodyString == "plain", "字符串体应原样使用");
});

Check("云厂商适配", () =>
{
    // Apache ACL：不转换 Topic/Group，不提供 NameServer 地址
    var acl = new AclProvider { AccessKey = "k", SecretKey = "s" };
    Assert(!acl.AclEnabled, "AclProvider.AclEnabled 默认应为 false");
    Assert(acl.ResourceType == 0 && acl.ResourceName == null, "ACL 2.0 资源默认值不符");
    Assert(!acl.Name.IsNullOrEmpty(), "AclProvider.Name 不应为空");
    Assert(acl.TransformTopic("tp") == "tp", "Apache ACL 不应转换 Topic");
    Assert(acl.TransformGroup("GID_x") == "GID_x", "Apache ACL 不应转换 Group");
    Assert(acl.GetNameServerAddress() == null, "Apache ACL 不应提供 NameServer 地址");

    var acl2 = new AclProvider { AccessKey = "k", SecretKey = "s", AclEnabled = true, ResourceType = 1, ResourceName = "tp" };
    Assert(acl2.AclEnabled && acl2.ResourceType == 1 && acl2.ResourceName == "tp", "ACL 2.0 属性设置失败");

    // 阿里云：实例 ID 前缀转换
    var aliyun = new AliyunProvider { AccessKey = "k", SecretKey = "s", InstanceId = "MQ_INST_1" };
    Assert(aliyun.TransformTopic("tp") == "MQ_INST_1%tp", "阿里云 Topic 前缀转换失败");
    Assert(aliyun.TransformGroup("GID_x") == "MQ_INST_1%GID_x", "阿里云 Group 前缀转换失败");

    var aliyun2 = new AliyunProvider { AccessKey = "k", SecretKey = "s" };
    Assert(aliyun2.TransformTopic("tp") == "tp", "阿里云无实例 ID 不应转换 Topic");

    // 华为云：不转换 Topic/Group，EnableSsl 默认关闭
    var huawei = new HuaweiProvider { AccessKey = "k", SecretKey = "s", InstanceId = "HW_INST_1" };
    Assert(huawei.TransformTopic("tp") == "tp", "华为云不应转换 Topic");
    Assert(!huawei.EnableSsl, "HuaweiProvider.EnableSsl 默认应为 false");

    // 腾讯云：命名空间前缀转换
    var tencent = new TencentProvider { AccessKey = "k", SecretKey = "s", Namespace = "ns_x" };
    Assert(tencent.TransformTopic("tp") == "ns_x%tp", "腾讯云 Topic 前缀转换失败");
    Assert(tencent.TransformGroup("GID_x") == "ns_x%GID_x", "腾讯云 Group 前缀转换失败");

    var tencent2 = new TencentProvider { AccessKey = "k", SecretKey = "s" };
    Assert(tencent2.TransformTopic("tp") == "tp", "腾讯云无命名空间不应转换 Topic");

    // 接口一致性
    foreach (var provider in new ICloudProvider[] { aliyun, huawei, tencent, acl })
    {
        Assert(!provider.Name.IsNullOrEmpty(), $"{provider.GetType().Name}.Name 不应为空");
    }
});

Check("双协议·消息模型转换", () =>
{
    var msg = new Message
    {
        Topic = "round-trip",
        Body = new Byte[] { 0xAA, 0xBB },
        Tags = "tagX",
    };
    msg.Properties["custom"] = "value";

    var grpcMsg = msg.ToGrpcMessage();
    Assert(grpcMsg.Topic?.Name == "round-trip", "gRPC Topic 映射不一致");
    Assert(grpcMsg.SystemProperties?.Tag == "tagX", "gRPC Tag 映射不一致");
    Assert(grpcMsg.UserProperties.ContainsKey("custom") && grpcMsg.UserProperties["custom"] == "value", "gRPC 用户属性映射不一致");

    var back = grpcMsg.ToMessage();
    Assert(back.Topic == "round-trip" && back.Tags == "tagX", "往返 Topic/Tags 不一致");
    Assert(back.Body != null && back.Body.Length == 2 && back.Body[0] == 0xAA, "往返 Body 不一致");
    Assert(back.Properties["custom"] == "value", "往返自定义属性不一致");

    var ext = grpcMsg.ToMessageExt();
    Assert(ext.Topic == "round-trip" && ext.Tags == "tagX", "gRPC→MessageExt 映射不一致");
});

Check("埋点·对象标签序列化", () =>
{
    DefaultTracer.Instance = new DefaultTracer();

    var cmd = new Command { Header = new Header { Code = 1, Opaque = 9, Language = "DOTNET" } };
    using var span = DefaultTracer.Instance.NewSpan("mq:test:Receive:1", cmd);
    Assert(span != null, "未创建 Span");
    span!.AppendTag(cmd.Header.ToJson());
    Assert(span.Tag != null && span.Tag.Contains("\"Header\""), $"Command 标签序列化失败：{span.Tag}");
});

Check("gRPC·超时头与访问点解析", () =>
{
    Assert(GrpcClient.FormatTimeout(TimeSpan.FromSeconds(30)) == "30000m", "超时头格式错误");
    Assert(GrpcClient.FormatTimeout(TimeSpan.FromDays(2)) == "172800S", "超时头升单位错误");

    var endpoint = GrpcClient.ParseEndpoints("127.0.0.1:8081");
    Assert(endpoint.Scheme == AddressScheme.IPv4, "访问点地址类型错误");
    Assert(endpoint.Addresses[0].Host == "127.0.0.1" && endpoint.Addresses[0].Port == 8081, "访问点地址错误");

    Assert(GrpcClient.ParseEndpoints("http://mq.example.com:8081").Scheme == AddressScheme.DOMAIN_NAME, "域名访问点解析错误");
});

// 端到端：需要 NameServer 可达
var nameserver = Environment.GetEnvironmentVariable("ROCKETMQ_NAMESERVER");
if (nameserver.IsNullOrEmpty())
{
    Console.WriteLine("[SKIP] 端到端收发：未设置 ROCKETMQ_NAMESERVER");
}
else
{
    var topic = "aot-smoke-topic";
    var group = "aot-smoke-group-" + Guid.NewGuid().ToString("N")[..8];
    var body = "native aot " + DateTime.Now.Ticks;

    try
    {
        if (!await EnsureTopicReadyAsync(nameserver, topic)) throw new Exception($"主题 {topic} 路由未在 45 秒内就绪");

        using var producer = new Producer
        {
            Topic = topic,
            Group = "aot-smoke-producer",
            NameServerAddress = nameserver,
        };
        producer.Start();

        var result = await producer.PublishAsync(body);
        Check("端到端·发送文本", () => Assert(result.Status == SendStatus.SendOK, $"发送状态 {result.Status}"));

        var result2 = await producer.PublishAsync(new Dictionary<String, Object> { ["kind"] = "aot", ["value"] = 42 });
        Check("端到端·发送对象体", () => Assert(result2.Status == SendStatus.SendOK, $"发送状态 {result2.Status}"));

        var result3 = producer.PublishBatch(["aot-batch-1", "aot-batch-2"]);
        Check("端到端·批量发送", () => Assert(result3.Status == SendStatus.SendOK, $"发送状态 {result3.Status}"));

        Check("端到端·延迟发送", () => producer.PublishDelay("aot-delay", DelayTimeLevels.S5));

        var tcs = new TaskCompletionSource<String>();
        using var consumer = new Consumer
        {
            Topic = topic,
            Group = group,
            NameServerAddress = nameserver,
        };
        consumer.OnConsume = (queue, msgs) =>
        {
            foreach (var m in msgs)
            {
                if (m.BodyString == body) tcs.TrySetResult(m.BodyString!);
            }
            return true;
        };
        consumer.Start();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        using var reg = cts.Token.Register(() => tcs.TrySetCanceled());

        try
        {
            var received = await tcs.Task;
            Check("端到端·消费", () => Assert(received == body, "消息内容不一致"));
        }
        catch (TaskCanceledException)
        {
            Check("端到端·消费", () => throw new Exception("30 秒内未收到消息"));
        }

        // 消费后的管理面入口：偏移查询、顺序锁、消费者/连接列表、POP 消费
        var queues = consumer.Queues;
        if (queues == null || queues.Length == 0)
            Console.WriteLine("[SKIP] 端到端·队列相关用例：未获取到已分配队列");
        else
        {
            var mq = queues[0];

            await CheckAsync("端到端·查询最大偏移", async () =>
            {
                var offset = await consumer.QueryMaxOffset(mq);
                Assert(offset >= 0, $"最大偏移 {offset}");
            });

            await CheckAsync("端到端·顺序消费锁", async () =>
            {
                var locked = await consumer.LockBatchMQAsync([mq]);
                Assert(locked != null, "锁定结果为空");
                await consumer.UnlockBatchMQAsync([mq]);
            });

            await CheckAsync("端到端·POP 拉取与确认", async () =>
            {
                // 关闭长轮询（pollTime=0），避免客户端请求超时；此处验证 POP 请求编解码链路可用
                var pr = await consumer.PopMessageAsync(mq.BrokerName, mq.QueueId, maxNums: 8, invisibleTime: 30_000, pollTime: 0);
                Assert(pr != null, "POP 结果为空");
                if (pr!.Messages != null && pr.Messages.Length > 0 && !pr.Messages[0].PopCheckPoint.IsNullOrEmpty())
                {
                    var acked = await consumer.AckMessageAsync(mq.BrokerName, pr.Messages[0]);
                    Assert(acked, "POP 确认失败");
                }
            });
        }

        await CheckAsync("端到端·消费者列表", async () =>
        {
            var cs = await consumer.GetConsumers();
            Assert(cs != null, "消费者列表为空");
        });

        await CheckAsync("端到端·连接列表", async () =>
        {
            var conn = await consumer.GetConsumerConnectionList();
            Assert(conn != null, "连接列表为空");
        });
    }
    catch (Exception ex)
    {
        failures.Add($"端到端异常 => {ex.Message}");
        Console.WriteLine($"[FAIL] 端到端异常 => {ex}");
    }
}

// gRPC 链路：需要 Proxy 可达（ROCKETMQ_GRPC_PROXY，如 http://127.0.0.1:8081），并依赖 NameServer 建主题
var proxy = Environment.GetEnvironmentVariable("ROCKETMQ_GRPC_PROXY");
if (proxy.IsNullOrEmpty())
{
    Console.WriteLine("[SKIP] gRPC 链路：未设置 ROCKETMQ_GRPC_PROXY");
}
else if (nameserver.IsNullOrEmpty())
{
    Console.WriteLine("[SKIP] gRPC 链路：未设置 ROCKETMQ_NAMESERVER，无法创建主题");
}
else
{
    var grpcTopic = "aot-smoke-grpc-topic";
    var grpcGroup = "aot-smoke-grpc-" + Guid.NewGuid().ToString("N")[..8];

    try
    {
        if (!await EnsureTopicReadyAsync(nameserver, grpcTopic)) throw new Exception($"主题 {grpcTopic} 路由未在 45 秒内就绪");

        // gRPC 不提供建主题接口，主题先由 Remoting 创建
        using (var warm = new Producer { Topic = grpcTopic, NameServerAddress = nameserver })
        {
            warm.Start();
            await warm.PublishAsync("aot-grpc-warm");
        }

        using var grpcProducer = new Producer
        {
            Topic = grpcTopic,
            Group = "aot-smoke-grpc-producer",
            NameServerAddress = nameserver,
            GrpcProxyAddress = proxy,
        };
        grpcProducer.Start();

        var grpcResult = await grpcProducer.PublishAsync("aot-grpc-body-" + DateTime.Now.Ticks);
        Check("gRPC·发送文本", () => Assert(grpcResult.Status == SendStatus.SendOK, $"发送状态 {grpcResult.Status}"));

        using var grpcConsumer = new Consumer
        {
            Topic = grpcTopic,
            Group = grpcGroup,
            NameServerAddress = nameserver,
            GrpcProxyAddress = proxy,
        };
        grpcConsumer.Start();

        // 资源上报长连接需要片刻完成 Proxy 侧注册
        await Task.Delay(1500);

        var grpcRoute = await grpcConsumer.QueryRouteViaGrpcAsync(grpcTopic);
        Check("gRPC·路由查询", () => Assert(grpcRoute.Status?.Code == GrpcCode.OK && grpcRoute.MessageQueues.Count > 0, $"路由状态 {grpcRoute.Status?.Code}"));

        var grpcReceived = 0;
        for (var i = 0; i < 3 && grpcReceived == 0; i++)
        {
            foreach (var q in grpcRoute.MessageQueues)
            {
                var msgs = await grpcConsumer.ReceiveMessageViaGrpcAsync(q, batchSize: 8, longPollingTimeout: TimeSpan.FromSeconds(3));
                if (msgs.Count == 0) continue;

                grpcReceived = msgs.Count;
                var ack = await grpcConsumer.AckMessageViaGrpcAsync(grpcTopic,
                [
                    .. msgs.Select(m => new AckMessageEntry { ReceiptHandle = m.SystemProperties?.ReceiptHandle, MessageId = m.SystemProperties?.MessageId }),
                ]);
                Check("gRPC·接收与确认", () => Assert(ack?.Status?.Code == GrpcCode.OK, $"确认状态 {ack?.Status?.Code}"));
                break;
            }
        }
        if (grpcReceived == 0) Check("gRPC·接收与确认", () => throw new Exception("未收到消息"));

        // 推送消费（PUSH_CONSUMER 语义，走长轮询与自动 Ack）
        var pushReceived = new List<String>();
        using var push = new GrpcPushConsumer
        {
            Topic = grpcTopic,
            Group = grpcGroup + "-push",
            Endpoints = proxy,
            LongPollingTimeout = TimeSpan.FromSeconds(3),
            OnMessage = m =>
            {
                lock (pushReceived) pushReceived.Add(m.Body?.ToStr());
                return Task.FromResult(true);
            },
        };
        await push.StartAsync();

        using (var warm = new Producer { Topic = grpcTopic, NameServerAddress = nameserver })
        {
            warm.Start();
            await warm.PublishAsync("aot-grpc-push");
        }

        var pushWatch = Stopwatch.StartNew();
        while (pushWatch.ElapsedMilliseconds < 20_000 && pushReceived.Count == 0) await Task.Delay(500);
        lock (pushReceived) Check("gRPC·推送消费", () => Assert(pushReceived.Count > 0, "20 秒内未收到推送消息"));

        await push.StopAsync();

        // 死信转发：接收后不确认，直接转发到 %DLQ%{group}
        using (var warm = new Producer { Topic = grpcTopic, NameServerAddress = nameserver })
        {
            warm.Start();
            await warm.PublishAsync("aot-grpc-dlq");
        }

        GrpcMessage dlqMsg = null;
        for (var i = 0; i < 6 && dlqMsg == null; i++)
        {
            foreach (var q in grpcRoute.MessageQueues)
            {
                var msgs = await grpcConsumer.ReceiveMessageViaGrpcAsync(q, batchSize: 8,
                    invisibleDuration: TimeSpan.FromSeconds(10), longPollingTimeout: TimeSpan.FromSeconds(3));
                dlqMsg = msgs.FirstOrDefault(m => m.Body?.ToStr() == "aot-grpc-dlq");
                if (dlqMsg != null) break;
            }

            if (dlqMsg == null) await Task.Delay(1000);
        }

        if (dlqMsg == null)
            Check("gRPC·转发死信队列", () => throw new Exception("未收到待转发消息"));
        else
        {
            var attempt = dlqMsg.SystemProperties?.DeliveryAttempt ?? 1;
            var dlq = await grpcConsumer.ForwardToDeadLetterQueueViaGrpcAsync(grpcTopic,
                dlqMsg.SystemProperties!.ReceiptHandle, dlqMsg.SystemProperties.MessageId, attempt, attempt);
            Check("gRPC·转发死信队列", () => Assert(dlq?.Status?.Code == GrpcCode.OK, $"转发状态 {dlq?.Status?.Code}"));
        }

        // FIFO 顺序消费：单队列 FIFO 主题 + 顺序处理，验证接收顺序与发送顺序一致
        var fifoTopic = "aot-smoke-fifo-topic";
        using (var warm = new Producer { Topic = fifoTopic, NameServerAddress = nameserver })
        {
            warm.Start();
            warm.CreateTopic(fifoTopic, 1, 0, TopicMessageType.Fifo);
        }

        var fifoReady = false;
        for (var i = 0; i < 45 && !fifoReady; i++)
        {
            try
            {
                using var probe = new Producer { Topic = fifoTopic, NameServerAddress = nameserver };
                probe.Start();
                var r = await probe.PublishAsync(new Message { Topic = fifoTopic, Body = "fifo-warm".GetBytes(), MessageGroup = "warm" }, null);
                fifoReady = r.Status == SendStatus.SendOK;
            }
            catch { }

            if (!fifoReady) await Task.Delay(1000);
        }

        if (!fifoReady) Check("gRPC·FIFO 顺序消费", () => throw new Exception($"主题 {fifoTopic} 路由未就绪"));
        else
        {
            const Int32 fifoTotal = 20;
            var fifoGroup = "aot-fifo-" + Guid.NewGuid().ToString("N")[..8];
            using (var fifoProducer = new Producer
            {
                Topic = fifoTopic,
                Group = "aot-fifo-producer",
                NameServerAddress = nameserver,
                GrpcProxyAddress = proxy,
            })
            {
                fifoProducer.Start();
                for (var i = 0; i < fifoTotal; i++)
                {
                    await fifoProducer.PublishAsync(new Message
                    {
                        Topic = fifoTopic,
                        Body = $"seq-{i:d3}".GetBytes(),
                        MessageGroup = fifoGroup,
                    }, null);
                }
            }

            var fifoReceived = new List<Int32>();
            using var fifoPush = new GrpcPushConsumer
            {
                Topic = fifoTopic,
                Group = "aot-fifo-consumer-" + Guid.NewGuid().ToString("N")[..8],
                Endpoints = proxy,
                Orderly = true,
                InvisibleDuration = TimeSpan.FromSeconds(30),
                OnMessage = m =>
                {
                    var body = m.Body.ToStr();
                    if (body.StartsWith("seq-"))
                    {
                        lock (fifoReceived) fifoReceived.Add(Int32.Parse(body[4..]));
                    }

                    return Task.FromResult(true);
                },
            };
            await fifoPush.StartAsync();

            var fifoWatch = Stopwatch.StartNew();
            while (fifoWatch.ElapsedMilliseconds < 30_000 && fifoReceived.Count < fifoTotal) await Task.Delay(200);
            await fifoPush.StopAsync();

            lock (fifoReceived)
            {
                Check("gRPC·FIFO 顺序消费", () =>
                {
                    Assert(fifoReceived.Count == fifoTotal, $"仅收到 {fifoReceived.Count}/{fifoTotal} 条");
                    for (var i = 0; i < fifoTotal; i++) Assert(fifoReceived[i] == i, $"顺序错误：第 {i} 条为 {fifoReceived[i]}");
                });
            }
        }
    }
    catch (Exception ex)
    {
        failures.Add($"gRPC 链路异常 => {ex.Message}");
        Console.WriteLine($"[FAIL] gRPC 链路异常 => {ex}");
    }
}

Console.WriteLine();
Console.WriteLine($"用例 {count} 个，失败 {failures.Count} 个");
foreach (var item in failures) Console.WriteLine($"  - {item}");

return failures.Count == 0 ? 0 : 1;
