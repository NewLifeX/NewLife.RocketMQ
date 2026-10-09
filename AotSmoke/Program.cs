// RocketMQ NativeAOT 冒烟测试
//
// 用途：以 NativeAOT（PublishAot=true）发布为原生可执行文件后运行，验证裁剪环境下以下链路仍然可用：
//   帧编解码、请求头属性字典、JSON 序列化（含源生成成员访问器）、云厂商适配、消息体对象序列化、端到端收发。
// 环境变量 ROCKETMQ_NAMESERVER（如 127.0.0.1:9876）可选；设置后追加真实收发用例，未设置时跳过。
//
// 本地：dotnet publish AotSmoke -c Release -r win-x64 -p:PublishAot=true && ./AotSmoke/bin/Release/net10.0/win-x64/publish/AotSmoke.exe
// CI  ：dotnet publish AotSmoke -c Release -r linux-x64 -p:PublishAot=true && ./AotSmoke/bin/Release/net10.0/linux-x64/publish/AotSmoke

using System.Buffers;
using NewLife;
using NewLife.Data;
using NewLife.Log;
using NewLife.RocketMQ;
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

void Assert(Boolean condition, String message)
{
    if (!condition) throw new Exception(message);
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
    var aliyun = new AliyunProvider { AccessKey = "k", SecretKey = "s", InstanceId = "MQ_INST_1" };
    Assert(aliyun.TransformTopic("tp") == "MQ_INST_1%tp", "阿里云 Topic 前缀转换失败");
    Assert(aliyun.TransformGroup("GID_x") == "MQ_INST_1%GID_x", "阿里云 Group 前缀转换失败");

    var acl = new AclProvider { AccessKey = "k", SecretKey = "s" };
    Assert(acl.TransformTopic("tp") == "tp", "Apache ACL 不应转换 Topic");
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
    }
    catch (Exception ex)
    {
        failures.Add($"端到端异常 => {ex.Message}");
        Console.WriteLine($"[FAIL] 端到端异常 => {ex}");
    }
}

Console.WriteLine();
Console.WriteLine($"用例 {count} 个，失败 {failures.Count} 个");
foreach (var item in failures) Console.WriteLine($"  - {item}");

return failures.Count == 0 ? 0 : 1;
