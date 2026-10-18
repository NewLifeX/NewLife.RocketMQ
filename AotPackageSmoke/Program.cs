// RocketMQ NuGet 包消费 AOT 冒烟
//
// 用途：以 PackageReference（而非 ProjectReference）消费本仓库打出的 nupkg，发布为 NativeAOT 原生可执行文件后运行，
//   验证「用户装包后能否 AOT 发布」这条真实路径：包内 lib 资产、依赖声明、传递的源生成器与裁剪元数据是否齐备，
//   并确认帧编解码、JSON 序列化、消息压缩、云厂商适配在裁剪后仍然可用。
// 环境变量 ROCKETMQ_NAMESERVER（如 127.0.0.1:9876）可选；设置后追加真实收发用例。

using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.IO.Compression;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text.Json.Serialization;
using NewLife;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Protocol;
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

if (args.Contains("--probe"))
{
    Console.WriteLine($"PACKAGE AOT PROBE {Environment.Version} {Environment.ProcessId}");
    return 0;
}

Console.WriteLine($"NuGet 包消费冒烟启动，运行时版本 {Environment.Version}");
Console.WriteLine($"程序集：{typeof(Message).Assembly.GetName().FullName}");

// 用作消息体的自定义类型需登记：NativeAOT 下裁剪器才会保留其公开属性，否则对象体被静默序列化为空对象
MqAot.RegisterBodyType<SmokeDto>();

Check("包资产·类型可用", () =>
{
    // 打包后 lib/net10.0 资产与依赖声明必须可用，否则此处会以 TypeLoadException / FileNotFoundException 失败
    Assert(typeof(Producer).Assembly == typeof(Message).Assembly, "生产者与协议类型应来自同一程序集");
    Assert(typeof(Consumer).Assembly.GetName().Version!.Major == 3, $"版本异常：{typeof(Consumer).Assembly.GetName().Version}");
});

Check("帧编解码·整帧往返", () =>
{
    var codec = new MqCodec();
    var cmd = new Command
    {
        Header = new Header { Code = 10, Opaque = 4321, Language = "DOTNET" }
    };
    cmd.Header.GetExtFields()["keyA"] = "valueA";
    cmd.SetBody(new NewLife.Data.ArrayPacket(new Byte[] { 7, 8, 9 }));

    var pk = codec.Build(cmd);
    Assert(pk != null, "Build 返回 null");
    var frame = pk!.GetSpan().ToArray();
    pk.Dispose();

    Assert(frame.Length == 4 + ((frame[0] << 24) + (frame[1] << 16) + (frame[2] << 8) + frame[3]), "帧长不一致");

    var parsed = codec.TryParse(new ReadOnlySequence<Byte>(frame));
    Assert(parsed != null && !parsed.Value.Invalid, "整帧未被识别");

    var result = parsed!.Value;
    if (result.Message is not Command got) throw new Exception("定界消息类型不匹配");
    Assert(got.Header.Opaque == 4321, $"定界消息 Opaque={got.Header.Opaque}");
    Assert(got.Header.GetExtFields()["keyA"] == "valueA", "扩展字段丢失");

    var res = new Command { Reply = true, Header = new Header { Opaque = 4321 } };
    Assert(codec.Match(cmd, res), "响应未配对");
});

Check("JSON·模型序列化（源生成访问器）", () =>
{
    // 裁剪导致成员访问器缺失时，这里会序列化为空对象
    var queue = new MessageQueue { Topic = "tp", BrokerName = "bn", QueueId = 1 };
    var json = JsonHelper.Default.Write(queue, false, false, true);
    Assert(json.Contains("\"queueId\":1"), $"MessageQueue JSON 不一致：{json}");

    var header = new Header { Code = 10, Opaque = 4321, Language = "DOTNET" };
    var hjson = header.ToJson();
    Assert(hjson.Contains("\"opaque\":4321"), $"Header JSON 不一致：{hjson}");
});

Check("消息体·对象体序列化", () =>
{
    var msg = new Message();
    msg.SetBody(new Dictionary<String, Object> { ["x"] = 1, ["s"] = "v" });
    Assert(msg.BodyString == "{\"x\":1,\"s\":\"v\"}", $"对象体 JSON 不一致：{msg.BodyString}");
});

Check("消息体·自定义类型序列化", () =>
{
    // 未登记的消费方类型：成员元数据被裁剪，序列化为空对象 {}
    var plain = new Message();
    plain.SetBody(new RawDto { Name = "n", Value = 7 });
    Console.WriteLine($"    diag: 未登记               = {plain.BodyString}");

    // 仅登记 JsonSerializerContext（成员访问器）：成员元数据仍被裁剪
    var registered = new Message();
    registered.SetBody(new AccessorOnlyDto { Name = "n", Value = 7 });
    Console.WriteLine($"    diag: 仅登记访问器         = {registered.BodyString}");

    // 仅声明元数据根（DynamicDependency）
    var rootOnly = new Message();
    rootOnly.SetBody(new RootOnlyDto { Name = "n", Value = 7 });
    Console.WriteLine($"    diag: 仅声明元数据根       = {rootOnly.BodyString}");

    // 库提供的登记入口：一行调用即保留成员元数据
    var full = new Message();
    full.SetBody(new SmokeDto { Name = "n", Value = 7 });
    Console.WriteLine($"    diag: MqAot.RegisterBodyType = {full.BodyString}");
    Assert(full.BodyString != null && full.BodyString.Contains("\"Name\":\"n\""), $"登记后仍序列化为 {full.BodyString}");
});

Check("消息压缩·ZLIB 往返与上限", () =>
{
    var compressor = MessageCompressorRegistry.GetOrThrow(0);
    var raw = new Byte[32 * 1024];
    for (var i = 0; i < raw.Length; i++) raw[i] = (Byte)(i % 251);

    var packed = compressor.Compress(raw);
    Assert(packed[0] == 0x78 && packed[1] == 0x9C, "ZLIB 头部不符");
    Assert(compressor.Decompress(packed).Length == raw.Length, "解压长度不一致");

    var bomb = compressor.Compress(new Byte[10 * 1024 * 1024]);
    // 默认上限面向大消息放得较宽，这里用显式小上限实例验证拦截逻辑
    var guarded = new ZlibMessageCompressor { MaxDecompressSize = 4 * 1024 * 1024 };
    var caught = false;
    try
    {
        guarded.Decompress(bomb);
    }
    catch (InvalidDataException ex)
    {
        caught = ex.Message.Contains("上限");
    }
    Assert(caught, "超出解压上限未按预期拦截");
});

Check("云厂商适配·ACL 与 HMAC-SHA1", () =>
{
    var acl = new AclProvider { AccessKey = "k", SecretKey = "s", AclEnabled = true, ResourceType = 1, ResourceName = "tp" };
    Assert(acl.AclEnabled && acl.ResourceType == 1 && acl.ResourceName == "tp", "ACL 2.0 属性设置失败");
    Assert(acl.TransformTopic("tp") == "tp", "Apache ACL 不应转换 Topic");

    // RFC 2202 用例：确认裁剪加密原语与十六进制转换可用
    using var hmac = new HMACSHA1("Jefe".GetBytes());
    Assert(hmac.ComputeHash("what do ya want for nothing?".GetBytes()).ToHex() == "EFFCDF6AE5EB2FA2D27416D5F184DF9C259A7C79", "HMAC-SHA1 结果不符");
});

// 可选：Pack 步骤之后 Broker 若可达，验证消费端真实收发（含发送端压缩 + 消费端解压）
var nameserver = Environment.GetEnvironmentVariable("ROCKETMQ_NAMESERVER");
if (nameserver.IsNullOrEmpty())
{
    Console.WriteLine("[SKIP] 端到端收发：未设置 ROCKETMQ_NAMESERVER");
}
else
{
    try
    {
        var topic = "aot-package-smoke";
        var group = "aot-package-group-" + Guid.NewGuid().ToString("N")[..8];

        using (var warm = new Producer { Topic = topic, NameServerAddress = nameserver })
        {
            warm.Start();
            warm.CreateTopic(topic, 4);
        }

        using var producer = new Producer { Topic = topic, Group = "aot-package-producer", NameServerAddress = nameserver };
        producer.Start();

        var large = "PKG-LARGE-" + new String('y', 100_000);
        var sent = await producer.PublishAsync(large);

        var tcs = new TaskCompletionSource<Int32>();
        using var consumer = new Consumer { Topic = topic, Group = group, NameServerAddress = nameserver };
        consumer.OnConsume = (queue, msgs) =>
        {
            foreach (var m in msgs)
            {
                if (m.BodyString != null && m.BodyString.StartsWith("PKG-LARGE-")) tcs.TrySetResult(m.BodyString.Length);
            }
            return true;
        };
        consumer.Start();

        var length = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(30));
        Check("端到端·发送与消费（含压缩往返）", () =>
        {
            Assert(sent.Status == SendStatus.SendOK, $"发送状态 {sent.Status}");
            Assert(length == large.Length, $"还原长度 {length}，应为 {large.Length}");
        });
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

/// <summary>消费方自定义类型的 AOT 元数据根：与库内 MqJsonAot 同套路，裁剪器据此保留公开属性</summary>
internal static class SmokeAot
{
    [ModuleInitializer]
    internal static void Initialize() => Preserve();

    [DynamicDependency(DynamicallyAccessedMemberTypes.PublicProperties, typeof(RootOnlyDto))]
    private static void Preserve() { }
}

/// <summary>登记消费方自定义类型：NewLife.Core 源生成器据此为本工程注册 JSON 成员访问器</summary>
[JsonSerializable(typeof(SmokeDto))]
[JsonSerializable(typeof(AccessorOnlyDto))]
internal partial class SmokeJsonContext : JsonSerializerContext { }

/// <summary>访问器与元数据根齐备的自定义类型</summary>
internal sealed class SmokeDto
{
    /// <summary>名称</summary>
    public String? Name { get; set; }

    /// <summary>数值</summary>
    public Int32 Value { get; set; }
}

/// <summary>仅登记访问器、无元数据根的自定义类型</summary>
internal sealed class AccessorOnlyDto
{
    /// <summary>名称</summary>
    public String? Name { get; set; }

    /// <summary>数值</summary>
    public Int32 Value { get; set; }
}

/// <summary>仅声明元数据根、无访问器登记的自定义类型</summary>
internal sealed class RootOnlyDto
{
    /// <summary>名称</summary>
    public String? Name { get; set; }

    /// <summary>数值</summary>
    public Int32 Value { get; set; }
}

/// <summary>两者皆无的自定义类型，用于观测裁剪边界</summary>
internal sealed class RawDto
{
    /// <summary>名称</summary>
    public String? Name { get; set; }

    /// <summary>数值</summary>
    public Int32 Value { get; set; }
}
