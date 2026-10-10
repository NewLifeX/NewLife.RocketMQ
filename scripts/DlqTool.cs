#:project c:\X\NewLife.RocketMQ\NewLife.RocketMQ\NewLife.RocketMQ.csproj
// ============================================================
// RocketMQ gRPC 死信队列运维工具（需要 .NET 10 SDK）
//
//   重投死信消息：dotnet run --file scripts/DlqTool.cs -- --group <消费组> --to <原主题> [--limit N]
//
//   参数：--group NAME   消费组（必填，死信主题为 %DLQ%{group}）
//         --to TOPIC     重投目标主题（必填）
//         --limit N      单次最多处理条数，默认 100
//         --nameserver   NameServer 地址，默认 127.0.0.1:9876
//         --proxy        gRPC Proxy 地址，默认 http://127.0.0.1:8081
//
//   说明：死信消息不携带原主题属性，重投目标需由 --to 指定。
//   流程为「拉取 → 打印 → 重投到目标主题 → 确认死信消息」，只有重投成功的消息才会被确认，
//   失败者保留待下次处理；未确认的死信消息会被 Broker 重新放入 %RETRY%{group}_%DLQ%{group}，
//   因此不建议在队列上做只读浏览，需要用该工具一次性处理。
//   退出码：0=成功、1=参数或执行错误。
// ============================================================

using System.Diagnostics;
using NewLife;
using NewLife.RocketMQ;

var group = GetArg("--group", "");
var target = GetArg("--to", "");
var limit = Int32.TryParse(GetArg("--limit", "100"), out var l) ? l : 100;
var nameserver = GetArg("--nameserver", "127.0.0.1:9876");
var proxy = GetArg("--proxy", "http://127.0.0.1:8081");

if (group.IsNullOrEmpty() || target.IsNullOrEmpty())
{
    Console.Error.WriteLine("用法：--group <消费组> --to <重投目标主题> [--limit N] [--nameserver HOST:PORT] [--proxy URL]");
    return 1;
}

var dlqTopic = $"%DLQ%{group}";
var sw = Stopwatch.StartNew();

using var consumer = new Consumer
{
    Topic = dlqTopic,
    Group = group,
    NameServerAddress = nameserver,
    GrpcProxyAddress = proxy,
};
consumer.Start();

// 资源上报长连接需要片刻完成 Proxy 侧注册
await Task.Delay(3000);

var route = await consumer.QueryRouteViaGrpcAsync(dlqTopic);
if (route.Status?.Code != NewLife.RocketMQ.Grpc.GrpcCode.OK)
{
    Console.WriteLine($"死信主题 {dlqTopic} 不可用（code={route.Status?.Code}），该消费组可能尚无死信消息");
    return 0;
}

using var producer = new Producer { Topic = target, NameServerAddress = nameserver };
producer.Start();

Console.WriteLine($"死信主题 {dlqTopic}，队列 {route.MessageQueues.Count}，重投目标 {target}");

var received = new List<NewLife.RocketMQ.Grpc.GrpcMessage>();
foreach (var queue in route.MessageQueues)
{
    for (var round = 0; round < 10 && received.Count < limit; round++)
    {
        var messages = await consumer.ReceiveMessageViaGrpcAsync(queue, batchSize: 32,
            invisibleDuration: TimeSpan.FromSeconds(30), longPollingTimeout: TimeSpan.FromSeconds(2));
        if (messages.Count == 0) break;
        received.AddRange(messages);
    }
}

Console.WriteLine($"读取死信消息 {received.Count} 条（耗时 {sw.Elapsed.TotalSeconds:n1}s）");

foreach (var msg in received.Take(20))
{
    var body = msg.Body?.ToStr() ?? "";
    if (body.Length > 80) body = body[..80] + "...";
    Console.WriteLine($"  attempt={msg.SystemProperties?.DeliveryAttempt} id={msg.SystemProperties?.MessageId} body={body}");
}
if (received.Count > 20) Console.WriteLine($"  ...（其余 {received.Count - 20} 条省略）");

if (received.Count > 0)
{
    var resent = new List<NewLife.RocketMQ.Grpc.GrpcMessage>();
    foreach (var msg in received)
    {
        try
        {
            var result = await producer.PublishAsync(msg.Body?.ToStr() ?? "", msg.SystemProperties?.Tag, null);
            if (result.Status != NewLife.RocketMQ.Protocol.SendStatus.SendOK)
            {
                Console.Error.WriteLine($"  重投失败：{result.Status}");
                continue;
            }
            resent.Add(msg);
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"  重投异常：{ex.Message}");
        }
    }

    // 仅确认重投成功的消息，失败者保留在死信队列等待下次处理
    var acked = await consumer.AckMessageViaGrpcAsync(dlqTopic,
    [
        .. resent.Select(m => new NewLife.RocketMQ.Grpc.AckMessageEntry
        {
            ReceiptHandle = m.SystemProperties?.ReceiptHandle,
            MessageId = m.SystemProperties?.MessageId,
        }),
    ]);

    Console.WriteLine($"已重投 {resent.Count} 条到 {target}，确认结果 code={acked?.Status?.Code}");
}

Console.WriteLine("完成");
return 0;

String GetArg(String name, String fallback)
{
    for (var i = 0; i < args.Length - 1; i++)
    {
        if (args[i] == name) return args[i + 1];
    }

    return fallback;
}