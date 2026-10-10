// ============================================================
// RocketMQ gRPC（5.x Proxy）长稳验收脚本（需要 .NET 10 SDK）
//
//   运行：dotnet run --file scripts/GrpcStability.cs -- --minutes 5
//   参数：--minutes N      持续分钟数，默认 5
//         --interval N     每轮间隔秒数，默认 15
//         --tolerate N     允许失败次数，默认 0（超过则退出码 1，便于 CI 判定）
//         --min-received N 至少接收条数，默认 1
//         --topic NAME     主题，默认 grpc-stability-topic
//         --topics A,B     多主题订阅（逗号分隔），默认取 --topic；设置多个时每个消费者走 Topics 多主题订阅
//         --consumers N    并发消费者数量，默认 1（各自独立消费组，各自收到全量消息）
//   环境变量：ROCKETMQ_NAMESERVER（默认 127.0.0.1:9876）
//             ROCKETMQ_GRPC_PROXY（默认 http://127.0.0.1:8081）
//
// 每轮：对每个主题投递标记消息 → 每个消费者逐主题逐队列 gRPC 接收 → gRPC 确认，统计收发与异常。
// 运行期间可人为杀掉并拉起 Proxy（CI 由后台命令注入），用于验证长连接重建、重新注册与接收恢复。
// ============================================================

#:project ../NewLife.RocketMQ/NewLife.RocketMQ.csproj

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using NewLife;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Grpc;
using NewLife.RocketMQ.Protocol;

return await GrpcStability.RunAsync(args);

/// <summary>gRPC 长稳验收</summary>
static class GrpcStability
{
    /// <summary>执行长稳验收</summary>
    /// <param name="args">命令行参数</param>
    /// <returns>失败次数超过容忍值或接收不足时返回 1</returns>
    public static async Task<Int32> RunAsync(String[] args)
    {
        var minutes = GetArg(args, "--minutes", GetEnv("ROCKETMQ_STABILITY_MINUTES", 5));
        var interval = GetArg(args, "--interval", 15);
        var tolerate = GetArg(args, "--tolerate", 0);
        var minReceived = GetArg(args, "--min-received", 1);
        var topic = GetStringArg(args, "--topic", "grpc-stability-topic");
        var consumerCount = GetArg(args, "--consumers", 1);
        var poll = GetArg(args, "--poll", 2);

        var topics = GetStringArg(args, "--topics", topic)
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (topics.Length == 0) topics = [topic];

        var nameserver = Environment.GetEnvironmentVariable("ROCKETMQ_NAMESERVER");
        if (nameserver.IsNullOrEmpty()) nameserver = "127.0.0.1:9876";

        var proxy = Environment.GetEnvironmentVariable("ROCKETMQ_GRPC_PROXY");
        if (proxy.IsNullOrEmpty()) proxy = "http://127.0.0.1:8081";

        Console.WriteLine($"[长稳] 计划 {minutes} 分钟，每 {interval} 秒一轮；NameServer={nameserver} Proxy={proxy}");
        Console.WriteLine($"[长稳] 主题=[{String.Join(",", topics)}]，消费者 {consumerCount} 个，允许失败 {tolerate} 次，至少接收 {minReceived} 条");

        var runId = Guid.NewGuid().ToString("N")[..6];

        // 预热：建主题（gRPC 不提供建主题接口），每个主题一个 Remoting 生产者用于投递标记消息
        var publishers = new List<Producer>();
        foreach (var item in topics)
        {
            var producer = new Producer { Topic = item, NameServerAddress = nameserver };
            producer.Start();
            producer.CreateTopic(item, 4);
            publishers.Add(producer);
        }

        // 并发消费者：各自独立消费组，多主题时走 Topics 多主题订阅
        var consumers = new List<StabilityConsumer>();
        for (var i = 0; i < consumerCount; i++)
        {
            var consumer = new StabilityConsumer
            {
                Topic = topics[0],
                Topics = topics.Length > 1 ? topics : null,
                Group = $"grpc-stability-{runId}-{i}",
                NameServerAddress = nameserver,
                GrpcProxyAddress = proxy,
            };
            consumer.Start();

            if (consumer.Telemetry != null)
            {
                consumer.Telemetry.RefreshInterval = TimeSpan.FromSeconds(10);
                consumer.Telemetry.ReconnectInterval = TimeSpan.FromSeconds(2);
            }

            consumers.Add(consumer);
        }

        // 路由就绪：Broker 侧注册有延迟
        var routes = new Dictionary<String, QueryRouteResponse>();
        foreach (var item in topics)
        {
            var route = await QueryRouteAsync(consumers[0], item);
            if (route == null)
            {
                Console.WriteLine($"[长稳] 主题 {item} 路由始终未就绪，退出");
                return 1;
            }

            routes[item] = route;
        }

        var routeText = String.Join("，", topics.Select(t => $"{t}({routes[t].MessageQueues.Count} 队列)"));
        Console.WriteLine($"[长稳] 通道就绪：{routeText}，Telemetry 重建周期 {consumers[0].Telemetry?.RefreshInterval.TotalSeconds}s");

        var sw = Stopwatch.StartNew();
        var rounds = 0;
        var received = 0;
        var acked = 0;
        var failures = new List<String>();
        var perConsumer = new Int32[consumers.Count];
        var targets = new Dictionary<String, MessageQueue>();

        while (sw.Elapsed < TimeSpan.FromMinutes(minutes))
        {
            rounds++;
            var roundGot = 0;

            // 每个主题投递一条标记消息，并记录落到的队列（接收优先打这个队列，避免逐队列空轮询拖慢节奏）
            for (var i = 0; i < topics.Length; i++)
            {
                try
                {
                    var sent = await publishers[i].PublishAsync($"{topics[i]}-{sw.ElapsedMilliseconds}");
                    if (sent.Status != SendStatus.SendOK) failures.Add($"[{sw.Elapsed:mm\\:ss}] 主题 {topics[i]} 发送失败：{sent.Status}");
                    else if (sent.Queue != null) targets[topics[i]] = sent.Queue;
                }
                catch (Exception ex)
                {
                    failures.Add($"[{sw.Elapsed:mm\\:ss}] 主题 {topics[i]} 发送异常 {ex.Message}");
                }
            }

            // 并发消费者各自接收全部主题
            for (var c = 0; c < consumers.Count; c++)
            {
                var consumer = consumers[c];
                foreach (var item in topics)
                {
                    // 优先打本轮消息所在队列；命中即换下一个主题，避免逐队列空轮询拖慢节奏
                    foreach (var queue in QueuesOf(item, routes, targets))
                    {
                        try
                        {
                            var messages = await consumer.ReceiveMessageViaGrpcAsync(queue, batchSize: 8, invisibleDuration: TimeSpan.FromSeconds(30), longPollingTimeout: TimeSpan.FromSeconds(poll));
                            if (messages.Count == 0) continue;

                            var ack = await consumer.AckMessageViaGrpcAsync(item,
                            [
                                .. messages.Select(m => new AckMessageEntry
                                {
                                    ReceiptHandle = m.SystemProperties?.ReceiptHandle,
                                    MessageId = m.SystemProperties?.MessageId,
                                }),
                            ]);

                            if (ack.Status?.Code == GrpcCode.OK) acked += messages.Count;
                            else failures.Add($"[{sw.Elapsed:mm\\:ss}] 消费者 {c} 确认失败：{ack.Status?.Code} {ack.Status?.Message}");

                            perConsumer[c] += messages.Count;
                            roundGot += messages.Count;

                            if (queue.Id == targets.GetValueOrDefault(item)?.QueueId) break;
                        }
                        catch (Exception ex)
                        {
                            failures.Add($"[{sw.Elapsed:mm\\:ss}] 消费者 {c} 主题 {item} 接收异常 {ex.GetType().Name}: {ex.Message}");
                            break;
                        }
                    }
                }
            }

            received += roundGot;

            var active = String.Join("/", consumers.Select(c => c.Telemetry?.Active ?? false));
            Console.WriteLine($"[{sw.Elapsed:mm\\:ss}] 第 {rounds} 轮：接收 {roundGot} 条（累计 {received}，确认 {acked}），异常 {failures.Count}，各消费者 [{String.Join(",", perConsumer)}]，Active=[{active}]");

            await Task.Delay(TimeSpan.FromSeconds(interval));
        }

        Console.WriteLine();
        Console.WriteLine($"[长稳] 结束：{rounds} 轮 × {topics.Length} 主题 × {consumers.Count} 消费者，接收 {received} 条，确认 {acked} 条，异常 {failures.Count} 次");
        Console.WriteLine($"[长稳] 各消费者接收：[{String.Join(",", perConsumer)}]");
        foreach (var item in failures.Take(10)) Console.WriteLine("  - " + item);
        if (failures.Count > 10) Console.WriteLine($"  …… 其余 {failures.Count - 10} 条略");

        var proc = Process.GetCurrentProcess();
        Console.WriteLine($"[长稳] 进程状态：线程 {proc.Threads.Count}，工作集 {proc.WorkingSet64 / 1024 / 1024} MB");

        foreach (var consumer in consumers)
        {
            consumer.Stop();
            consumer.Dispose();
        }

        if (received < minReceived)
        {
            Console.WriteLine($"[长稳] 接收数量不足：{received} < {minReceived}");
            return 1;
        }

        return failures.Count > tolerate ? 1 : 0;
    }

    /// <summary>构造本轮接收队列顺序：优先本轮消息所在队列，再补齐其余队列以保证覆盖</summary>
    /// <param name="topic">主题</param>
    /// <param name="routes">主题路由</param>
    /// <param name="targets">本轮消息所在队列</param>
    /// <returns>队列列表</returns>
    private static IList<GrpcMessageQueue> QueuesOf(String topic, IDictionary<String, QueryRouteResponse> routes, IDictionary<String, MessageQueue> targets)
    {
        var list = new List<GrpcMessageQueue>();

        if (targets.TryGetValue(topic, out var target) && target != null)
        {
            var hit = routes[topic].MessageQueues.FirstOrDefault(q => q.Id == target.QueueId);
            if (hit != null) list.Add(hit);
        }

        foreach (var queue in routes[topic].MessageQueues)
        {
            if (!list.Contains(queue)) list.Add(queue);
        }

        return list;
    }

    /// <summary>查询路由，容忍 Broker 侧路由注册延迟</summary>
    /// <param name="consumer">消费者</param>
    /// <param name="topic">主题</param>
    /// <returns>路由，未就绪返回 null</returns>
    private static async Task<QueryRouteResponse> QueryRouteAsync(Consumer consumer, String topic)
    {
        var sw = Stopwatch.StartNew();
        while (sw.Elapsed < TimeSpan.FromSeconds(60))
        {
            var route = await consumer.QueryRouteViaGrpcAsync(topic);
            if (route.Status?.Code == GrpcCode.OK && route.MessageQueues.Count > 0) return route;

            Console.WriteLine($"[长稳] 路由未就绪：{route.Status?.Code} {route.Status?.Message}");
            await Task.Delay(3000);
        }

        return null;
    }

    private static Int32 GetArg(String[] args, String name, Int32 defaultValue)
    {
        var index = Array.IndexOf(args, name);
        if (index >= 0 && index + 1 < args.Length && Int32.TryParse(args[index + 1], out var value)) return value;

        return defaultValue;
    }

    private static String GetStringArg(String[] args, String name, String defaultValue)
    {
        var index = Array.IndexOf(args, name);
        if (index >= 0 && index + 1 < args.Length && !args[index + 1].IsNullOrEmpty()) return args[index + 1];

        return defaultValue;
    }

    private static Int32 GetEnv(String name, Int32 defaultValue)
    {
        var str = Environment.GetEnvironmentVariable(name);
        return Int32.TryParse(str, out var value) ? value : defaultValue;
    }
}

/// <summary>暴露资源上报会话的消费者，便于输出长连接状态并调整周期</summary>
sealed class StabilityConsumer : Consumer
{
    /// <summary>资源上报长连接会话</summary>
    public GrpcTelemetrySession Telemetry => _GrpcTelemetry;
}
