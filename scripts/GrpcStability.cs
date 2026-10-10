// ============================================================
// RocketMQ gRPC（5.x Proxy）长稳验收脚本（需要 .NET 10 SDK）
//
//   运行：dotnet run --file scripts/GrpcStability.cs -- --minutes 5
//   参数：--minutes N    持续分钟数，默认 5
//         --interval N   每轮间隔秒数，默认 15
//         --tolerate N   允许失败次数，默认 0（超过则退出码 1，便于 CI 判定）
//         --topic NAME   主题，默认 grpc-stability-topic
//   环境变量：ROCKETMQ_NAMESERVER（默认 127.0.0.1:9876）
//             ROCKETMQ_GRPC_PROXY（默认 http://127.0.0.1:8081）
//
// 每轮：Remoting 投递标记消息 → gRPC 逐队列接收 → gRPC 确认，统计收发与异常。
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
    /// <returns>失败次数超过容忍值时返回 1</returns>
    public static async Task<Int32> RunAsync(String[] args)
    {
        var minutes = GetArg(args, "--minutes", GetEnv("ROCKETMQ_STABILITY_MINUTES", 5));
        var interval = GetArg(args, "--interval", 15);
        var tolerate = GetArg(args, "--tolerate", 0);
        var minReceived = GetArg(args, "--min-received", 1);
        var topic = GetStringArg(args, "--topic", "grpc-stability-topic");

        var nameserver = Environment.GetEnvironmentVariable("ROCKETMQ_NAMESERVER");
        if (nameserver.IsNullOrEmpty()) nameserver = "127.0.0.1:9876";

        var proxy = Environment.GetEnvironmentVariable("ROCKETMQ_GRPC_PROXY");
        if (proxy.IsNullOrEmpty()) proxy = "http://127.0.0.1:8081";

        Console.WriteLine($"[长稳] 计划 {minutes} 分钟，每 {interval} 秒一轮；NameServer={nameserver} Proxy={proxy}");
        Console.WriteLine($"[长稳] 主题={topic}，允许失败 {tolerate} 次，至少接收 {minReceived} 条");

        // 预热：建主题并投递标记消息（gRPC 不提供建主题接口）
        using var publisher = new Producer { Topic = topic, NameServerAddress = nameserver };
        publisher.Start();
        publisher.CreateTopic(topic, 4);

        using var consumer = new StabilityConsumer
        {
            Topic = topic,
            Group = "grpc-stability-" + Guid.NewGuid().ToString("N")[..8],
            NameServerAddress = nameserver,
            GrpcProxyAddress = proxy,
        };
        consumer.Start();

        if (consumer.Telemetry != null)
        {
            consumer.Telemetry.RefreshInterval = TimeSpan.FromSeconds(10);
            consumer.Telemetry.ReconnectInterval = TimeSpan.FromSeconds(2);
        }

        var route = await QueryRouteAsync(consumer, topic);
        if (route == null)
        {
            Console.WriteLine("[长稳] 路由始终未就绪，退出");
            return 1;
        }

        Console.WriteLine($"[长稳] 通道就绪：队列 {route.MessageQueues.Count}，Telemetry 重建周期 {consumer.Telemetry?.RefreshInterval.TotalSeconds}s");

        var sw = Stopwatch.StartNew();
        var rounds = 0;
        var received = 0;
        var acked = 0;
        var failures = new List<String>();

        while (sw.Elapsed < TimeSpan.FromMinutes(minutes))
        {
            rounds++;
            var mark = $"stability-{sw.ElapsedMilliseconds}";
            try
            {
                var sent = await publisher.PublishAsync(mark);
                if (sent.Status != SendStatus.SendOK) throw new Exception($"发送失败：{sent.Status}");
            }
            catch (Exception ex)
            {
                failures.Add($"[{sw.Elapsed:mm\\:ss}] 发送异常 {ex.Message}");
            }

            var got = 0;
            foreach (var queue in route.MessageQueues)
            {
                try
                {
                    var messages = await consumer.ReceiveMessageViaGrpcAsync(queue, batchSize: 8, invisibleDuration: TimeSpan.FromSeconds(30), longPollingTimeout: TimeSpan.FromSeconds(2));
                    if (messages.Count == 0) continue;

                    var ack = await consumer.AckMessageViaGrpcAsync(topic,
                    [
                        .. messages.Select(m => new AckMessageEntry
                        {
                            ReceiptHandle = m.SystemProperties?.ReceiptHandle,
                            MessageId = m.SystemProperties?.MessageId,
                        }),
                    ]);

                    if (ack.Status?.Code == GrpcCode.OK) acked += messages.Count;
                    else failures.Add($"[{sw.Elapsed:mm\\:ss}] 确认失败：{ack.Status?.Code} {ack.Status?.Message}");

                    got += messages.Count;
                }
                catch (Exception ex)
                {
                    failures.Add($"[{sw.Elapsed:mm\\:ss}] 接收异常 {ex.GetType().Name}: {ex.Message}");
                    break;
                }
            }

            received += got;
            Console.WriteLine($"[{sw.Elapsed:mm\\:ss}] 第 {rounds} 轮：接收 {got} 条（累计 {received}，确认 {acked}），异常 {failures.Count}，Telemetry.Active={consumer.Telemetry?.Active ?? false}");

            await Task.Delay(TimeSpan.FromSeconds(interval));
        }

        Console.WriteLine();
        Console.WriteLine($"[长稳] 结束：{rounds} 轮，接收 {received} 条，确认 {acked} 条，异常 {failures.Count} 次");
        foreach (var item in failures.Take(10)) Console.WriteLine("  - " + item);
        if (failures.Count > 10) Console.WriteLine($"  …… 其余 {failures.Count - 10} 条略");

        consumer.Stop();
        consumer.Dispose();

        if (received < minReceived)
        {
            Console.WriteLine($"[长稳] 接收数量不足：{received} < {minReceived}");
            return 1;
        }

        return failures.Count > tolerate ? 1 : 0;
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
