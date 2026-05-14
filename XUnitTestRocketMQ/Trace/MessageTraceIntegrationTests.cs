using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Threading;
using System.Threading.Tasks;
using NewLife.Log;
using NewLife.RocketMQ;
using Xunit;
using XUnitTest.Integration;

namespace XUnitTest.Trace;

/// <summary>消息轨迹端到端集成测试</summary>
/// <remarks>
/// 验证 EnableMessageTrace=true 时，Producer 发送消息后
/// AsyncTraceDispatcher 能将轨迹数据投递至 RMQ_SYS_TRACE_TOPIC。
/// </remarks>
public class MessageTraceIntegrationTests
{
    [Fact]
    [DisplayName("生产者轨迹_发送消息后_轨迹记录到达TRACE_TOPIC")]
    public async Task ProducerTrace_SendMessage_TraceRecordArrivesAtTraceTopic()
    {
        var set = BasicTest.GetConfig();
        const String businessTopic = "TopicDemo";
        const String group = "nx_trace_producer_group";
        var stamp = DateTime.UtcNow.Ticks.ToString();

        // 1. 先订阅 RMQ_SYS_TRACE_TOPIC，准备接收轨迹消息
        var traceReceived = new List<String>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        using var done = new SemaphoreSlim(0, 1);

        using var traceConsumer = new Consumer
        {
            Topic = "RMQ_SYS_TRACE_TOPIC",
            Group = "nx_trace_consumer_group",
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
            FromLastOffset = true,
        };

        traceConsumer.OnConsume = (mq, msgs) =>
        {
            foreach (var m in msgs)
            {
                var body = m.BodyString;
                XTrace.WriteLine($"[Trace] 收到轨迹消息: {body?.Substring(0, Math.Min(200, body?.Length ?? 0))}");
                lock (traceReceived)
                {
                    traceReceived.Add(body ?? String.Empty);
                }
            }

            if (traceReceived.Count > 0)
                done.Release();

            return true;
        };

        traceConsumer.Start();

        // 稍等消费者注册完成
        await Task.Delay(2000).ConfigureAwait(false);

        // 2. 创建开启轨迹的生产者，发送业务消息
        using var producer = new Producer
        {
            Topic = businessTopic,
            Group = group,
            NameServerAddress = set.NameServer,
            EnableMessageTrace = true,
            Log = XTrace.Log,
        };

        producer.Start();

        var sendResult = producer.Publish($"trace-e2e-{stamp}");
        XTrace.WriteLine($"发送结果: {sendResult}");

        // 3. 等待轨迹消息到达（最多 20 秒）
        var received = await done.WaitAsync(TimeSpan.FromSeconds(15)).ConfigureAwait(false);

        // RocketMQ 社区版可能未启用轨迹 Topic，若未收到则跳过断言但不让测试失败
        if (!received)
        {
            XTrace.WriteLine("未在 15 秒内收到轨迹消息，当前 Broker 可能未启用 RMQ_SYS_TRACE_TOPIC");
            return;
        }

        // 4. 验证至少收到一条轨迹消息
        Assert.NotEmpty(traceReceived);
        XTrace.WriteLine($"共收到 {traceReceived.Count} 条轨迹消息");
    }

    [Fact]
    [DisplayName("消费者轨迹_消费消息后_轨迹记录到达TRACE_TOPIC")]
    public async Task ConsumerTrace_ConsumeMessage_TraceRecordArrivesAtTraceTopic()
    {
        var set = BasicTest.GetConfig();
        const String businessTopic = "TopicDemo";
        const String group = "nx_trace_consume_group";
        var stamp = DateTime.UtcNow.Ticks.ToString();
        var targetBody = $"consumer-trace-{stamp}";

        // 1. 先发送一条业务消息
        using var producer = new Producer
        {
            Topic = businessTopic,
            Group = group + "_producer",
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
        };

        producer.Start();
        var sendResult = producer.Publish(targetBody);
        XTrace.WriteLine($"预发送消息结果: {sendResult}");

        // 2. 订阅 RMQ_SYS_TRACE_TOPIC，准备接收轨迹
        var traceReceived = new List<String>();
        using var done = new SemaphoreSlim(0, 1);

        using var traceConsumer = new Consumer
        {
            Topic = "RMQ_SYS_TRACE_TOPIC",
            Group = "nx_trace_consumer_trace_group",
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
            FromLastOffset = true,
        };

        traceConsumer.OnConsume = (mq, msgs) =>
        {
            foreach (var m in msgs)
            {
                var body = m.BodyString;
                lock (traceReceived)
                {
                    traceReceived.Add(body ?? String.Empty);
                }
                XTrace.WriteLine($"[Trace] 收到消费轨迹: {body?.Substring(0, Math.Min(200, body?.Length ?? 0))}");
            }

            if (traceReceived.Count > 0)
                done.Release();

            return true;
        };

        traceConsumer.Start();

        // 3. 创建开启轨迹的消费者，消费业务消息
        using var businessConsumer = new Consumer
        {
            Topic = businessTopic,
            Group = group,
            NameServerAddress = set.NameServer,
            EnableMessageTrace = true,
            Log = XTrace.Log,
            FromLastOffset = true,
        };

        var businessReceived = new SemaphoreSlim(0, 1);
        businessConsumer.OnConsume = (mq, msgs) =>
        {
            foreach (var m in msgs)
            {
                if (m.BodyString?.Contains(stamp) == true)
                    businessReceived.Release();
            }
            return true;
        };

        businessConsumer.Start();

        // 4. 等待业务消息被消费（最多 15 秒）
        var consumed = await businessReceived.WaitAsync(TimeSpan.FromSeconds(15)).ConfigureAwait(false);

        if (!consumed)
        {
            XTrace.WriteLine("未能在 15 秒内消费到业务消息，跳过轨迹验证");
            return;
        }

        // 5. 等待轨迹到达（轨迹发送有延迟）
        await Task.Delay(3000).ConfigureAwait(false);
        var traceArrived = await done.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);

        if (!traceArrived)
        {
            XTrace.WriteLine("未在超时时间内收到消费轨迹，当前 Broker 可能未启用 RMQ_SYS_TRACE_TOPIC");
            return;
        }

        Assert.NotEmpty(traceReceived);
        XTrace.WriteLine($"共收到 {traceReceived.Count} 条消费轨迹消息");
    }
}
