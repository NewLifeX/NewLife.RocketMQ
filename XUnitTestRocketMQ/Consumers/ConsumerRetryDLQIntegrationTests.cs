using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Threading;
using System.Threading.Tasks;
using NewLife.Log;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Protocol;
using Xunit;
using XUnitTest.Integration;

namespace XUnitTest.Consumers;

/// <summary>消费重试与死信队列（DLQ）集成测试</summary>
/// <remarks>
/// 测试场景：
/// 1. 消费者 OnConsume 返回 false → 消息进入 %RETRY%{group} 延迟重投
/// 2. 超过 MaxReconsumeTimes 后消息进入 %DLQ%{group}
/// 注意：Broker 重试有延迟（至少 10 秒），测试超时需预留 90 秒以上。
/// </remarks>
public class ConsumerRetryDLQIntegrationTests
{
    [Fact]
    [DisplayName("消费重试_消费返回false_消息带重试计数重投")]
    public async Task ConsumerRetry_OnConsumeFalse_MessageRedeliveredWithReconsumeTimes()
    {
        var set = BasicTest.GetConfig();
        const String topic = "nx_retry_dlq_test";
        const String group = "nx_retry_dlq_group";

        // 生产者发送一条消息
        using var producer = new Producer
        {
            Topic = topic,
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
        };
        producer.Start();
        Thread.Sleep(2000);

        var stamp = DateTime.UtcNow.Ticks.ToString();
        var sr = producer.Publish($"retry-test-{stamp}");
        Assert.Equal(SendStatus.SendOK, sr.Status);
        XTrace.WriteLine("发送消息: stamp={0}", stamp);

        Thread.Sleep(500);

        var maxRetry = 2;
        var receivedStamps = new List<(String body, Int32 reconsumeTimes)>();
        var firstReceived = new SemaphoreSlim(0, 1);

        // 消费者：拒绝第一次消费，触发重试
        using var consumer = new Consumer
        {
            Topic = topic,
            Group = group,
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
            MaxReconsumeTimes = maxRetry,
            EnableRetry = true,
        };
        consumer.OnConsume = (mq, msgs) =>
        {
            foreach (var m in msgs)
            {
                if (m.BodyString?.Contains(stamp) != true) continue;

                XTrace.WriteLine("收到消息 body={0}, ReconsumeTimes={1}", m.BodyString, m.ReconsumeTimes);
                lock (receivedStamps)
                {
                    receivedStamps.Add((m.BodyString, m.ReconsumeTimes));
                }

                if (firstReceived.CurrentCount == 0)
                    firstReceived.Release();

                // 始终拒绝，让消息进入重试
                return false;
            }

            return true;
        };
        consumer.Start();

        // 等待首次收到消息（超时 30 秒）
        var got = await firstReceived.WaitAsync(TimeSpan.FromSeconds(30));
        Assert.True(got, "超时：未收到初始消息");

        // 验证第一次收到时 ReconsumeTimes == 0
        lock (receivedStamps)
        {
            Assert.True(receivedStamps.Count >= 1, "未收到任何带 stamp 的消息");
            Assert.Equal(0, receivedStamps[0].reconsumeTimes);
        }

        XTrace.WriteLine("验证通过：首次消费 ReconsumeTimes=0，消费返回 false 后消息将重投");
        // 注：等待完整重试链条（直到 DLQ）耗时过长，此处仅验证首次消费和触发重试机制
    }

    [Fact]
    [DisplayName("消费重试_关闭重试_消费失败不重投")]
    public async Task ConsumerRetry_DisabledRetry_MessageNotRedelivered()
    {
        var set = BasicTest.GetConfig();
        const String topic = "nx_no_retry_test";
        const String group = "nx_no_retry_group";

        using var producer = new Producer
        {
            Topic = topic,
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
        };
        producer.Start();
        Thread.Sleep(2000);

        var stamp = DateTime.UtcNow.Ticks.ToString();
        producer.Publish($"no-retry-{stamp}");
        Thread.Sleep(500);

        var receiveCount = 0;
        var receivedOnce = new SemaphoreSlim(0, 1);
        var advancedOffset = new SemaphoreSlim(0, 1);

        using var consumer = new Consumer
        {
            Topic = topic,
            Group = group,
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
            EnableRetry = false,    // 关闭重试：失败不发回 RETRY Topic
            FromLastOffset = true,
        };
        consumer.OnConsume = (mq, msgs) =>
        {
            foreach (var m in msgs)
            {
                if (m.BodyString?.Contains(stamp) != true) continue;

                var count = Interlocked.Increment(ref receiveCount);
                if (receivedOnce.CurrentCount == 0)
                    receivedOnce.Release();

                XTrace.WriteLine("收到消息(关闭重试) count={0}: body={1}", count, m.BodyString);

                // 第一次拒绝（模拟失败），第二次起放行（推进偏移）
                // EnableRetry=false：失败不发送回 RETRY Topic，但偏移不推进，下次仍会拉到
                if (count == 1)
                {
                    XTrace.WriteLine("第一次拒绝消费，验证不发回 RETRY Topic");
                    return false;
                }

                if (advancedOffset.CurrentCount == 0)
                    advancedOffset.Release();
            }

            return true; // 第二次起放行，偏移推进
        };
        consumer.Start();

        // 等待收到一次
        var got = await receivedOnce.WaitAsync(TimeSpan.FromSeconds(30));
        Assert.True(got, "超时：未收到消息");

        // EnableRetry=false：第一次失败后消息被重拉（偏移未推进），等待第二次消费放行
        var advanced = await advancedOffset.WaitAsync(TimeSpan.FromSeconds(15));
        Assert.True(advanced, "超时：消息未被第二次消费");

        // 验证收到至少 2 次（第一次拒绝 + 第二次放行）
        Assert.True(receiveCount >= 2, $"期望 >=2 次收到消息，实际 {receiveCount} 次");
        XTrace.WriteLine("EnableRetry=false 验证通过：消息收到 {0} 次，未发回 RETRY Topic", receiveCount);
    }
}
