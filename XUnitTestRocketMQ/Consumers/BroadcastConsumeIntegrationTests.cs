using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Threading;
using System.Threading.Tasks;
using NewLife.Log;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Models;
using NewLife.RocketMQ.Protocol;
using Xunit;
using XUnitTest.Integration;

namespace XUnitTest.Consumers;

/// <summary>广播消费集成测试</summary>
/// <remarks>
/// 广播模式：同一 Group 下所有消费者都能收到每条消息，而非集群模式的负载均衡分配。
/// 偏移量存储在本地文件，不由 Broker 管理。
/// </remarks>
public class BroadcastConsumeIntegrationTests
{
    [Fact]
    [DisplayName("广播消费_同组两个消费者_都收到全部消息")]
    public async Task BroadcastConsume_TwoConsumersInSameGroup_BothReceiveAllMessages()
    {
        var set = BasicTest.GetConfig();
        const String topic = "nx_broadcast_test";
        const String group = "nx_broadcast_group";
        const Int32 msgCount = 3;

        var stamp = DateTime.UtcNow.Ticks.ToString();

        // 两个消费者，都使用广播模式，FromLastOffset = true（只消费新消息）
        var received1 = new List<String>();
        var received2 = new List<String>();
        var consumer1Done = new SemaphoreSlim(0, 1);
        var consumer2Done = new SemaphoreSlim(0, 1);

        using var consumer1 = new Consumer
        {
            Topic = topic,
            Group = group,
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
            MessageModel = MessageModels.Broadcasting,
            FromLastOffset = true,
        };
        consumer1.OnConsume = (mq, msgs) =>
        {
            foreach (var m in msgs)
            {
                if (m.BodyString?.Contains(stamp) != true) continue;

                lock (received1)
                {
                    received1.Add(m.BodyString);
                    if (received1.Count >= msgCount && consumer1Done.CurrentCount == 0)
                        consumer1Done.Release();
                }
            }

            return true;
        };

        using var consumer2 = new Consumer
        {
            Topic = topic,
            Group = group,
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
            MessageModel = MessageModels.Broadcasting,
            FromLastOffset = true,
        };
        consumer2.OnConsume = (mq, msgs) =>
        {
            foreach (var m in msgs)
            {
                if (m.BodyString?.Contains(stamp) != true) continue;

                lock (received2)
                {
                    received2.Add(m.BodyString);
                    if (received2.Count >= msgCount && consumer2Done.CurrentCount == 0)
                        consumer2Done.Release();
                }
            }

            return true;
        };

        consumer1.Start();
        consumer2.Start();

        // 等待两个消费者完成 Rebalance
        Thread.Sleep(5000);

        // 发送消息
        using var producer = new Producer
        {
            Topic = topic,
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
        };
        producer.Start();
        for (var i = 0; i < msgCount; i++)
        {
            var sr = producer.Publish($"broadcast-{stamp}-{i}");
            Assert.Equal(SendStatus.SendOK, sr.Status);
        }

        XTrace.WriteLine("发送 {0} 条广播消息", msgCount);

        // 最多等 30 秒，两个消费者都收到全部消息
        var t1 = await consumer1Done.WaitAsync(TimeSpan.FromSeconds(30));
        var t2 = await consumer2Done.WaitAsync(TimeSpan.FromSeconds(30));

        Assert.True(t1, $"Consumer1 超时，只收到 {received1.Count}/{msgCount} 条");
        Assert.True(t2, $"Consumer2 超时，只收到 {received2.Count}/{msgCount} 条");

        // 广播模式：两个消费者应各自收到全部消息
        Assert.True(received1.Count >= msgCount, $"Consumer1 收到 {received1.Count} 条，期望 {msgCount}");
        Assert.True(received2.Count >= msgCount, $"Consumer2 收到 {received2.Count} 条，期望 {msgCount}");

        XTrace.WriteLine("广播消费验证通过：Consumer1={0}条, Consumer2={1}条", received1.Count, received2.Count);
    }
}
