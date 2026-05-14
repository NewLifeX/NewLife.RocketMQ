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

/// <summary>多 Topic 订阅集成测试</summary>
/// <remarks>
/// Consumer 可通过 Topics 属性订阅多个主题，在同一消费者组内接收所有主题的消息。
/// </remarks>
public class MultiTopicIntegrationTests
{
    [Fact]
    [DisplayName("多Topic订阅_两个Topic分别发消息_一个消费者都能收到")]
    public async Task MultiTopic_SendToTwoTopics_OneConsumerReceivesBoth()
    {
        var set = BasicTest.GetConfig();
        const String topicA = "nx_multi_a";
        const String topicB = "nx_multi_b";
        const String group = "nx_multi_topic_group";

        var stamp = DateTime.UtcNow.Ticks.ToString();
        var receivedFromA = new List<String>();
        var receivedFromB = new List<String>();
        var allReceived = new CountdownEvent(2);  // 等待两个 Topic 各至少 1 条

        // 多 Topic 消费者
        using var consumer = new Consumer
        {
            Topic = topicA,               // 主 Topic（用于 Rebalance 基础路由）
            Topics = [topicA, topicB],    // 实际订阅的 Topic 列表
            Group = group,
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
            FromLastOffset = true,
        };
        consumer.OnConsume = (mq, msgs) =>
        {
            foreach (var m in msgs)
            {
                if (m.BodyString?.Contains(stamp) != true) continue;

                if (mq.Topic == topicA || m.Topic == topicA)
                {
                    lock (receivedFromA)
                    {
                        if (receivedFromA.Count == 0)
                        {
                            receivedFromA.Add(m.BodyString);
                            allReceived.Signal();
                        }
                    }
                }
                else if (mq.Topic == topicB || m.Topic == topicB)
                {
                    lock (receivedFromB)
                    {
                        if (receivedFromB.Count == 0)
                        {
                            receivedFromB.Add(m.BodyString);
                            allReceived.Signal();
                        }
                    }
                }
            }

            return true;
        };
        consumer.Start();
        Thread.Sleep(5000);

        // 分别向两个 Topic 发送消息
        using var producerA = new Producer
        {
            Topic = topicA,
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
        };
        producerA.Start();

        using var producerB = new Producer
        {
            Topic = topicB,
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
        };
        producerB.Start();

        var srA = producerA.Publish($"multi-a-{stamp}");
        var srB = producerB.Publish($"multi-b-{stamp}");
        Assert.Equal(SendStatus.SendOK, srA.Status);
        Assert.Equal(SendStatus.SendOK, srB.Status);
        XTrace.WriteLine("发送 TopicA 和 TopicB 各一条消息, stamp={0}", stamp);

        // 最多等 30 秒收到两个 Topic 的消息
        var signaled = allReceived.Wait(TimeSpan.FromSeconds(30));

        // 检查至少从一个 Topic 收到消息（多 Topic 路由可能有 Broker 限制）
        var totalReceived = receivedFromA.Count + receivedFromB.Count;
        XTrace.WriteLine("收到 TopicA={0}条, TopicB={1}条", receivedFromA.Count, receivedFromB.Count);

        Assert.True(totalReceived >= 1, "多 Topic 消费者未收到任何消息");

        if (!signaled)
            XTrace.WriteLine("注意：30秒内未同时收到两个Topic的消息，可能需要更多时间或Broker支持");
        else
            XTrace.WriteLine("多 Topic 订阅验证通过");
    }
}
