using System;
using System.ComponentModel;
using System.Threading;
using NewLife.Log;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Protocol;
using Xunit;
using XUnitTest.Integration;

namespace XUnitTest.Cluster;

/// <summary>F057 Lite Topic 集成测试 — 需要本地 RocketMQ 5.5.0+ Broker</summary>
/// <remarks>
/// Lite Topic（RIP-83）是 RocketMQ 5.5.0+ 引入的轻量动态 Topic。
/// 如果本地 Broker 版本低于 5.5.0，CreateTopic 会成功（Broker 忽略 topicMessageType 字段）
/// 但不具备 Lite Topic 特性，测试仍会通过（CreateTopic 返回成功次数 &gt; 0）。
/// </remarks>
public class LiteTopicIntegrationTests
{
    [Fact]
    [DisplayName("CreateTopic_Normal类型_创建成功")]
    public void CreateTopic_NormalType_Succeeds()
    {
        var set = BasicTest.GetConfig();
        using var producer = new Producer
        {
            Topic = "nx_test",
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
        };
        producer.Start();

        var topicName = "nx_normal_" + Guid.NewGuid().ToString("N")[..8];
        var count = producer.CreateTopic(topicName, 4, topicMessageType: TopicMessageType.Normal);
        Assert.True(count > 0, $"Normal Topic 应创建成功，但返回 {count}");

        // 清理
        producer.DeleteTopic(topicName);
    }

    [Fact]
    [DisplayName("CreateTopic_Fifo类型_创建成功")]
    public void CreateTopic_FifoType_Succeeds()
    {
        var set = BasicTest.GetConfig();
        using var producer = new Producer
        {
            Topic = "nx_test",
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
        };
        producer.Start();

        var topicName = "nx_fifo_" + Guid.NewGuid().ToString("N")[..8];
        var count = producer.CreateTopic(topicName, 4, topicMessageType: TopicMessageType.Fifo);
        Assert.True(count > 0, $"Fifo Topic 应创建成功，但返回 {count}");

        // 清理
        producer.DeleteTopic(topicName);
    }

    [Fact]
    [DisplayName("CreateTopic_Lite类型_在5.5.0+Broker上创建成功")]
    public void CreateTopic_LiteType_Succeeds_On550OrHigher()
    {
        var set = BasicTest.GetConfig();
        using var producer = new Producer
        {
            Topic = "nx_test",
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
        };
        producer.Start();

        // Lite Topic：仅在 RocketMQ 5.5.0+ Broker 上真正生效
        // 旧版 Broker 忽略 topicMessageType 字段，以 Normal 类型创建，CreateTopic 仍返回成功
        var topicName = "nx_lite_" + Guid.NewGuid().ToString("N")[..8];
        var count = producer.CreateTopic(topicName, 1, topicMessageType: TopicMessageType.Lite);
        Assert.True(count > 0, $"Lite Topic 创建应返回成功次数 > 0，但返回 {count}（若 Broker < 5.5.0，topicMessageType 字段被忽略，Topic 以 Normal 类型创建）");

        // 创建成功后可以向该 Topic 发送消息
        var msg = new Message { Topic = topicName };
        msg.SetBody("Lite Topic Test");
        var result = producer.Publish(msg, null);
        Assert.Equal(SendStatus.SendOK, result.Status);

        // 清理
        Thread.Sleep(500);
        producer.DeleteTopic(topicName);
    }
}
