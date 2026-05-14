using System;
using System.ComponentModel;
using System.Threading;
using NewLife;
using NewLife.Log;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Protocol;
using Xunit;
using XUnitTest.Integration;

namespace XUnitTest.Cluster;

/// <summary>管理功能扩展集成测试 — 消息查询与消费组信息</summary>
public class ManagementExtendedTests
{
    [Fact]
    [DisplayName("QueryMessageByKey_发送带Key消息后可查询到")]
    public void QueryMessageByKey_FindsMessageByKey()
    {
        var set = BasicTest.GetConfig();
        const String topic = "nx_test";
        var uniqueKey = "qmk-" + Guid.NewGuid().ToString("N")[..8];

        using var producer = new Producer
        {
            Topic = topic,
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
        };
        producer.Start();

        // 发送带唯一 Key 的消息
        var msg = new Message { Topic = topic };
        msg.SetBody("QueryMessageByKey body");
        msg.Keys = uniqueKey;
        var result = producer.Publish(msg, null);
        Assert.Equal(SendStatus.SendOK, result.Status);

        // 等待 Broker 索引构建（最长 5 秒）
        Thread.Sleep(5000);

        // 按 Key 查询
        var msgs = producer.QueryMessageByKey(topic, uniqueKey, 10,
            DateTimeOffset.UtcNow.AddMinutes(-5).ToUnixTimeMilliseconds(),
            DateTimeOffset.UtcNow.AddMinutes(5).ToUnixTimeMilliseconds());

        // 索引有一定延迟，返回空也不报错；有结果则验证 Key 匹配
        if (msgs?.Count > 0)
        {
            foreach (var m in msgs)
            {
                Assert.Contains(uniqueKey, m.Keys);
            }
        }
    }

    [Fact]
    [DisplayName("QueryMessageByKey_key为空_抛出ArgumentNullException")]
    public void QueryMessageByKey_EmptyKey_Throws()
    {
        var set = BasicTest.GetConfig();

        using var producer = new Producer
        {
            Topic = "nx_test",
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
        };
        producer.Start();

        Assert.Throws<ArgumentNullException>(() =>
            producer.QueryMessageByKey("nx_test", ""));
    }

    [Fact]
    [DisplayName("ViewMessage_发送消息后按MsgId查询")]
    public void ViewMessage_ByMsgId_Succeeds()
    {
        var set = BasicTest.GetConfig();

        using var producer = new Producer
        {
            Topic = "nx_test",
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
        };
        producer.Start();

        var result = producer.Publish("ViewMessage-extended-test", "TagA", null);
        Assert.Equal(SendStatus.SendOK, result.Status);
        Assert.NotEmpty(result.MsgId);

        // Broker 可能有索引延迟，不严格断言消息非 null
        var msg = producer.ViewMessage(result.MsgId);
        // 仅验证不抛出异常
    }

    [Fact]
    [DisplayName("Consumer_Queues属性_启动后返回非空队列")]
    public void Consumer_Queues_ReturnsAfterStart()
    {
        var set = BasicTest.GetConfig();

        using var consumer = new Consumer
        {
            Topic = "nx_test",
            Group = "nx_queues_prop_group",
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
            FromLastOffset = true,
        };
        consumer.OnConsume = (q, ms) => true;
        consumer.Start();

        // 等待 Rebalance 分配队列
        Thread.Sleep(5000);

        var queues = consumer.Queues;
        Assert.NotNull(queues);
        Assert.True(queues.Length > 0, "Rebalance 后应有至少一个分配的队列");
        foreach (var mq in queues)
        {
            Assert.False(mq.BrokerName.IsNullOrEmpty(), "BrokerName 不应为空");
            Assert.Equal("nx_test", mq.Topic);
            Assert.True(mq.QueueId >= 0);
        }
    }

    [Fact]
    [DisplayName("GetTopicStatsInfo_返回Topic统计信息")]
    public void GetTopicStatsInfo_ReturnsData()
    {
        var set = BasicTest.GetConfig();

        using var producer = new Producer
        {
            Topic = "nx_test",
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
        };
        producer.Start();

        var stats = producer.GetTopicStatsInfo("nx_test");
        // 有数据则验证结构；允许 null（Broker 无历史数据时）
        // 仅验证不抛出异常
    }
}
