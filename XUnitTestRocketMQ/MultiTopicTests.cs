using System;
using System.ComponentModel;
using System.Linq;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Protocol;
using Xunit;

namespace XUnitTestRocketMQ;

/// <summary>多Topic订阅测试</summary>
public class MultiTopicTests
{
    [Fact]
    [DisplayName("Consumer_Topics属性默认为null")]
    public void Consumer_Topics_DefaultNull()
    {
        using var consumer = new Consumer();
        Assert.Null(consumer.Topics);
    }

    [Fact]
    [DisplayName("Consumer_可设置多个Topics")]
    public void Consumer_Topics_CanBeSet()
    {
        using var consumer = new Consumer
        {
            Topics = ["topic_a", "topic_b", "topic_c"]
        };

        Assert.NotNull(consumer.Topics);
        Assert.Equal(3, consumer.Topics.Length);
        Assert.Equal("topic_a", consumer.Topics[0]);
    }

    [Fact]
    [DisplayName("Consumer_Topics为空时保持单Topic行为")]
    public void Consumer_Topics_EmptyFallsBackToSingleTopic()
    {
        using var consumer = new Consumer
        {
            Topic = "default_topic",
            Topics = null
        };

        // Topics为null时，内部应使用单Topic
        Assert.Equal("default_topic", consumer.Topic);
    }

    [Fact]
    [DisplayName("MessageQueue_已包含Topic属性")]
    public void MessageQueue_HasTopicProperty()
    {
        var mq1 = new MessageQueue { Topic = "topic_a", BrokerName = "broker-a", QueueId = 0 };
        var mq2 = new MessageQueue { Topic = "topic_b", BrokerName = "broker-a", QueueId = 0 };

        // 不同Topic的队列不相等
        Assert.NotEqual(mq1, mq2);
        Assert.NotEqual(mq1.GetHashCode(), mq2.GetHashCode());
    }

    [Fact]
    [DisplayName("MessageQueue_相同Topic相等")]
    public void MessageQueue_SameTopic_Equal()
    {
        var mq1 = new MessageQueue { Topic = "topic_a", BrokerName = "broker-a", QueueId = 0 };
        var mq2 = new MessageQueue { Topic = "topic_a", BrokerName = "broker-a", QueueId = 0 };

        Assert.Equal(mq1, mq2);
    }

    [Fact(Skip = "需要RocketMQ服务器")]
    [DisplayName("Consumer_多Topic启动消费")]
    public void Consumer_MultiTopic_Start()
    {
        using var consumer = new Consumer
        {
            Topic = "topic_main",
            Topics = ["topic_a", "topic_b"],
            Group = "test_multi_group",
            NameServerAddress = "127.0.0.1:9876",
        };

        // 验证Topics设置正确
        Assert.Equal(2, consumer.Topics.Length);
    }
}
