using System;
using System.ComponentModel;
using System.Threading;
using NewLife.Log;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Protocol;
using Xunit;
using XUnitTest.Integration;

namespace XUnitTest.Producers;

/// <summary>单向消息（PublishOneway）集成测试</summary>
public class OnewayTests
{
    [Fact]
    [DisplayName("发送单向消息_返回结果不为null")]
    public void PublishOneway_ReturnsNonNull()
    {
        var set = BasicTest.GetConfig();

        using var producer = new Producer
        {
            Topic = "nx_oneway_test",
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
        };
        producer.Start();

        var message = new Message { Topic = "nx_oneway_test" };
        message.SetBody("oneway-message-001");

        var mq = producer.SelectQueue();
        mq.Topic = "nx_oneway_test";

        var result = producer.PublishOneway(message, mq);

        // 单向消息 Broker 不回应，SendResult 从响应头构建
        Assert.NotNull(result);
    }

    [Fact]
    [DisplayName("发送多条单向消息_全部不抛异常")]
    public void PublishOneway_Multiple_NoException()
    {
        var set = BasicTest.GetConfig();

        using var producer = new Producer
        {
            Topic = "nx_oneway_test",
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
        };
        producer.Start();

        var mq = producer.SelectQueue();
        mq.Topic = "nx_oneway_test";

        for (var i = 0; i < 5; i++)
        {
            var message = new Message { Topic = "nx_oneway_test" };
            message.SetBody($"oneway-{i}");
            producer.PublishOneway(message, mq);
        }
        // 不抛异常即通过
    }

    [Fact]
    [DisplayName("发送单向消息_消费者可收到消息")]
    public void PublishOneway_ConsumerCanReceive()
    {
        var set = BasicTest.GetConfig();
        const String topic = "nx_oneway_consumer_test";

        // 预创建 Topic
        using var producer = new Producer
        {
            Topic = topic,
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
        };
        producer.Start();
        producer.Publish("_init_");
        Thread.Sleep(500);

        // 创建消费者
        using var consumer = new Consumer
        {
            Topic = topic,
            Group = "nx_oneway_consumer_group",
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
            FromLastOffset = true,
        };

        var received = new ManualResetEventSlim(false);
        consumer.OnConsume = (q, ms) =>
        {
            foreach (var m in ms)
            {
                if (m.BodyString?.Contains("oneway-consumer-test") == true)
                    received.Set();
            }
            return true;
        };
        consumer.Start();
        Thread.Sleep(3000);

        var mq = producer.SelectQueue();
        mq.Topic = topic;
        var message = new Message { Topic = topic };
        message.SetBody("oneway-consumer-test");
        producer.PublishOneway(message, mq);

        var signaled = received.Wait(TimeSpan.FromSeconds(15));
        Assert.True(signaled, "在超时时间内未通过单向消息收到消费");
    }
}
