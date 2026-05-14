using System;
using System.ComponentModel;
using System.Threading;
using NewLife.Log;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Models;
using NewLife.RocketMQ.Protocol;
using Xunit;
using XUnitTest.Integration;

namespace XUnitTest.Producers;

/// <summary>延迟消息集成测试</summary>
public class DelayMessageTests
{
    [Fact]
    [DisplayName("发布延迟消息_级别S1_发送成功并能被消费")]
    public void PublishDelay_LevelS1_CanBeConsumed()
    {
        var set = BasicTest.GetConfig();
        const String topic = "nx_delay_test";

        // 预创建 Topic
        using var initProducer = new Producer
        {
            Topic = topic,
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
        };
        initProducer.Start();
        initProducer.Publish("_init_");
        Thread.Sleep(500);

        // 创建消费者
        using var consumer = new Consumer
        {
            Topic = topic,
            Group = "nx_delay_group",
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
            FromLastOffset = true,
        };

        var received = new ManualResetEventSlim(false);
        MessageExt receivedMsg = null;

        consumer.OnConsume = (q, ms) =>
        {
            foreach (var m in ms)
            {
                if (m.BodyString?.Contains("delay-test-S1") == true)
                {
                    receivedMsg = m;
                    received.Set();
                }
            }
            return true;
        };
        consumer.Start();
        Thread.Sleep(3000);

        // 发布延迟消息，等级 S1（约1秒后投递）
        initProducer.PublishDelay("delay-test-S1", DelayTimeLevels.S1);

        // 最多等 15 秒
        var signaled = received.Wait(TimeSpan.FromSeconds(15));

        Assert.True(signaled, "在超时时间内未收到延迟消息");
        Assert.NotNull(receivedMsg);
        Assert.Contains("delay-test-S1", receivedMsg.BodyString);
    }

    [Fact]
    [DisplayName("发布延迟消息_设置DelayTimeLevel属性正确")]
    public void PublishDelay_SetsDelayTimeLevelOnMessage()
    {
        var set = BasicTest.GetConfig();

        using var producer = new Producer
        {
            Topic = "nx_delay_test",
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
        };
        producer.Start();

        var message = new Message { Topic = "nx_delay_test" };
        message.SetBody("delay-level-check");
        // 调用前 DelayTimeLevel 应为 0
        Assert.Equal(0, message.DelayTimeLevel);

        var result = producer.PublishDelay(message, null, DelayTimeLevels.Min1);

        // 调用后 message 的 DelayTimeLevel 已被设置为对应枚举值
        Assert.Equal((Int32)DelayTimeLevels.Min1, message.DelayTimeLevel);
        Assert.Equal(SendStatus.SendOK, result.Status);
    }

    [Fact]
    [DisplayName("发布延迟消息_多个等级均发送成功")]
    public void PublishDelay_MultipleLevels_AllSendOK()
    {
        var set = BasicTest.GetConfig();

        using var producer = new Producer
        {
            Topic = "nx_delay_test",
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
        };
        producer.Start();

        var levels = new[]
        {
            DelayTimeLevels.S1,
            DelayTimeLevels.S5,
            DelayTimeLevels.S10,
            DelayTimeLevels.Min1,
            DelayTimeLevels.Min2,
        };

        foreach (var level in levels)
        {
            var msg = new Message { Topic = "nx_delay_test" };
            msg.SetBody($"level-{level}");
            var result = producer.PublishDelay(msg, null, level);
            Assert.Equal(SendStatus.SendOK, result.Status);
        }
    }
}
