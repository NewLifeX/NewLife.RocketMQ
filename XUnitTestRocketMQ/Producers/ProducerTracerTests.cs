using System;
using System.Threading;
using NewLife.Log;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Protocol;
using Xunit;
using XUnitTest.Integration;

namespace XUnitTest.Producers;

public class ProducerTracerTests
{
    private const String Topic = "TopicDemo";
    private const String Group = "TraceTestGroup";

    [Fact]
    public void Producer_And_Consumer_With_Trace_Enabled_Should_Work()
    {
        XTrace.UseConsole();

        var set = BasicTest.GetConfig();
        var mre = new ManualResetEvent(false);

        // 1. 先启动消费者，确保 Rebalance 完成后再发送消息
        using var consumer = new Consumer
        {
            Topic             = Topic,
            Group             = Group,
            NameServerAddress = set.NameServer,
            FromLastOffset    = true,
            Log               = XTrace.Log,
        };

        consumer.OnConsume = (q, ms) =>
        {
            foreach (var item in ms)
            {
                XTrace.WriteLine("消费到消息: {0}", item.BodyString);
                mre.Set();
            }
            return true;
        };

        consumer.Start();

        // 等待消费者完成 Rebalance
        Thread.Sleep(3000);

        // 2. 创建并启动生产者
        using var producer = new Producer
        {
            Topic             = Topic,
            Group             = Group,
            NameServerAddress = set.NameServer,
            EnableMessageTrace = true,
            Log               = XTrace.Log,
        };

        producer.Start();

        // 3. 发送消息
        var messageBody = "Hello, RocketMQ with Message Trace!";
        var sendResult = producer.Publish(messageBody);

        Assert.NotNull(sendResult);
        Assert.Equal(SendStatus.SendOK, sendResult.Status);
        XTrace.WriteLine("消息发送成功: MsgId={0}", sendResult.MsgId);

        // 4. 等待消费者收到消息，30 秒超时
        var consumed = mre.WaitOne(TimeSpan.FromSeconds(30));

        // 5. 断言
        Assert.True(consumed, "消费者在超时时间内没有收到消息。");
    }
}
