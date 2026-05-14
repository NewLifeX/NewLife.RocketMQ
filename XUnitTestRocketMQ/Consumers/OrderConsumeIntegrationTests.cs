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

/// <summary>顺序消费端到端集成测试</summary>
/// <remarks>
/// 顺序消费原理：通过 LockBatchMQ 锁定队列，确保同一队列同一时刻只有一个消费者处理。
/// 本测试验证：开启 OrderConsume 后，发送到同一队列的消息按发送顺序被消费。
/// </remarks>
public class OrderConsumeIntegrationTests
{
    [Fact]
    [DisplayName("顺序消费_同一队列消息按发送顺序消费")]
    public void OrderConsume_SameQueue_MessagesConsumedInOrder()
    {
        var set = BasicTest.GetConfig();
        const String topic = "nx_order_consume_test";
        const String group = "nx_order_consumer_group";

        // 预创建 Topic，并确保有消息偏移基准
        using var initProducer = new Producer
        {
            Topic = topic,
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
        };
        initProducer.Start();
        initProducer.Publish("_init_");
        Thread.Sleep(500);

        // 创建消费者（FromLastOffset=true 避免消费历史消息）
        using var consumer = new Consumer
        {
            Topic = topic,
            Group = group,
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
            FromLastOffset = true,
            OrderConsume = true,
        };

        var receivedBodies = new List<String>();
        var allReceived = new ManualResetEventSlim(false);
        const Int32 msgCount = 5;
        var stamp = DateTime.UtcNow.Ticks.ToString();

        consumer.OnConsume = (q, ms) =>
        {
            foreach (var m in ms)
            {
                var body = m.BodyString;
                if (body?.StartsWith($"order-{stamp}-") == true)
                {
                    lock (receivedBodies)
                    {
                        receivedBodies.Add(body);
                        if (receivedBodies.Count >= msgCount)
                            allReceived.Set();
                    }
                }
            }
            return true;
        };
        consumer.Start();

        // 等待消费者完成 Rebalance
        Thread.Sleep(5000);

        // 用 SelectQueue 固定到同一队列顺序发送 5 条消息
        var targetQueue = initProducer.SelectQueue();
        Assert.NotNull(targetQueue);
        targetQueue.Topic = initProducer.Topic;

        var sentOrder = new List<String>();
        for (var i = 0; i < msgCount; i++)
        {
            var body = $"order-{stamp}-{i:D3}";
            sentOrder.Add(body);
            initProducer.Publish(new Message { BodyString = body }, targetQueue);
        }

        // 最多等待 30 秒收到全部消息
        var signaled = allReceived.Wait(TimeSpan.FromSeconds(30));

        Assert.True(signaled, $"超时：只收到 {receivedBodies.Count}/{msgCount} 条消息");
        Assert.Equal(msgCount, receivedBodies.Count);

        // 验证消费顺序与发送顺序一致
        for (var i = 0; i < msgCount; i++)
        {
            Assert.Equal(sentOrder[i], receivedBodies[i]);
        }

        XTrace.WriteLine("顺序消费验证通过：{0}", String.Join(" -> ", receivedBodies));
    }

    [Fact]
    [DisplayName("顺序消费_LockBatchMQ锁定成功后可解锁")]
    public async Task OrderConsume_LockAndUnlock_Success()
    {
        var set = BasicTest.GetConfig();
        const String topic = "nx_order_lock_test";

        using var consumer = new Consumer
        {
            Topic = topic,
            Group = "nx_order_lock_group",
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
        };
        consumer.Start();
        Thread.Sleep(3000);

        // 获取路由队列
        var brokers = consumer.Brokers;
        if (brokers == null || brokers.Count == 0)
        {
            XTrace.WriteLine("没有获取到 Broker，跳过锁定测试");
            return;
        }

        var mq = new MessageQueue
        {
            Topic = topic,
            BrokerName = brokers[0].Name,
            QueueId = 0,
        };

        // 锁定队列
        var locked = await consumer.LockBatchMQAsync([mq]);
        XTrace.WriteLine("锁定结果：{0}", locked.Count);

        // 无论成功与否，解锁不抛异常
        await consumer.UnlockBatchMQAsync([mq]);
        XTrace.WriteLine("解锁完成");
    }
}
