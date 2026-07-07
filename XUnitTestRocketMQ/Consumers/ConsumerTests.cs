using System;
using System.Linq;
using System.Threading;
using NewLife;
using NewLife.Log;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Protocol;
using Xunit;
using XUnitTest.Integration;

namespace XUnitTest.Consumers;

public class ConsumerTests
{
    private Consumer _consumer;

    [Fact]
    [System.ComponentModel.DisplayName("Consumer集成测试_启动消费不抛异常_并获取路由信息")]
    public void ConsumeTest()
    {
        BasicTest.EnsureAvailable();

        var set = BasicTest.GetConfig();
        var consumer = new Consumer
        {
            Topic = "nx_test",
            Group = "test",
            NameServerAddress = set.NameServer,

            FromLastOffset = true,
            BatchSize = 20,

            Log = XTrace.Log,
        };

        consumer.OnConsume = OnConsume;
        consumer.Start();

        _consumer = consumer;

        // 等待 Rebalance 后验证路由信息
        Thread.Sleep(3000);

        // 验证消费者已获取到 Broker 路由信息
        Assert.NotNull(consumer.Brokers);
        Assert.NotEmpty(consumer.Brokers);
        XTrace.WriteLine("Consumer 获取到 {0} 个 Broker", consumer.Brokers.Count);
        //foreach (var item in consumer.Clients)
        //{
        //    var rs = item.GetRuntimeInfo();
        //    Console.WriteLine("{0}\t{1}", item.Name, rs["brokerVersionDesc"]);
        //}
    }

    private Boolean OnConsume(MessageQueue q, MessageExt[] ms)
    {
        Console.WriteLine("[{0}@{1}]收到消息[{2}]", q.BrokerName, q.QueueId, ms.Length);

        foreach (var item in ms.ToList())
        {
            Console.WriteLine($"消息：主键【{item.Keys}】，产生时间【{item.BornTimestamp.ToDateTime()}】，内容【{item.Body.ToStr()}】");
        }

        return true;
    }
}