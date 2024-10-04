using NewLife;
using NewLife.Log;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Protocol;
using System;
using System.Linq;
using System.Threading;
using Xunit;

namespace XUnitTestRocketMQ;

public class ConsumerTests
{
    private static Consumer _consumer;
    [Fact]
    public static void ConsumeTest()
    {
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

        Thread.Sleep(3000);
        //foreach (var item in consumer.Clients)
        //{
        //    var rs = item.GetRuntimeInfo();
        //    Console.WriteLine("{0}\t{1}", item.Name, rs["brokerVersionDesc"]);
        //}
    }

    private static Boolean OnConsume(MessageQueue q, MessageExt[] ms)
    {
        Console.WriteLine("[{0}@{1}]收到消息[{2}]", q.BrokerName, q.QueueId, ms.Length);

        foreach (var item in ms.ToList())
        {
            Console.WriteLine($"消息：主键【{item.Keys}】，产生时间【{item.BornTimestamp.ToDateTime()}】，内容【{item.Body.ToStr()}】");
        }

        return true;
    }
}