using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using NewLife.Log;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Common;

namespace Test
{
    class Program
    {
        static void Main(String[] args)
        {
            XTrace.UseConsole();

            Test2();

            Console.WriteLine("OK!");
            Console.ReadKey();
        }

        static void Test1()
        {
            var mq = new Producer
            {
                //Server = "http://onsaddr-internet.aliyun.com/rocketmq/nsaddr4client-internet",
                //AccessKey = "LTAINsp1qKfO61c5",
                //SecretKey = "BvX6DpQffUz8xKIQ0u13EMxBW6YJmp",

                Topic = "nx_test",
                Group = "PID_Stone_001",
                NameServerAddress = "10.9.30.35:9876",

                Log = XTrace.Log,
            };
            // 105命令的数字签名是 NyRea4g3OHmd7RxEUoVJUz58lXc=

            mq.Start();

            //mq.CreateTopic("nx_test", 2);

            for (var i = 0; i < 16; i++)
            {
                var sr = mq.Publish("学无先后达者为师" + i, "TagA");

                Console.WriteLine("[{0}] {1} {2} {3}", sr.Queue.BrokerName, sr.Queue.QueueId, sr.MsgId, sr.QueueOffset);

                // 阿里云发送消息不能过快，否则报错“服务不可用”
                Thread.Sleep(100);
            }

            mq.Dispose();
        }

        static void Test2()
        {
            var consumer = new Consumer
            {
                //Server = "http://onsaddr-internet.aliyun.com/rocketmq/nsaddr4client-internet",
                //AccessKey = "LTAINsp1qKfO61c5",
                //SecretKey = "BvX6DpQffUz8xKIQ0u13EMxBW6YJmp",

                Topic = "nx_test",
                //Topic = "defaulttopic1",
                Group = "CID_Stone_001",
                NameServerAddress = "10.9.30.35:9876",

                Log = XTrace.Log,
            };

            consumer.OnConsume = (q, pr) =>
            {
                XTrace.WriteLine("[{0}@{1}]收到消息[{2}]({3}, {4})", q.BrokerName, q.QueueId, pr.Messages.Length, pr.MinOffset, pr.MaxOffset);

                return true;
            };

            consumer.Start();

            //Thread.Sleep(1000);
            //for (var i = 0; i < 1000; i++)
            //{
            //    //var cs = consumer.GetConsumers();
            //    //if (cs.Count > 0) XTrace.WriteLine("发现消费者：{0}", cs.Join());
            //    if (consumer.Rebalance())
            //    {
            //        var qs = consumer.Queues;
            //        var dic = qs.GroupBy(e => e.BrokerName).ToDictionary(e => e.Key, e => e.Join(",", x => x.QueueId));

            //        XTrace.WriteLine("重新平衡[{0}]：\r\n{1}", qs.Length, dic.Join("\r\n", e => $"{e.Key}[{e.Value}]"));
            //    }

            //    Thread.Sleep(1000);
            //}

            //var br = consumer.Brokers.FirstOrDefault();
            //var mq = new MessageQueue { BrokerName = br.Name, QueueId = 1 };

            ////foreach (var mq in consumer.Queues)
            //{
            //    //var offset = 0;
            //    var offset = consumer.QueryOffset(mq);
            //    var pr = consumer.Pull(mq, offset, 32, 500);

            //    Console.WriteLine("消费：{0}", pr.Messages.Length);

            //    consumer.UpdateOffset(mq, pr.MaxOffset);
            //}
        }

        static void Test3()
        {
            var list = new List<BrokerInfo>
            {
                new BrokerInfo { Name = "A", WriteQueueNums = 5 },
                new BrokerInfo { Name = "B", WriteQueueNums = 7,Addresses=new[]{ "111","222"} },
                new BrokerInfo { Name = "C", WriteQueueNums = 9 },
            };
            var list2 = new List<BrokerInfo>
            {
                new BrokerInfo { Name = "A", WriteQueueNums = 5 },
                new BrokerInfo { Name = "B", WriteQueueNums = 7 ,Addresses=new[]{ "111","222"}},
                new BrokerInfo { Name = "C", WriteQueueNums = 9 },
            };

            Console.WriteLine(list[1].Equals(list2[1]));
            Console.WriteLine(list2.SequenceEqual(list));

            var robin = new WeightRoundRobin(list.Select(e => e.WriteQueueNums).ToArray());
            var count = list.Sum(e => e.WriteQueueNums);
            for (var i = 0; i < count; i++)
            {
                var idx = robin.Get(out var times);
                var bk = list[idx];
                Console.WriteLine("{0} {1} {2}", i, bk.Name, times - 1);
            }
        }
    }
}