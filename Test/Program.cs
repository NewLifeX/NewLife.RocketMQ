using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using NewLife.Log;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Common;
using NewLife.RocketMQ.Protocol;

namespace Test
{
    class Program
    {
        static void Main(String[] args)
        {
            XTrace.UseConsole();

            Test1();

            Console.WriteLine("OK!");
            Console.ReadKey();
        }

        static void Test1()
        {
            var mq = new Producer
            {
                Server = "http://onsaddr-internet.aliyun.com/rocketmq/nsaddr4client-internet",
                AccessKey = "LTAINsp1qKfO61c5",
                SecretKey = "BvX6DpQffUz8xKIQ0u13EMxBW6YJmp",

                Topic = "nx_test",
                Group = "PID_Stone_001",
                NameServerAddress = "10.9.30.35:9876",

                Log = XTrace.Log,
            };
            // 105命令的数字签名是 NyRea4g3OHmd7RxEUoVJUz58lXc=

            mq.Start();

            for (var i = 0; i < 16; i++)
            {
                var sr = mq.Send("学无先后达者为师" + i, "TagA");

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
                Server = "http://onsaddr-internet.aliyun.com/rocketmq/nsaddr4client-internet",
                AccessKey = "LTAINsp1qKfO61c5",
                SecretKey = "BvX6DpQffUz8xKIQ0u13EMxBW6YJmp",

                Topic = "nx_test",
                //Topic = "defaulttopic1",
                Group = "CID_Stone_001",
                NameServerAddress = "10.9.30.35:9876",

                Log = XTrace.Log,
            };

            consumer.Start();
            var br = consumer.Brokers.FirstOrDefault();
            var mq = new MessageQueue { BrokerName = br.Name, QueueId = 1 };

            //foreach (var mq in consumer.Queues)
            {
                var offset = 0;
                //var offset = consumer.QueryOffset(mq);
                var pr = consumer.Pull(mq, offset, 32);

                Console.WriteLine(pr);
            }
        }

        static void Test3()
        {
            var list = new List<BrokerInfo>
            {
                new BrokerInfo { Name = "A", WriteQueueNums = 5 },
                new BrokerInfo { Name = "B", WriteQueueNums = 7 },
                new BrokerInfo { Name = "C", WriteQueueNums = 9 }
            };

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