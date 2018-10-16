using System;
using System.Linq;
using NewLife.Log;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Protocol;

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
                Server = "http://onsaddr-internet.aliyun.com/rocketmq/nsaddr4client-internet",
                AccessKey = "LTAINsp1qKfO61c5",
                SecretKey = "BvX6DpQffUz8xKIQ0u13EMxBW6YJmp",

                Topic = "nx_test",
                Group = "PID_Stone_001",
                NameServerAddress = "10.9.30.35:9876",

                Log = XTrace.Log,
            };

            mq.Start();

            for (var i = 0; i < 10; i++)
            {
                var str = "学无先后达者为师" + i;
                var msg = new Message
                {
                    //Topic = "nx_test",
                    Body = str.GetBytes(),
                    Tags = "TagA",
                    Keys = "OrderID001",
                };

                var sr = mq.Send(msg);
                Console.WriteLine("{0} {1} {2}", sr.MsgId, sr.Queue.QueueId, sr.QueueOffset);
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
    }
}