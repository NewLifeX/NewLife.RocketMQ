using System;
using NewLife.Log;
using NewLife.RocketMQ.Consumer;
using NewLife.RocketMQ.Producer;
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
            var mq = new MQProducer
            {
                //Server = "http://onsaddr-internet.aliyun.com/rocketmq/nsaddr4client-internet",
                //AccessKey = "LTAINsp1qKfO61c5",
                //SecretKey = "BvX6DpQffUz8xKIQ0u13EMxBW6YJmp",

                //CreateTopicKey = "nx_test",
                Topic = "defaulttopic1",
                Group = "PID_Stone_001",
                NameServerAddress = "10.9.30.35:9876",
                InstanceName = "Producer",
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
                Console.WriteLine("{0} {1}", sr.MsgId, sr.QueueOffset);
            }

            mq.Dispose();
        }

        static void Test2()
        {
            var mq = new MQPullConsumer
            {
                //Server = "http://onsaddr-internet.aliyun.com/rocketmq/nsaddr4client-internet",
                //AccessKey = "LTAINsp1qKfO61c5",
                //SecretKey = "BvX6DpQffUz8xKIQ0u13EMxBW6YJmp",

                Topic = "defaulttopic1",
                //ProducerGroup = "PID_Stone_001",
                NameServerAddress = "10.9.30.35:9876",
                InstanceName = "Consumer",
            };

            mq.Start();
        }
    }
}