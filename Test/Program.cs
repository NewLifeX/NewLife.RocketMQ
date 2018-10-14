using System;
using NewLife.Log;
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
                Server = "http://onsaddr-internet.aliyun.com/rocketmq/nsaddr4client-internet",
                AccessKey = "LTAINsp1qKfO61c5",
                SecretKey = "BvX6DpQffUz8xKIQ0u13EMxBW6YJmp",

                ProducerGroup = "PID_Stone_001",
                //NameServerAddress = "127.0.0.1:9876",
                //NameServerAddress = "sh02.newlifex.com:9876",
                InstanceName = "Producer",
            };

            mq.Start();

            for (var i = 0; i < 10; i++)
            {
                var str = "学无先后达者为师" + i;
                var msg = new Message
                {
                    Topic = "nx_test",
                    Body = str.GetBytes(),
                    Tags = "TagA",
                    Keys = "OrderID001",
                };

                mq.Send(msg);
            }

            mq.Dispose();
        }
    }
}