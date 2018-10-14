using System;
using NewLife.Log;
using NewLife.RocketMQ.Client;
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
                AccessKey = "",
                SecretKey = "",

                ProducerGroup = "测试组",
                NameServerAddress = "192.168.1.15:9876",
            };

            mq.Start();

            var msg = new Message
            {
                Topic = "主题",
                Body = "学无先后达者为师".GetBytes(),
            };

            mq.Send(msg);
            mq.Dispose();
        }
    }
}