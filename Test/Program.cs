using NewLife.Log;
using NewLife.RocketMQ.Producer;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
                SecretKey = ""
            };
        }
    }
}