using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NewLife.Log;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Common;
using NewLife.Security;
using NewLife.Serialization;

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
                NameServerAddress = "127.0.0.1:9876",

                //Log = XTrace.Log,
            };
            // 105命令的数字签名是 NyRea4g3OHmd7RxEUoVJUz58lXc=

            mq.Start();

            //mq.CreateTopic("nx_test", 2);

            for (var i = 0; i < 1000_000; i++)
            {
                var str = "学无先后达者为师" + i;
                //var str = Rand.NextString(1337);

                var sr = mq.Publish(str, "TagA");

                //Console.WriteLine("[{0}] {1} {2} {3}", sr.Queue.BrokerName, sr.Queue.QueueId, sr.MsgId, sr.QueueOffset);

                // 阿里云发送消息不能过快，否则报错“服务不可用”
                //Thread.Sleep(100);
            }

            Console.WriteLine("完成");

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
                Group = "test",
                NameServerAddress = "127.0.0.1:9876",

                FromLastOffset = true,
                SkipOverStoredMsgCount = 0,
                BatchSize = 20,

                Log = XTrace.Log,
            };

            consumer.OnConsume = (q, ms) =>
            {
                Console.WriteLine("[{0}@{1}]收到消息[{2}]", q.BrokerName, q.QueueId, ms.Length);

                foreach (var item in ms.ToList())
                {
                    Console.WriteLine($"消息：主键【{item.Keys}】，产生时间【{item.BornTimestamp.ToDateTime()}】，内容【{item.Body.ToStr()}】");
                }

                return true;
            };

            consumer.Start();

            //Thread.Sleep(3000);
            //foreach (var item in consumer.Clients)
            //{
            //    var rs = item.GetRuntimeInfo();
            //    Console.WriteLine("{0}\t{1}", item.Name, rs["brokerVersionDesc"]);
            //}
        }

        static void Test3()
        {
            var dic = new SortedList<String, String>(StringComparer.Ordinal)
            {
                ["subscription"] = "aaa",
                ["subVersion"] = "ccc",
            };
            Console.WriteLine(dic.Join(",", e => $"{e.Key}={e.Value}"));

            Console.WriteLine('s' > 'V');

            Console.WriteLine();
            var cmp = Comparer<String>.Default;
            Console.WriteLine(cmp.Compare("s", "S"));
            Console.WriteLine(cmp.Compare("s", "v"));
            Console.WriteLine(cmp.Compare("s", "V"));

            Console.WriteLine();
            var cmp2 = StringComparer.OrdinalIgnoreCase;
            Console.WriteLine(cmp2.Compare("s", "S"));
            Console.WriteLine(cmp2.Compare("s", "v"));
            Console.WriteLine(cmp2.Compare("s", "V"));

            Console.WriteLine();
            cmp2 = StringComparer.Ordinal;
            Console.WriteLine(cmp2.Compare("s", "S"));
            Console.WriteLine(cmp2.Compare("s", "v"));
            Console.WriteLine(cmp2.Compare("s", "V"));

            //dic.Clear();
            //dic = dic.OrderBy(e => e.Key).ToDictionary(e => e.Key, e => e.Value);
            //Console.WriteLine(dic.Join(",", e => $"{e.Key}={e.Value}"));

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