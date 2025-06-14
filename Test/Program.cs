using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using NewLife;
using NewLife.Log;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Common;
using NewLife.RocketMQ.Protocol;

namespace Test;

class Program
{
    static void Main(String[] args)
    {
        XTrace.UseConsole();

        Test1();
        //TestAliyun();

        Console.WriteLine("OK!");
        Console.ReadKey();
    }

    static void Test1()
    {
        XTrace.WriteLine("");
        XTrace.WriteLine("创建生产者……");
        var producer = new Producer
        {
            Topic = "nx_test",
            NameServerAddress = "rocketmq.newlifex.com:9876",
            Version = MQVersion.V5_2_0,

            Log = XTrace.Log,
        };

        producer.Configure(MqSetting.Current);
        producer.Start();

        //mq.CreateTopic("nx_test", 2);

        XTrace.WriteLine("");
        XTrace.WriteLine("创建消费者……");
        var consumer = new Consumer
        {
            Topic = producer.Topic,
            Group = "test",
            NameServerAddress = producer.NameServerAddress,

            FromLastOffset = false,
            //SkipOverStoredMsgCount = 0,
            //BatchSize = 20,

            Log = XTrace.Log,
            ClientLog = XTrace.Log,
        };

        consumer.OnConsume = OnConsume;

        consumer.Configure(MqSetting.Current);
        consumer.Start();
        Thread.Sleep(1000);

        XTrace.WriteLine("");
        XTrace.WriteLine("发布测试消息……");
        for (var i = 0; i < 10; i++)
        {
            var str = "学无先后达者为师" + i;
            //var str = Rand.NextString(1337);

            var sr = producer.Publish(str, "TagA", null);

            //Console.WriteLine("[{0}] {1} {2} {3}", sr.Queue.BrokerName, sr.Queue.QueueId, sr.MsgId, sr.QueueOffset);

            // 阿里云发送消息不能过快，否则报错“服务不可用”
            Thread.Sleep(500);
        }

        Console.WriteLine("完成");

        producer.Dispose();
    }

    private static Consumer _consumer;
    static void Test2()
    {
        var consumer = new Consumer
        {
            Topic = "nx_test",
            Group = "test",
            NameServerAddress = "127.0.0.1:9876",

            FromLastOffset = false,
            //SkipOverStoredMsgCount = 0,
            //BatchSize = 20,

            Log = XTrace.Log,
            ClientLog = XTrace.Log,
        };

        consumer.OnConsume = OnConsume;

        consumer.Configure(MqSetting.Current);
        consumer.Start();

        _consumer = consumer;
    }

    static void TestAliyun()
    {
        // 2024-04-10 对接阿里云RocketMQ v4测试通过。
        // 创建RocketMQ实例后，需要手工创建Topic和Group，并创建正确的AccessKey
        var consumer = new Consumer
        {
            Topic = "newlife_test_02",
            Group = "GID_newlife_Group02",
            NameServerAddress = "http://MQ_INST_1827694722767531_BXxCwUhm.mq-internet-access.mq-internet.aliyuncs.com:80",

            Aliyun = new AliyunOptions
            {
                AccessKey = "LTAI5tKTGShu31C61xRARVC4",
                SecretKey = "a9oPwph1IcMGanWckzUOwOf3Ork8LO",
                //InstanceId = "MQ_INST_1827694722767531_BXxCwUhm",
            },

            FromLastOffset = true,
            //SkipOverStoredMsgCount = 0,
            BatchSize = 1,

            Log = XTrace.Log,
            ClientLog = XTrace.Log,
        };

        consumer.OnConsume = OnConsume;

        consumer.Configure(MqSetting.Current);
        consumer.Start();

        _consumer = consumer;
    }

    private static Boolean OnConsume(MessageQueue q, MessageExt[] ms)
    {
        Console.WriteLine("[{0}@{1}]收到消息[{2}]", q.BrokerName, q.QueueId, ms.Length);

        foreach (var item in ms.ToList())
        {
            Console.WriteLine($"消息：主键【{item.Keys}】 Topic 【{item.Topic}】，产生时间【{item.BornTimestamp.ToDateTime().ToFullString()}】，内容【{item.Body.ToStr(null, 0, 64)}】");
        }

        return true;
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

        var robin = new WeightRoundRobin();
        robin.Set(list.Select(e => e.WriteQueueNums).ToArray());
        var count = list.Sum(e => e.WriteQueueNums);
        for (var i = 0; i < count; i++)
        {
            var idx = robin.Get(out var times);
            var bk = list[idx];
            Console.WriteLine("{0} {1} {2}", i, bk.Name, times - 1);
        }
    }

    static void Test4()
    {
        var a1 = File.ReadAllBytes("a1".GetFullPath());
        var a2 = File.ReadAllBytes("a2".GetFullPath());

        //var ms = new MemoryStream(a1);
        //ms.Position += 2;
        //var ds = new DeflateStream(ms, CompressionMode.Decompress);
        //var buf = ds.ReadBytes();

        var buf = a1.ReadBytes(2, -1).Decompress();

        var rs = a2.ToBase64() == buf.ToBase64();
        Console.WriteLine(rs);

        Console.WriteLine(buf.ToStr());
    }

    static void Test5()
    {
        var _consumers = new List<Consumer>();
        var topics = new List<string>() { "flow", "flow2" };

        for (int i = 0; i < 2; i++)
        {
            var consumer = new Consumer
            {
                Topic = topics[i],
                //Group = "test",
                NameServerAddress = "172.19.177.185:9876",

                BatchSize = 1,
                Log = XTrace.Log,
            };

            consumer.OnConsume = OnConsume;
            consumer.Start();

            _consumers.Add(consumer);

        }
    }
}