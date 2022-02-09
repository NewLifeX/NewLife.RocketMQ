using NewLife;
using NewLife.Log;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Client;
using NewLife.RocketMQ.Protocol;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace XUnitTestRocketMQ
{
    public class AliyunTests
    {
        private static void SetConfig(MqBase mq)
        {
            mq.Server = "http://onsaddr-internet.aliyun.com/rocketmq/nsaddr4client-internet";
            mq.Configure(MqSetting.Current);

            mq.Log = XTrace.Log;
        }

        [Fact]
        public void CreateTopic()
        {
            var mq = new Producer
            {
                //Topic = "nx_test",
            };
            SetConfig(mq);

            mq.Start();

            // 创建topic时，start前不能指定topic，让其使用默认TBW102
            Assert.Equal("TBW102", mq.Topic);

            mq.CreateTopic("nx_test", 2);
        }

        [Fact]
        static void ProduceTest()
        {
            using var mq = new Producer
            {
                Topic = "test1",
            };
            SetConfig(mq);

            mq.Start();

            for (var i = 0; i < 10; i++)
            {
                var str = "学无先后达者为师" + i;
                //var str = Rand.NextString(1337);

                var sr = mq.Publish(str, "TagA", null);
            }
        }

        [Fact]
        static async Task ProduceAsyncTest()
        {
            using var mq = new Producer
            {
                Topic = "test1",
            };
            SetConfig(mq);

            mq.Start();

            for (var i = 0; i < 10; i++)
            {
                var str = "学无先后达者为师" + i;
                //var str = Rand.NextString(1337);

                var sr = await mq.PublishAsync(str, "TagA", null);
            }
        }

        private static Consumer _consumer;
        [Fact]
        static void ConsumeTest()
        {
            var consumer = new Consumer
            {
                Topic = "test1",
                Group = "test",

                FromLastOffset = true,
                SkipOverStoredMsgCount = 0,
                BatchSize = 20,
            };
            SetConfig(consumer);

            consumer.OnConsume = OnConsume;
            consumer.Start();

            _consumer = consumer;

            Thread.Sleep(3000);
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
}