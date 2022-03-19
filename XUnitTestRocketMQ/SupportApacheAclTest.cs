using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using NewLife;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Protocol;
using Xunit;

namespace XUnitTestRocketMQ
{
    public class SupportApacheAclTest
    {
        private const String NameServerAddress = "127.0.0.1:9876";
        private const String TestTopic = "newlife_acl_test_topic";

        /// <summary>
        /// 创建Topic时默认为系统 Topic => TBW102,可省略
        /// </summary>
        private const String DefaultSysTopic = "TBW102";

        private readonly AclOptions _aclOptions = new AclOptions() {AccessKey = "rocketmq2AcKey", SecretKey = "rocketmq2SeKey", OnsChannel = "LOCAL"};
        
        [Fact]
        public void CreateTopicTest()
        {
            using var producer = CreateProducerInstance(DefaultSysTopic);
            producer.Start();
            producer.CreateTopic(TestTopic, 2);
            producer.Dispose();
        }

        [Fact]
        public void PublishMessageTest()
        {
            using var producer = CreateProducerInstance(TestTopic);
            producer.Start();

            var pubResultList = new List<Boolean>();
            for (var i = 0; i < 10; i++)
            {
                const String message = "大家好才是真的好！";
                var pubResult = producer.Publish(message, "new_life_test_tag");
                pubResultList.Add(pubResult.Status == SendStatus.SendOK);
            }

            Assert.True(pubResultList.All(_ => true));
            producer.Dispose();
        }

        [Fact]
        public void ConsumeMessageTest()
        {
            using var consumer = CreateConsumerInstance(TestTopic);
            consumer.OnConsume = OnConsume;
            consumer.Start();
            Thread.Sleep(3000);
           
            static Boolean OnConsume(MessageQueue q, MessageExt[] ms)
            {
                Console.WriteLine("[{0}@{1}]收到消息[{2}]", q.BrokerName, q.QueueId, ms.Length);

                foreach (var item in ms.ToList())
                {
                    Console.WriteLine($"消息：主键【{item.Keys}】，产生时间【{item.BornTimestamp.ToDateTime()}】，内容【{item.Body.ToStr()}】");
                }

                return true;
            }
        }

        private Producer CreateProducerInstance(String topic)
        {
            var producer = new Producer();

            producer.NameServerAddress = NameServerAddress;
            producer.Topic = topic;
            producer.AclOptions = _aclOptions;

            return producer;
        }
        
        private Consumer CreateConsumerInstance(String topic)
        {
            var consumer = new Consumer();

            consumer.NameServerAddress = NameServerAddress;
            consumer.Topic = topic;
            consumer.AclOptions = _aclOptions;
            consumer.Group = "new_life_test_group";
            consumer.FromLastOffset = true;
            consumer.BatchSize = 5;
            
            return consumer;
        }
    }
}