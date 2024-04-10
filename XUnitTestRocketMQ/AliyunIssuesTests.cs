using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using NewLife;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Protocol;
using Xunit;

namespace XUnitTestRocketMQ;

/// <summary>
/// 修复Issues调用阿里云版RocketMQ相关问题
/// #35、#24
/// </summary>
public class AliyunIssuesTests
{
    private readonly String _testTopic = "newlife_test_01";
    private readonly String _testGroup = "GID_newlife_Group01";
    private static readonly AliyunOptions _aliyunOptions = new AliyunOptions()
    {
        AccessKey = "LTAIxxxxxxxxxxxxRARVC4",
        SecretKey = "a9oPwxxxxxxxxxxx3OrxxLO",
        Server = "http://onsaddr-internet.aliyun.com/rocketmq/nsaddr4client-internet",
        InstanceId = "MQ_INST_xxxxxxxxxxxx_AXxCwUhm"
    };

    [Fact]
    public void ProducerForAliyun_Test()
    {
        var producer = new Producer()
        {
            Topic = _testTopic,
            Aliyun = _aliyunOptions,
            //NameServerAddress = "http://MQ_INST_xxxxxxxxxx_AXxCwUhm.mq-internet-access.mq-internet.aliyuncs.com:80",
            //如果不用上面的默认Server地址，直接将NameServerAddress设为你自己的TCP公网接收点地址也是可以的
        };

        producer.Start();

        var pubResultList = new List<Boolean>();
        for (var i = 0; i < 2; i++)
        {
            var message = "大家好才是真的好！";
            var pubResult = producer.Publish(message, "newlife_test_tag");
            pubResultList.Add(pubResult.Status == SendStatus.SendOK);
        }
        Assert.True(pubResultList.All(t => true));

        producer.Dispose();
    }

    [Fact]
    public void ConsumerForAliyun_Test()
    {
        var consumer = new Consumer()
        {
            Topic = _testTopic,
            Aliyun = _aliyunOptions,
            Group = _testGroup,
            FromLastOffset = true,
            BatchSize = 1,
        };

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
}
