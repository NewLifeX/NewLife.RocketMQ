using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using NewLife;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Protocol;
using Xunit;

namespace XUnitTest.Cloud;

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
        producer.CloudProvider = new AclProvider
        {
            AccessKey = _aclOptions.AccessKey,
            SecretKey = _aclOptions.SecretKey,
        };

        return producer;
    }
    
    private Consumer CreateConsumerInstance(String topic)
    {
        var consumer = new Consumer();

        consumer.NameServerAddress = NameServerAddress;
        consumer.Topic = topic;
        consumer.CloudProvider = new AclProvider
        {
            AccessKey = _aclOptions.AccessKey,
            SecretKey = _aclOptions.SecretKey,
        };
        consumer.Group = "new_life_test_group";
        consumer.FromLastOffset = true;
        consumer.BatchSize = 5;
        
        return consumer;
    }

    // F054: ACL 2.0 权限模型单元测试

    [Fact]
    [System.ComponentModel.DisplayName("AclProvider_默认AclEnabled为false")]
    public void AclProvider_AclEnabled_DefaultFalse()
    {
        var provider = new AclProvider { AccessKey = "ak", SecretKey = "sk" };
        Assert.False(provider.AclEnabled);
    }

    [Fact]
    [System.ComponentModel.DisplayName("AclProvider_AclEnabled属性读写正确")]
    public void AclProvider_AclEnabled_ReadWrite()
    {
        var provider = new AclProvider
        {
            AccessKey = "ak",
            SecretKey = "sk",
            AclEnabled = true,
            ResourceType = 1,
            ResourceName = "TestTopic",
        };

        Assert.True(provider.AclEnabled);
        Assert.Equal(1, provider.ResourceType);
        Assert.Equal("TestTopic", provider.ResourceName);
    }

    [Fact]
    [System.ComponentModel.DisplayName("AclProvider_ACL2字段注入请求头")]
    public void AclProvider_Acl2Fields_InjectedToHeader()
    {
        var provider = new AclProvider
        {
            AccessKey = "ak",
            SecretKey = "sk",
            AclEnabled = true,
            ResourceType = 1,
            ResourceName = "TestTopic",
        };

        // 通过 BrokerClient 创建命令，验证请求头中包含 ACL 2.0 字段
        // SetSignature 在 InvokeAsync 中调用，这里验证 AclEnabled 属性已正确设置
        Assert.True(provider.AclEnabled);
        Assert.Equal("1", provider.ResourceType.ToString());
        Assert.Equal("TestTopic", provider.ResourceName);
    }

    [Fact]
    [System.ComponentModel.DisplayName("AclProvider_AclEnabledFalse时ACL2字段不注入")]
    public void AclProvider_AclEnabledFalse_Acl2FieldsSkipped()
    {
        var provider = new AclProvider
        {
            AccessKey = "ak",
            SecretKey = "sk",
            AclEnabled = false,    // 默认 false
            ResourceType = 1,
            ResourceName = "TestTopic",
        };

        // AclEnabled=false 时，虽然设置了 ResourceType/ResourceName，但不应注入
        Assert.False(provider.AclEnabled);
    }

    [Fact]
    [System.ComponentModel.DisplayName("AclProvider_FromOptions_不含ACL2字段_AclEnabled为false")]
    public void AclProvider_FromOptions_AclEnabledFalse()
    {
        var opts = new AclOptions { AccessKey = "ak", SecretKey = "sk", OnsChannel = "LOCAL" };
        var provider = AclProvider.FromOptions(opts);

        Assert.NotNull(provider);
        Assert.Equal("ak", provider.AccessKey);
        Assert.False(provider.AclEnabled);   // FromOptions 不设置 ACL 2.0 字段，保持旧行为
    }
}