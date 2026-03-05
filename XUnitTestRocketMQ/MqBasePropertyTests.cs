using System;
using System.ComponentModel;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Client;
using NewLife.RocketMQ.Protocol;
using Xunit;

namespace XUnitTestRocketMQ;

/// <summary>MqBase基类属性测试</summary>
public class MqBasePropertyTests
{
    #region 默认值
    [Fact]
    [DisplayName("Producer_默认Group为DEFAULT_PRODUCER")]
    public void Producer_DefaultGroup()
    {
        using var producer = new Producer();

        Assert.Equal("DEFAULT_PRODUCER", producer.Group);
    }

    [Fact]
    [DisplayName("Producer_默认Topic为TBW102")]
    public void Producer_DefaultTopic()
    {
        using var producer = new Producer();

        Assert.Equal("TBW102", producer.Topic);
    }

    [Fact]
    [DisplayName("Producer_默认DefaultTopicQueueNums为4")]
    public void Producer_DefaultTopicQueueNums()
    {
        using var producer = new Producer();

        Assert.Equal(4, producer.DefaultTopicQueueNums);
    }

    [Fact]
    [DisplayName("Producer_默认InstanceName不为空")]
    public void Producer_DefaultInstanceName()
    {
        using var producer = new Producer();

        // InstanceName 默认为进程 ID
        Assert.NotNull(producer.InstanceName);
        Assert.NotEmpty(producer.InstanceName);
    }

    [Fact]
    [DisplayName("Producer_默认PollNameServerInterval为30000")]
    public void Producer_DefaultPollInterval()
    {
        using var producer = new Producer();

        Assert.Equal(30_000, producer.PollNameServerInterval);
    }

    [Fact]
    [DisplayName("Producer_默认HeartbeatBrokerInterval为30000")]
    public void Producer_DefaultHeartbeatInterval()
    {
        using var producer = new Producer();

        Assert.Equal(30_000, producer.HeartbeatBrokerInterval);
    }

    [Fact]
    [DisplayName("Producer_默认SerializeType为JSON")]
    public void Producer_DefaultSerializeType()
    {
        using var producer = new Producer();

        Assert.Equal(SerializeType.JSON, producer.SerializeType);
    }

    [Fact]
    [DisplayName("Producer_默认Version为V4_9_7")]
    public void Producer_DefaultVersion()
    {
        using var producer = new Producer();

        Assert.Equal(MQVersion.V4_9_7, producer.Version);
    }

    [Fact]
    [DisplayName("Producer_默认不启用VIP通道")]
    public void Producer_DefaultVipChannelDisabled()
    {
        using var producer = new Producer();

        Assert.False(producer.VipChannelEnabled);
    }

    [Fact]
    [DisplayName("Producer_默认不启用消息轨迹")]
    public void Producer_DefaultTraceDisabled()
    {
        using var producer = new Producer();

        Assert.False(producer.EnableMessageTrace);
    }

    [Fact]
    [DisplayName("Producer_默认不使用外部代理")]
    public void Producer_DefaultExternalBrokerDisabled()
    {
        using var producer = new Producer();

        Assert.False(producer.ExternalBroker);
    }
    #endregion

    #region ClientId
    [Fact]
    [DisplayName("ClientId_包含IP和InstanceName")]
    public void ClientId_ContainsIPAndInstance()
    {
        using var producer = new Producer();

        var clientId = producer.ClientId;

        Assert.NotNull(clientId);
        Assert.Contains("@", clientId);
    }

    [Fact]
    [DisplayName("ClientId_含UnitName时追加")]
    public void ClientId_WithUnitName()
    {
        using var producer = new Producer { UnitName = "unit1" };

        var clientId = producer.ClientId;

        Assert.Contains("@unit1", clientId);
    }
    #endregion

    #region 属性设置
    [Fact]
    [DisplayName("Producer_可设置Name")]
    public void Producer_SetName()
    {
        using var producer = new Producer { Name = "TestProducer" };

        Assert.Equal("TestProducer", producer.Name);
    }

    [Fact]
    [DisplayName("Producer_可设置NameServerAddress")]
    public void Producer_SetNameServerAddress()
    {
        using var producer = new Producer { NameServerAddress = "127.0.0.1:9876" };

        Assert.Equal("127.0.0.1:9876", producer.NameServerAddress);
    }

    [Fact]
    [DisplayName("Producer_DefaultTopic静态属性为TBW102")]
    public void DefaultTopic_IsTBW102()
    {
        Assert.Equal("TBW102", MqBase.DefaultTopic);
    }

    [Fact]
    [DisplayName("Consumer_默认Group为DEFAULT_PRODUCER")]
    public void Consumer_DefaultGroup()
    {
        using var consumer = new Consumer();

        Assert.Equal("DEFAULT_PRODUCER", consumer.Group);
    }

    [Fact]
    [DisplayName("Consumer_可设置属性")]
    public void Consumer_SetProperties()
    {
        using var consumer = new Consumer
        {
            Topic = "order_topic",
            Group = "CG_ORDER",
            NameServerAddress = "10.0.0.1:9876",
        };

        Assert.Equal("order_topic", consumer.Topic);
        Assert.Equal("CG_ORDER", consumer.Group);
        Assert.Equal("10.0.0.1:9876", consumer.NameServerAddress);
    }
    #endregion

    #region Active状态
    [Fact]
    [DisplayName("Producer_未启动时Active为false")]
    public void Producer_NotStarted_ActiveFalse()
    {
        using var producer = new Producer();

        Assert.False(producer.Active);
    }

    [Fact]
    [DisplayName("Consumer_未启动时Active为false")]
    public void Consumer_NotStarted_ActiveFalse()
    {
        using var consumer = new Consumer();

        Assert.False(consumer.Active);
    }
    #endregion
}
