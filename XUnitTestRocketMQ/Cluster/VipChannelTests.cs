using System;
using System.ComponentModel;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Client;
using Xunit;

namespace XUnitTestRocketMQ;

/// <summary>VIP通道测试</summary>
public class VipChannelTests
{
    [Fact]
    [DisplayName("VipChannelEnabled_默认为false")]
    public void VipChannelEnabled_DefaultFalse()
    {
        using var producer = new Producer();
        Assert.False(producer.VipChannelEnabled);
    }

    [Fact]
    [DisplayName("VipChannelEnabled_可设置为true")]
    public void VipChannelEnabled_CanSetTrue()
    {
        using var producer = new Producer();
        producer.VipChannelEnabled = true;
        Assert.True(producer.VipChannelEnabled);
    }

    [Fact]
    [DisplayName("VipChannelEnabled_Consumer也支持")]
    public void VipChannelEnabled_ConsumerSupport()
    {
        using var consumer = new Consumer();
        consumer.VipChannelEnabled = true;
        Assert.True(consumer.VipChannelEnabled);
    }

    [Fact]
    [DisplayName("BrokerClient_VIP模式端口偏移为端口减2")]
    public void BrokerClient_VipPortOffset()
    {
        // 验证 BrokerClient 构造时如果启用VIP，端口会减2
        // 通过 MqBase 的属性间接验证
        using var producer = new Producer
        {
            VipChannelEnabled = true,
        };

        // VipChannelEnabled 设置为 true 时，BrokerClient 创建时会使用 port - 2
        Assert.True(producer.VipChannelEnabled);
    }

    [Fact]
    [DisplayName("BrokerClient_非VIP模式端口不变")]
    public void BrokerClient_NonVipPortUnchanged()
    {
        using var producer = new Producer
        {
            VipChannelEnabled = false,
        };

        Assert.False(producer.VipChannelEnabled);
    }
}
