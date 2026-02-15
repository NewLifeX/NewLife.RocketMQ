using System;
using System.ComponentModel;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Protocol;
using Xunit;

namespace XUnitTestRocketMQ;

/// <summary>MQVersion相关测试</summary>
public class MQVersionUpdateTests
{
    [Fact]
    [DisplayName("MqBase默认版本为V4_9_7")]
    public void MqBase_DefaultVersion_V4_9_7()
    {
        using var producer = new Producer();
        Assert.Equal(MQVersion.V4_9_7, producer.Version);
    }

    [Fact]
    [DisplayName("MQVersion枚举包含5.x版本")]
    public void MQVersion_Contains_5x()
    {
        Assert.True(Enum.IsDefined(typeof(MQVersion), MQVersion.V5_0_0));
        Assert.True(Enum.IsDefined(typeof(MQVersion), MQVersion.V5_9_9));
        Assert.True(Enum.IsDefined(typeof(MQVersion), MQVersion.HIGHER_VERSION));
    }

    [Fact]
    [DisplayName("MQVersion_V4_9_7对应正确的枚举值")]
    public void MQVersion_V4_9_7_Value()
    {
        var ver = MQVersion.V4_9_7;
        Assert.Equal("V4_9_7", ver.ToString());
        // 确保是有效的枚举值
        Assert.True((Int32)ver > (Int32)MQVersion.V4_8_0);
    }

    [Fact]
    [DisplayName("MQVersion可自定义为5.x版本")]
    public void MqBase_Version_CanSet5x()
    {
        using var producer = new Producer { Version = MQVersion.V5_0_0 };
        Assert.Equal(MQVersion.V5_0_0, producer.Version);
    }
}
