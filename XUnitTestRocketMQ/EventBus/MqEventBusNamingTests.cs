using System;
using System.ComponentModel;
using NewLife.RocketMQ;
using Xunit;

namespace XUnitTest.EventBus;

/// <summary>事件总线命名转换测试</summary>
public class MqEventBusNamingTests
{
    [Theory]
    [DisplayName("主题转换_非法字符替换为下划线")]
    [InlineData("order.created", "order_created")]
    [InlineData("app/order:paid", "app_order_paid")]
    [InlineData("order_created", "order_created")]
    [InlineData("a-b%c", "a-b%c")]
    public void ToTopic_InvalidChars_Replaced(String input, String expected) => Assert.Equal(expected, MqEventBusNaming.ToTopic(input));

    [Fact]
    [DisplayName("组名转换_本机标识合法化")]
    public void ToGroup_MachineId_Sanitized()
    {
        var group = MqEventBusNaming.ToGroup("192.168.1.5@1234");

        Assert.Equal("192_168_1_5_1234", group);
    }

    [Fact]
    [DisplayName("名称转换_超长名称按上限截断")]
    public void Sanitize_TooLong_Truncated()
    {
        var name = new String('a', 200);

        Assert.Equal(MqEventBusNaming.MaxNameLength, MqEventBusNaming.Sanitize(name).Length);
    }

    [Fact]
    [DisplayName("名称转换_空值原样返回")]
    public void Sanitize_Empty_ReturnsInput()
    {
        Assert.Null(MqEventBusNaming.Sanitize(null));
        Assert.Equal("", MqEventBusNaming.Sanitize(""));
    }
}
