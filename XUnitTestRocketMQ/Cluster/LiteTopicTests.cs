using System;
using System.ComponentModel;
using NewLife.RocketMQ.Protocol;
using Xunit;

namespace XUnitTest.Cluster;

/// <summary>F057 Lite Topic 单元测试</summary>
/// <remarks>
/// Lite Topic（RIP-83）是 RocketMQ 5.5.0+ 引入的轻量动态 Topic，
/// 支持按需动态创建数百万级轻量 Topic，适合 AI Agent 等海量临时 Topic 场景。
/// 本测试类覆盖不依赖 Broker 的枚举值验证，实际创建见 LiteTopicIntegrationTests。
/// </remarks>
public class LiteTopicTests
{
    [Fact]
    [DisplayName("TopicMessageType_枚举值定义正确")]
    public void TopicMessageType_EnumValues_Correct()
    {
        Assert.Equal(0, (Int32)TopicMessageType.Normal);
        Assert.Equal(1, (Int32)TopicMessageType.Fifo);
        Assert.Equal(2, (Int32)TopicMessageType.Delay);
        Assert.Equal(3, (Int32)TopicMessageType.Transaction);
        Assert.Equal(4, (Int32)TopicMessageType.Lite);
    }

    [Fact]
    [DisplayName("TopicMessageType_ToString_返回Pascal命名")]
    public void TopicMessageType_ToString_ReturnsPascalCase()
    {
        Assert.Equal("Normal", TopicMessageType.Normal.ToString());
        Assert.Equal("Fifo", TopicMessageType.Fifo.ToString());
        Assert.Equal("Delay", TopicMessageType.Delay.ToString());
        Assert.Equal("Transaction", TopicMessageType.Transaction.ToString());
        Assert.Equal("Lite", TopicMessageType.Lite.ToString());
    }

    [Fact]
    [DisplayName("TopicMessageType_Lite枚举值字符串为Lite")]
    public void TopicMessageType_Lite_StringIsLite()
    {
        // 确认传入 Broker 的 topicMessageType 字段值为 "Lite"
        var typeStr = TopicMessageType.Lite.ToString();
        Assert.Equal("Lite", typeStr);
    }

    [Fact]
    [DisplayName("TopicMessageType_Normal枚举值字符串为Normal")]
    public void TopicMessageType_Normal_StringIsNormal()
    {
        Assert.Equal("Normal", TopicMessageType.Normal.ToString());
    }

    [Fact]
    [DisplayName("TopicMessageType_Lite值与其他类型不重复")]
    public void TopicMessageType_Lite_ValueIsUnique()
    {
        var liteValue = (Int32)TopicMessageType.Lite;
        Assert.NotEqual(liteValue, (Int32)TopicMessageType.Normal);
        Assert.NotEqual(liteValue, (Int32)TopicMessageType.Fifo);
        Assert.NotEqual(liteValue, (Int32)TopicMessageType.Delay);
        Assert.NotEqual(liteValue, (Int32)TopicMessageType.Transaction);
    }

    [Fact]
    [DisplayName("TopicMessageType_可从整数转换")]
    public void TopicMessageType_CanCastFromInt()
    {
        var lite = (TopicMessageType)4;
        Assert.Equal(TopicMessageType.Lite, lite);
    }

    [Fact]
    [DisplayName("TopicMessageType_共5个枚举值")]
    public void TopicMessageType_HasFiveValues()
    {
        var values = Enum.GetValues<TopicMessageType>();
        Assert.Equal(5, values.Length);
    }
}

