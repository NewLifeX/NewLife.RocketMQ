using System;
using System.ComponentModel;
using NewLife.RocketMQ.Protocol;
using Xunit;

namespace XUnitTestRocketMQ;

/// <summary>5.x MessageId格式测试</summary>
public class MessageId5xTests
{
    [Fact]
    [DisplayName("CreateMessageId5x_生成有效的5x格式ID")]
    public void CreateMessageId5x_GeneratesValidId()
    {
        var mac = new Byte[] { 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF };
        var id = MessageExt.CreateMessageId5x(1, mac, 12345, 67890);

        Assert.NotNull(id);
        Assert.Equal(32, id.Length);
        Assert.StartsWith("01", id, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    [DisplayName("TryParseMessageId5x_解析生成的ID")]
    public void TryParseMessageId5x_ParseCreatedId()
    {
        var mac = new Byte[] { 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF };
        var id = MessageExt.CreateMessageId5x(1, mac, 12345, 67890);

        var ok = MessageExt.TryParseMessageId5x(id, out var version, out var parsedMac, out var processId, out var counter);

        Assert.True(ok);
        Assert.Equal(1, version);
        Assert.Equal(mac, parsedMac);
        Assert.Equal(12345, processId);
        Assert.Equal(67890, counter);
    }

    [Fact]
    [DisplayName("TryParseMessageId5x_非5x格式返回false")]
    public void TryParseMessageId5x_Invalid_ReturnsFalse()
    {
        // 4.x格式（32个hex，但前缀不是01）
        var id = "AABBCCDD00001111000000000000FFFF";
        var ok = MessageExt.TryParseMessageId5x(id, out _, out _, out _, out _);
        Assert.False(ok);
    }

    [Fact]
    [DisplayName("TryParseMessageId5x_Null输入返回false")]
    public void TryParseMessageId5x_Null_ReturnsFalse()
    {
        var ok = MessageExt.TryParseMessageId5x(null, out _, out _, out _, out _);
        Assert.False(ok);
    }

    [Fact]
    [DisplayName("TryParseMessageId5x_空字符串返回false")]
    public void TryParseMessageId5x_Empty_ReturnsFalse()
    {
        var ok = MessageExt.TryParseMessageId5x("", out _, out _, out _, out _);
        Assert.False(ok);
    }

    [Fact]
    [DisplayName("TryParseMessageId5x_长度不匹配返回false")]
    public void TryParseMessageId5x_WrongLength_ReturnsFalse()
    {
        var ok = MessageExt.TryParseMessageId5x("0101AABB", out _, out _, out _, out _);
        Assert.False(ok);
    }

    [Fact]
    [DisplayName("IsMessageId5x_有效5x格式返回true")]
    public void IsMessageId5x_Valid_ReturnsTrue()
    {
        var mac = new Byte[] { 0x11, 0x22, 0x33, 0x44, 0x55, 0x66 };
        var id = MessageExt.CreateMessageId5x(1, mac, 100, 200);
        Assert.True(MessageExt.IsMessageId5x(id));
    }

    [Fact]
    [DisplayName("IsMessageId5x_4x格式返回false")]
    public void IsMessageId5x_4xFormat_ReturnsFalse()
    {
        // 典型的4.x格式MsgId（16字节=32hex，但前缀不是01）
        Assert.False(MessageExt.IsMessageId5x("C0A80001000030390000000000000001"));
    }

    [Fact]
    [DisplayName("IsMessageId5x_Null返回false")]
    public void IsMessageId5x_Null_ReturnsFalse()
    {
        Assert.False(MessageExt.IsMessageId5x(null));
    }

    [Fact]
    [DisplayName("CreateMessageId5x_无MAC时使用随机字节")]
    public void CreateMessageId5x_NullMac_UsesRandom()
    {
        var id1 = MessageExt.CreateMessageId5x(1, null, 1, 1);
        var id2 = MessageExt.CreateMessageId5x(1, null, 1, 1);

        Assert.NotNull(id1);
        Assert.Equal(32, id1.Length);
        Assert.StartsWith("01", id1, StringComparison.OrdinalIgnoreCase);
        // 随机MAC，两次结果可能不同（概率极高）
    }

    [Fact]
    [DisplayName("CreateMessageId5x_不同计数器产生不同ID")]
    public void CreateMessageId5x_DifferentCounter_DifferentId()
    {
        var mac = new Byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06 };
        var id1 = MessageExt.CreateMessageId5x(1, mac, 100, 1);
        var id2 = MessageExt.CreateMessageId5x(1, mac, 100, 2);

        Assert.NotEqual(id1, id2);
    }
}
