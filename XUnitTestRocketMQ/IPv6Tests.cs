using System;
using System.ComponentModel;
using System.IO;
using System.Net;
using NewLife.Data;
using NewLife.RocketMQ.Protocol;
using Xunit;

namespace XUnitTestRocketMQ;

/// <summary>IPv6消息解码测试</summary>
public class IPv6Tests
{
    /// <summary>写入大端序Int32</summary>
    private static void WriteBigEndianInt32(MemoryStream ms, Int32 value)
    {
        ms.WriteByte((Byte)(value >> 24));
        ms.WriteByte((Byte)(value >> 16));
        ms.WriteByte((Byte)(value >> 8));
        ms.WriteByte((Byte)value);
    }

    /// <summary>写入大端序Int64</summary>
    private static void WriteBigEndianInt64(MemoryStream ms, Int64 value)
    {
        WriteBigEndianInt32(ms, (Int32)(value >> 32));
        WriteBigEndianInt32(ms, (Int32)value);
    }

    /// <summary>写入大端序Int16</summary>
    private static void WriteBigEndianInt16(MemoryStream ms, Int16 value)
    {
        ms.WriteByte((Byte)(value >> 8));
        ms.WriteByte((Byte)value);
    }

    /// <summary>构造消息的二进制数据</summary>
    private static Byte[] BuildMessageBinary(Boolean ipv6)
    {
        var ms = new MemoryStream();

        var ipBytes = ipv6 ? IPAddress.IPv6Loopback.GetAddressBytes() : new Byte[] { 127, 0, 0, 1 };
        // SysFlag: 第2位(0x04)标识IPv6
        var sysFlag = ipv6 ? 4 : 0;

        var body = "hello"u8.ToArray();
        var topic = "test_topic"u8.ToArray();
        var props = ""u8.ToArray();

        // 计算StoreSize
        var ipSize = ipv6 ? 16 : 4;
        // StoreSize(4) + MagicCode(4) + BodyCRC(4) + QueueId(4) + Flag(4) +
        // QueueOffset(8) + CommitLogOffset(8) + SysFlag(4) +
        // BornTimestamp(8) + BornIP(ipSize) + BornPort(4) +
        // StoreTimestamp(8) + StoreIP(ipSize) + StorePort(4) +
        // ReconsumeTimes(4) + PreparedTransactionOffset(8) +
        // BodyLen(4) + body + TopicLen(1) + topic + PropsLen(2) + props
        var storeSize = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 +
                        8 + ipSize + 4 +
                        8 + ipSize + 4 +
                        4 + 8 +
                        4 + body.Length + 1 + topic.Length + 2 + props.Length;

        WriteBigEndianInt32(ms, storeSize);  // StoreSize
        WriteBigEndianInt32(ms, 0);          // MagicCode
        WriteBigEndianInt32(ms, 0);          // BodyCRC
        WriteBigEndianInt32(ms, 1);          // QueueId
        WriteBigEndianInt32(ms, 0);          // Flag
        WriteBigEndianInt64(ms, 100L);       // QueueOffset
        WriteBigEndianInt64(ms, 200L);       // CommitLogOffset
        WriteBigEndianInt32(ms, sysFlag);    // SysFlag

        WriteBigEndianInt64(ms, 1000L);      // BornTimestamp
        ms.Write(ipBytes, 0, ipBytes.Length);// BornHost IP
        WriteBigEndianInt32(ms, 9876);       // BornHost Port

        WriteBigEndianInt64(ms, 2000L);      // StoreTimestamp
        ms.Write(ipBytes, 0, ipBytes.Length);// StoreHost IP
        WriteBigEndianInt32(ms, 10911);      // StoreHost Port

        WriteBigEndianInt32(ms, 0);          // ReconsumeTimes
        WriteBigEndianInt64(ms, 0L);         // PreparedTransactionOffset

        WriteBigEndianInt32(ms, body.Length); // BodyLength
        ms.Write(body, 0, body.Length);

        ms.WriteByte((Byte)topic.Length);    // Topic length (1 byte)
        ms.Write(topic, 0, topic.Length);

        WriteBigEndianInt16(ms, (Int16)props.Length); // Properties length (2 bytes)
        if (props.Length > 0) ms.Write(props, 0, props.Length);

        return ms.ToArray();
    }

    [Fact]
    [DisplayName("IPv4消息正确解码")]
    public void ReadMessage_IPv4()
    {
        var data = BuildMessageBinary(false);
        var pk = new ArrayPacket(data);
        var msgs = MessageExt.ReadAll(pk);

        Assert.Single(msgs);
        var msg = msgs[0];
        Assert.Equal(1, msg.QueueId);
        Assert.Equal(100, msg.QueueOffset);
        Assert.Equal(200, msg.CommitLogOffset);
        Assert.Equal(0, msg.SysFlag & 4); // 不是IPv6
        Assert.StartsWith("127.0.0.1:", msg.BornHost);
        Assert.StartsWith("127.0.0.1:", msg.StoreHost);
        Assert.Contains("9876", msg.BornHost);
        Assert.Contains("10911", msg.StoreHost);
        Assert.Equal("hello", msg.BodyString);
        Assert.Equal("test_topic", msg.Topic);
        // IPv4 MsgId应该是32个hex字符(16字节)
        Assert.Equal(32, msg.MsgId.Length);
    }

    [Fact]
    [DisplayName("IPv6消息正确解码")]
    public void ReadMessage_IPv6()
    {
        var data = BuildMessageBinary(true);
        var pk = new ArrayPacket(data);
        var msgs = MessageExt.ReadAll(pk);

        Assert.Single(msgs);
        var msg = msgs[0];
        Assert.Equal(1, msg.QueueId);
        Assert.Equal(100, msg.QueueOffset);
        Assert.Equal(200, msg.CommitLogOffset);
        Assert.NotEqual(0, msg.SysFlag & 4); // 是IPv6
        Assert.Contains("::1", msg.BornHost);
        Assert.Contains("::1", msg.StoreHost);
        Assert.Contains("9876", msg.BornHost);
        Assert.Contains("10911", msg.StoreHost);
        Assert.Equal("hello", msg.BodyString);
        Assert.Equal("test_topic", msg.Topic);
        // IPv6 MsgId应该是56个hex字符(28字节)
        Assert.Equal(56, msg.MsgId.Length);
    }

    [Fact]
    [DisplayName("SysFlag第2位判断IPv6")]
    public void SysFlag_IPv6_Bit()
    {
        // SysFlag=0 -> IPv4
        Assert.Equal(0, 0 & 4);

        // SysFlag=4 -> IPv6
        Assert.NotEqual(0, 4 & 4);

        // SysFlag=5(压缩+IPv6) -> IPv6
        Assert.NotEqual(0, 5 & 4);

        // SysFlag=1(仅压缩) -> IPv4
        Assert.Equal(0, 1 & 4);
    }
}
