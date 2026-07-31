using System;
using System.ComponentModel;
using NewLife;
using NewLife.Buffers;
using NewLife.Data;
using NewLife.RocketMQ.Protocol;
using Xunit;

namespace XUnitTest.Protocol;

/// <summary>MessageExt 解析健壮性测试</summary>
public class MessageExtRobustnessTests
{
    /// <summary>构造一条最小有效消息的二进制字节（大端序）</summary>
    private static Byte[] BuildMessage(String topic, String body)
    {
        var bodyBytes = body.GetBytes();
        var topicBytes = topic.GetBytes();
        var propsBytes = Array.Empty<Byte>();

        // 固定部分 + 变长部分
        var storeSize = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + 4 + 4 + 8 + 4 + 4 + 4 + 8 + 4 + bodyBytes.Length + 1 + topicBytes.Length + 2 + propsBytes.Length;
        var buf = new Byte[storeSize];
        var writer = new SpanWriter(buf) { IsLittleEndian = false };

        writer.Write(storeSize);                    // StoreSize
        writer.Write((Int32)0);                     // MagicCode
        writer.Write((Int32)0);                     // BodyCRC
        writer.Write((Int32)0);                     // QueueId
        writer.Write((Int32)0);                     // Flag
        writer.Write((Int64)0);                     // QueueOffset
        writer.Write((Int64)100);                   // CommitLogOffset
        writer.Write((Int32)0);                     // SysFlag
        writer.Write((Int64)0);                     // BornTimestamp
        writer.Write(new Byte[4]);                  // BornHost IPv4
        writer.Write((Int32)0);                     // BornPort
        writer.Write((Int64)0);                     // StoreTimestamp
        writer.Write(new Byte[4]);                  // StoreHost IPv4
        writer.Write((Int32)0);                     // StorePort
        writer.Write((Int32)0);                     // ReconsumeTimes
        writer.Write((Int64)0);                     // PreparedTransactionOffset
        writer.Write(bodyBytes.Length);             // BodyLen
        writer.Write(bodyBytes);                    // Body
        writer.Write((Byte)topicBytes.Length);      // TopicLen
        writer.Write(topicBytes);                   // Topic
        writer.Write((Int16)propsBytes.Length);     // PropsLen
        writer.Write(propsBytes);                   // Props

        return buf;
    }

    [Fact]
    [DisplayName("ReadAll_单条损坏消息_不影响前面正常消息")]
    public void ReadAll_CorruptMessage_Isolated()
    {
        // 一条有效消息 + 一段损坏数据（BodyLen=1000 但无数据，触发长度校验异常）
        var valid = BuildMessage("test-topic", "hello");
        var corrupt = new Byte[]
        {
            0x00, 0x00, 0x00, 0x20,                         // StoreSize=32
            0x00, 0x00, 0x00, 0x00,                         // MagicCode
            0x00, 0x00, 0x00, 0x00,                         // BodyCRC
            0x00, 0x00, 0x00, 0x00,                         // QueueId
            0x00, 0x00, 0x00, 0x00,                         // Flag
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // QueueOffset
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // CommitLogOffset
            0x00, 0x00, 0x00, 0x00,                         // SysFlag
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // BornTimestamp
            0x00, 0x00, 0x00, 0x00,                         // BornHost
            0x00, 0x00, 0x00, 0x00,                         // BornPort
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // StoreTimestamp
            0x00, 0x00, 0x00, 0x00,                         // StoreHost
            0x00, 0x00, 0x00, 0x00,                         // StorePort
            0x00, 0x00, 0x00, 0x00,                         // ReconsumeTimes
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // PreparedTransactionOffset
            0x00, 0x00, 0x03, 0xE8,                         // BodyLen=1000（无数据）
        };

        var all = new Byte[valid.Length + corrupt.Length];
        Buffer.BlockCopy(valid, 0, all, 0, valid.Length);
        Buffer.BlockCopy(corrupt, 0, all, valid.Length, corrupt.Length);

        // 不抛异常，正常消息被解析出来
        var msgs = MessageExt.ReadAll(new ArrayPacket(all));

        Assert.Single(msgs);
        Assert.Equal("hello", msgs[0].BodyString);
        Assert.Equal("test-topic", msgs[0].Topic);
        Assert.Equal(100, msgs[0].CommitLogOffset);
    }

    [Fact]
    [DisplayName("ReadAll_负BodyLen_安全跳过")]
    public void ReadAll_NegativeBodyLen_SafeSkip()
    {
        // 仅一段损坏数据：BodyLen 为负数
        var corrupt = new Byte[]
        {
            0x00, 0x00, 0x00, 0x20,                         // StoreSize=32
            0x00, 0x00, 0x00, 0x00,                         // MagicCode
            0x00, 0x00, 0x00, 0x00,                         // BodyCRC
            0x00, 0x00, 0x00, 0x00,                         // QueueId
            0x00, 0x00, 0x00, 0x00,                         // Flag
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // QueueOffset
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // CommitLogOffset
            0x00, 0x00, 0x00, 0x00,                         // SysFlag
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // BornTimestamp
            0x00, 0x00, 0x00, 0x00,                         // BornHost
            0x00, 0x00, 0x00, 0x00,                         // BornPort
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // StoreTimestamp
            0x00, 0x00, 0x00, 0x00,                         // StoreHost
            0x00, 0x00, 0x00, 0x00,                         // StorePort
            0x00, 0x00, 0x00, 0x00,                         // ReconsumeTimes
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // PreparedTransactionOffset
            0xFF, 0xFF, 0xFF, 0xFF,                         // BodyLen=-1（非法）
        };

        var msgs = MessageExt.ReadAll(new ArrayPacket(corrupt));

        // 安全跳过，无消息且不抛异常
        Assert.Empty(msgs);
    }

    [Fact]
    [DisplayName("Body_外部赋值后_BodyString缓存失效")]
    public void Body_DirectAssign_InvalidatesBodyStringCache()
    {
        var message = new Message();
        message.SetBody("first");
        Assert.Equal("first", message.BodyString);

        // 外部直接给 Body 赋值
        message.Body = "second".GetBytes();

        // 缓存必须失效，返回新值
        Assert.Equal("second", message.BodyString);
    }
}
