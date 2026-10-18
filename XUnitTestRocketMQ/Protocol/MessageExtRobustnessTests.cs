using System;
using System.Collections.Generic;
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
    private static Byte[] BuildMessage(String topic, String body, Int32 sysFlag = 0) => BuildMessage(topic, body.GetBytes(), sysFlag);

    /// <summary>构造一条最小有效消息的二进制字节（大端序）</summary>
    private static Byte[] BuildMessage(String topic, Byte[] bodyBytes, Int32 sysFlag = 0)
    {
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
        writer.Write(sysFlag);                      // SysFlag
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
    [DisplayName("ReadAll_中间消息解码失败_跳过并继续解析后续消息")]
    public void ReadAll_DecodeFailure_SkipsAndContinues()
    {
        var first = BuildMessage("test-topic", "hello");
        // SysFlag=1 标记 zlib 压缩，但 Body 为不可解压数据，解码时抛异常
        var corrupt = BuildMessage("bad-topic", [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF], 1);
        var last = BuildMessage("test-topic", "world");

        var all = new Byte[first.Length + corrupt.Length + last.Length];
        Buffer.BlockCopy(first, 0, all, 0, first.Length);
        Buffer.BlockCopy(corrupt, 0, all, first.Length, corrupt.Length);
        Buffer.BlockCopy(last, 0, all, first.Length + corrupt.Length, last.Length);

        var errors = new List<MessageDecodeError>();
        var msgs = MessageExt.ReadAll(new ArrayPacket(all), errors.Add);

        // 坏消息被跳过，前后两条正常消息均被解析
        Assert.Equal(2, msgs.Count);
        Assert.Equal("hello", msgs[0].BodyString);
        Assert.Equal("world", msgs[1].BodyString);

        var err = Assert.Single(errors);
        Assert.Equal(first.Length, err.Offset);
        Assert.Equal(corrupt.Length, err.StoreSize);
        Assert.Equal(corrupt, err.Data);
        Assert.NotNull(err.Exception);
    }

    [Fact]
    [DisplayName("ReadAll_整批消息解码失败_返回空列表并逐条上报")]
    public void ReadAll_AllDecodeFailed_ReturnsEmptyAndReports()
    {
        var bad1 = BuildMessage("bad-topic", [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF], 1);
        var bad2 = BuildMessage("bad-topic", [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF], 1);

        var all = new Byte[bad1.Length + bad2.Length];
        Buffer.BlockCopy(bad1, 0, all, 0, bad1.Length);
        Buffer.BlockCopy(bad2, 0, all, bad1.Length, bad2.Length);

        var errors = new List<MessageDecodeError>();
        var msgs = MessageExt.ReadAll(new ArrayPacket(all), errors.Add);

        Assert.Empty(msgs);
        Assert.Equal(2, errors.Count);
        Assert.Equal(0, errors[0].Offset);
        Assert.Equal(bad1.Length, errors[1].Offset);
    }

    [Fact]
    [DisplayName("ReadAll_压缩大消息_正常解码")]
    public void ReadAll_LargeCompressedMessage()
    {
        // 回归 #120：6MB 消息压缩后解码，解压结果远超 RocketMQ 默认 4MB 上限
        var body = new Byte[6 * 1024 * 1024];
        for (var i = 0; i < body.Length; i++) body[i] = (Byte)(i * 31 % 251);

        var compressed = new ZlibMessageCompressor().Compress(body);
        var data = BuildMessage("test-topic", compressed, 1);

        var msgs = MessageExt.ReadAll(new ArrayPacket(data));

        var msg = Assert.Single(msgs);
        Assert.Equal(body.Length, msg.Body.Length);
        Assert.True(body.AsSpan().SequenceEqual(msg.Body));
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
