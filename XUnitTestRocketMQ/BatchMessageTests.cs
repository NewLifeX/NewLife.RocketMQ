using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Text;
using NewLife;
using NewLife.Data;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Protocol;
using Xunit;

namespace XUnitTestRocketMQ;

/// <summary>批量消息发送测试</summary>
[Collection("Basic")]
public class BatchMessageTests
{
    [Fact]
    [DisplayName("批量消息发送_消息列表为空时抛出异常")]
    public void PublishBatch_EmptyList_ThrowsException()
    {
        using var mq = new Producer { Topic = "nx_test" };

        Assert.Throws<ArgumentException>(() => mq.PublishBatch(new List<Message>()));
    }

    [Fact]
    [DisplayName("批量消息发送_消息列表为null时抛出异常")]
    public void PublishBatch_NullList_ThrowsException()
    {
        using var mq = new Producer { Topic = "nx_test" };

        Assert.Throws<ArgumentException>(() => mq.PublishBatch((IList<Message>)null));
    }

    [Fact]
    [DisplayName("批量消息字符串重载_空列表抛出异常")]
    public void PublishBatch_StringOverload_EmptyList_ThrowsException()
    {
        using var mq = new Producer { Topic = "nx_test" };

        Assert.Throws<ArgumentException>(() => mq.PublishBatch(new List<String>()));
    }

    [Fact(Skip = "需要RocketMQ服务器")]
    [DisplayName("批量消息发送_发送多条消息")]
    public void PublishBatch_SendMultipleMessages()
    {
        var set = BasicTest.GetConfig();
        using var mq = new Producer
        {
            Topic = "nx_test",
            NameServerAddress = set.NameServer,
        };
        mq.Start();

        var messages = new List<Message>();
        for (var i = 0; i < 5; i++)
        {
            var msg = new Message();
            msg.SetBody($"批量消息{i}");
            msg.Tags = "TagBatch";
            messages.Add(msg);
        }

        var result = mq.PublishBatch(messages);
        Assert.NotNull(result);
        Assert.Equal(SendStatus.SendOK, result.Status);
    }

    #region 批量消息解码辅助方法
    /// <summary>写入大端序Int32</summary>
    private static void WriteBE32(MemoryStream ms, Int32 value)
    {
        ms.WriteByte((Byte)(value >> 24));
        ms.WriteByte((Byte)(value >> 16));
        ms.WriteByte((Byte)(value >> 8));
        ms.WriteByte((Byte)value);
    }

    /// <summary>写入大端序Int64</summary>
    private static void WriteBE64(MemoryStream ms, Int64 value)
    {
        WriteBE32(ms, (Int32)(value >> 32));
        WriteBE32(ms, (Int32)value);
    }

    /// <summary>写入大端序Int16</summary>
    private static void WriteBE16(MemoryStream ms, Int16 value)
    {
        ms.WriteByte((Byte)(value >> 8));
        ms.WriteByte((Byte)value);
    }

    /// <summary>构建批量消息Body（多条子消息的二进制数据）</summary>
    private static Byte[] BuildBatchBody(params (String body, String topic, String props)[] items)
    {
        var ms = new MemoryStream();

        foreach (var (body, topic, props) in items)
        {
            var bodyBytes = body.GetBytes();
            var topicBytes = topic.GetBytes();
            var propsBytes = props.GetBytes();

            // TotalSize = 4+4+4+4 + 4+bodyLen + 1+topicLen + 2+propsLen
            var totalSize = 4 + 4 + 4 + 4 + bodyBytes.Length + 1 + topicBytes.Length + 2 + propsBytes.Length;

            WriteBE32(ms, totalSize);       // TotalSize
            WriteBE32(ms, 0);               // MagicCode
            WriteBE32(ms, 0);               // BodyCRC
            WriteBE32(ms, 0);               // Flag

            WriteBE32(ms, bodyBytes.Length); // BodyLength
            ms.Write(bodyBytes, 0, bodyBytes.Length);

            ms.WriteByte((Byte)topicBytes.Length); // TopicLength
            ms.Write(topicBytes, 0, topicBytes.Length);

            WriteBE16(ms, (Int16)propsBytes.Length); // PropertiesLength
            if (propsBytes.Length > 0) ms.Write(propsBytes, 0, propsBytes.Length);
        }

        return ms.ToArray();
    }

    /// <summary>构建一条完整的外层消息二进制（带SysFlag=0x10标识批量）</summary>
    private static Byte[] BuildOuterMessage(Byte[] batchBody)
    {
        var ms = new MemoryStream();
        var ipBytes = new Byte[] { 127, 0, 0, 1 };
        var topic = "batch_topic"u8.ToArray();
        var props = ""u8.ToArray();
        var sysFlag = 0x10; // 批量消息标志

        var storeSize = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 +
                        8 + 4 + 4 +
                        8 + 4 + 4 +
                        4 + 8 +
                        4 + batchBody.Length + 1 + topic.Length + 2 + props.Length;

        WriteBE32(ms, storeSize);
        WriteBE32(ms, 0);            // MagicCode
        WriteBE32(ms, 0);            // BodyCRC
        WriteBE32(ms, 0);            // QueueId
        WriteBE32(ms, 0);            // Flag
        WriteBE64(ms, 0L);           // QueueOffset
        WriteBE64(ms, 100L);         // CommitLogOffset
        WriteBE32(ms, sysFlag);      // SysFlag (batch)

        WriteBE64(ms, 1000L);        // BornTimestamp
        ms.Write(ipBytes, 0, 4);     // BornHost IP
        WriteBE32(ms, 9876);         // BornHost Port

        WriteBE64(ms, 2000L);        // StoreTimestamp
        ms.Write(ipBytes, 0, 4);     // StoreHost IP
        WriteBE32(ms, 10911);        // StoreHost Port

        WriteBE32(ms, 0);            // ReconsumeTimes
        WriteBE64(ms, 0L);           // PreparedTransactionOffset

        WriteBE32(ms, batchBody.Length); // BodyLength
        ms.Write(batchBody, 0, batchBody.Length);

        ms.WriteByte((Byte)topic.Length);
        ms.Write(topic, 0, topic.Length);

        WriteBE16(ms, (Int16)props.Length);

        return ms.ToArray();
    }
    #endregion

    #region 批量消息解码测试
    [Fact]
    [DisplayName("批量消息解码_解码2条子消息")]
    public void DecodeBatch_TwoMessages()
    {
        var batchBody = BuildBatchBody(
            ("hello", "topic1", ""),
            ("world", "topic2", "")
        );

        var parent = new MessageExt
        {
            QueueId = 1,
            CommitLogOffset = 100,
            SysFlag = 0x10,
            BornTimestamp = 1000,
            BornHost = "127.0.0.1:9876",
            StoreTimestamp = 2000,
            StoreHost = "127.0.0.1:10911",
            Body = batchBody,
            Topic = "batch_topic",
            MsgId = "PARENT_ID",
        };

        var msgs = MessageExt.DecodeBatch(parent);

        Assert.Equal(2, msgs.Count);

        Assert.Equal("hello", msgs[0].BodyString);
        Assert.Equal("topic1", msgs[0].Topic);
        Assert.Equal(1, msgs[0].QueueId);
        Assert.Equal(100, msgs[0].CommitLogOffset);
        Assert.Equal("127.0.0.1:9876", msgs[0].BornHost);

        Assert.Equal("world", msgs[1].BodyString);
        Assert.Equal("topic2", msgs[1].Topic);
    }

    [Fact]
    [DisplayName("批量消息解码_解码带Properties的子消息")]
    public void DecodeBatch_WithProperties()
    {
        var props = "TAGS\u0001TagA\u0002KEYS\u0001Key1\u0002";
        var batchBody = BuildBatchBody(
            ("data", "my_topic", props)
        );

        var parent = new MessageExt
        {
            SysFlag = 0x10,
            Body = batchBody,
            MsgId = "P1",
        };

        var msgs = MessageExt.DecodeBatch(parent);

        Assert.Single(msgs);
        Assert.Equal("data", msgs[0].BodyString);
        Assert.Equal("my_topic", msgs[0].Topic);
        Assert.Equal("TagA", msgs[0].Tags);
        Assert.Equal("Key1", msgs[0].Keys);
    }

    [Fact]
    [DisplayName("批量消息解码_空Body返回空列表")]
    public void DecodeBatch_EmptyBody()
    {
        var parent = new MessageExt
        {
            SysFlag = 0x10,
            Body = [],
            MsgId = "P1",
        };

        var msgs = MessageExt.DecodeBatch(parent);
        Assert.Empty(msgs);
    }

    [Fact]
    [DisplayName("批量消息解码_Null参数抛出异常")]
    public void DecodeBatch_NullParent_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => MessageExt.DecodeBatch(null));
    }

    [Fact]
    [DisplayName("批量消息ReadAll自动展开")]
    public void ReadAll_BatchMessage_AutoExpand()
    {
        var batchBody = BuildBatchBody(
            ("msg1", "t1", ""),
            ("msg2", "t2", ""),
            ("msg3", "t3", "")
        );

        var data = BuildOuterMessage(batchBody);
        var pk = new ArrayPacket(data);
        var msgs = MessageExt.ReadAll(pk);

        Assert.Equal(3, msgs.Count);
        Assert.Equal("msg1", msgs[0].BodyString);
        Assert.Equal("msg2", msgs[1].BodyString);
        Assert.Equal("msg3", msgs[2].BodyString);
        Assert.Equal("t1", msgs[0].Topic);
        Assert.Equal("t2", msgs[1].Topic);
        Assert.Equal("t3", msgs[2].Topic);
    }

    [Fact]
    [DisplayName("SysFlag批量位判断")]
    public void SysFlag_Batch_Bit()
    {
        Assert.NotEqual(0, 0x10 & 0x10);  // 批量
        Assert.Equal(0, 0 & 0x10);        // 普通
        Assert.NotEqual(0, 0x11 & 0x10);  // 压缩+批量
        Assert.Equal(0, 0x01 & 0x10);     // 仅压缩
    }
    #endregion
}
