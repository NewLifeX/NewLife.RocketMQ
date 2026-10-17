using System;
using System.ComponentModel;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Grpc;
using NewLife.RocketMQ.Protocol;
using Xunit;

namespace XUnitTest.Protocol;

/// <summary>双协议消息模型转换助手测试</summary>
public class MessageConverterTests
{
    [Fact]
    [DisplayName("GrpcMessage_ToMessageExt_字段映射正确")]
    public void GrpcToMessageExt_MapsFields()
    {
        var grpc = new GrpcMessage
        {
            Topic = new GrpcResource { Name = "topic1" },
            Body = new Byte[] { 0x01, 0x02 },
            SystemProperties = new GrpcSystemProperties
            {
                Tag = "tagA",
                MessageId = "msg-001",
                QueueId = 3,
                QueueOffset = 12345,
                BornHost = "10.0.0.1:10911",
                DeliveryAttempt = 2,
                Keys = ["k1", "k2"],
            },
        };
        grpc.UserProperties["user_key"] = "user_value";

        var ext = grpc.ToMessageExt();

        Assert.Equal("topic1", ext.Topic);
        Assert.Equal(new Byte[] { 0x01, 0x02 }, ext.Body);
        Assert.Equal("tagA", ext.Tags);
        Assert.Equal("k1,k2", ext.Keys);
        Assert.Equal("msg-001", ext.MsgId);
        Assert.Equal(3, ext.QueueId);
        Assert.Equal(12345, ext.QueueOffset);
        Assert.Equal(2, ext.ReconsumeTimes);
        Assert.Equal("user_value", ext.Properties["user_key"]);
    }

    [Fact]
    [DisplayName("Message_ToGrpcMessage_字段映射正确")]
    public void MessageToGrpc_MapsFields()
    {
        var msg = new Message
        {
            Topic = "topic1",
            Body = new Byte[] { 0x03, 0x04 },
            Tags = "tagB",
            Keys = "k1,k2",
        };
        msg.Properties["user_key"] = "user_value";

        var grpc = msg.ToGrpcMessage();

        Assert.Equal("topic1", grpc.Topic?.Name);
        Assert.Equal(new Byte[] { 0x03, 0x04 }, grpc.Body);
        Assert.Equal("tagB", grpc.SystemProperties?.Tag);
        Assert.Equal(GrpcMessageType.NORMAL, grpc.SystemProperties?.MessageType);
        Assert.Contains("k1", grpc.SystemProperties?.Keys);
        Assert.Contains("k2", grpc.SystemProperties?.Keys);
        Assert.Equal("user_value", grpc.UserProperties["user_key"]);
        // 内部系统属性不应混入用户属性
        Assert.DoesNotContain("TAGS", grpc.UserProperties.Keys);
    }

    [Fact]
    [DisplayName("GrpcMessage_ToMessageExt_null_抛出ArgumentNullException")]
    public void GrpcToMessageExt_Null_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => ((GrpcMessage)null).ToMessageExt());
    }

    [Fact]
    [DisplayName("GrpcMessage_ToMessage_null_抛出ArgumentNullException")]
    public void GrpcToMessage_Null_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => ((GrpcMessage)null).ToMessage());
    }

    [Fact]
    [DisplayName("Message_ToGrpcMessage_null_抛出ArgumentNullException")]
    public void MessageToGrpc_Null_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => ((Message)null).ToGrpcMessage());
    }

    [Fact]
    [DisplayName("GrpcMessage_无系统属性_转换使用默认值")]
    public void GrpcToMessageExt_WithoutSystemProperties_UsesDefaults()
    {
        var grpc = new GrpcMessage
        {
            Topic = new GrpcResource { Name = "topic-default" },
            Body = new Byte[] { 0x05 },
        };

        var ext = grpc.ToMessageExt();

        Assert.Equal("topic-default", ext.Topic);
        Assert.Equal(new Byte[] { 0x05 }, ext.Body);
        Assert.Equal(0, ext.QueueId);
        Assert.Equal(0, ext.QueueOffset);
        Assert.Equal(0, ext.ReconsumeTimes);
        Assert.Null(ext.Tags);
        Assert.Null(ext.Keys);
    }

    [Fact]
    [DisplayName("Message_ToGrpcMessage_UNIQ_KEY映射为MsgId且空Keys被过滤")]
    public void MessageToGrpc_UniqKeyAndEmptyKeys()
    {
        var msg = new Message
        {
            Topic = "topic1",
            Body = new Byte[] { 0x06 },
            Tags = "tag",
            Keys = "k1,,k2,",
        };
        msg.Properties["UNIQ_KEY"] = "uniq-001";
        msg.Properties["TAGS"] = "should-be-skipped";

        var grpc = msg.ToGrpcMessage();

        Assert.Equal("uniq-001", grpc.SystemProperties?.MessageId);
        Assert.Equal(new[] { "k1", "k2" }, grpc.SystemProperties?.Keys);
        Assert.DoesNotContain("UNIQ_KEY", grpc.UserProperties.Keys);
        Assert.DoesNotContain("TAGS", grpc.UserProperties.Keys);
    }

    [Fact]
    [DisplayName("双向转换_往返字段保持")]
    public void RoundTrip_FieldsPreserved()
    {
        var msg = new Message
        {
            Topic = "round-trip",
            Body = new Byte[] { 0xAA, 0xBB },
            Tags = "tagX",
        };
        msg.Properties["custom"] = "value";

        var grpc = msg.ToGrpcMessage();
        var back = grpc.ToMessage();

        Assert.Equal("round-trip", back.Topic);
        Assert.Equal(new Byte[] { 0xAA, 0xBB }, back.Body);
        Assert.Equal("tagX", back.Tags);
        Assert.Equal("value", back.Properties["custom"]);
    }
}
