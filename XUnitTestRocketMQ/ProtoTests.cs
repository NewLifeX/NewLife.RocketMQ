using System;
using System.Collections.Generic;
using System.ComponentModel;
using NewLife.RocketMQ.Grpc;
using Xunit;

namespace XUnitTestRocketMQ;

/// <summary>Protobuf编解码器测试</summary>
public class ProtoTests
{
    #region ProtoWriter/ProtoReader 基础编解码
    [Fact]
    [DisplayName("Varint_编码解码正确")]
    public void Varint_RoundTrip()
    {
        var writer = new ProtoWriter();
        writer.WriteRawVarint(0);
        writer.WriteRawVarint(1);
        writer.WriteRawVarint(127);
        writer.WriteRawVarint(128);
        writer.WriteRawVarint(300);
        writer.WriteRawVarint(UInt64.MaxValue);

        var reader = new ProtoReader(writer.ToArray());
        Assert.Equal(0UL, reader.ReadRawVarint());
        Assert.Equal(1UL, reader.ReadRawVarint());
        Assert.Equal(127UL, reader.ReadRawVarint());
        Assert.Equal(128UL, reader.ReadRawVarint());
        Assert.Equal(300UL, reader.ReadRawVarint());
        Assert.Equal(UInt64.MaxValue, reader.ReadRawVarint());
        Assert.True(reader.IsEnd);
    }

    [Fact]
    [DisplayName("Fixed32_编码解码正确")]
    public void Fixed32_RoundTrip()
    {
        var writer = new ProtoWriter();
        writer.WriteRawFixed32(0);
        writer.WriteRawFixed32(12345);
        writer.WriteRawFixed32(UInt32.MaxValue);

        var reader = new ProtoReader(writer.ToArray());
        Assert.Equal(0U, reader.ReadRawFixed32());
        Assert.Equal(12345U, reader.ReadRawFixed32());
        Assert.Equal(UInt32.MaxValue, reader.ReadRawFixed32());
    }

    [Fact]
    [DisplayName("Fixed64_编码解码正确")]
    public void Fixed64_RoundTrip()
    {
        var writer = new ProtoWriter();
        writer.WriteRawFixed64(0);
        writer.WriteRawFixed64(1234567890123456789);
        writer.WriteRawFixed64(UInt64.MaxValue);

        var reader = new ProtoReader(writer.ToArray());
        Assert.Equal(0UL, reader.ReadRawFixed64());
        Assert.Equal(1234567890123456789UL, reader.ReadRawFixed64());
        Assert.Equal(UInt64.MaxValue, reader.ReadRawFixed64());
    }

    [Fact]
    [DisplayName("String字段_编码解码正确")]
    public void StringField_RoundTrip()
    {
        var writer = new ProtoWriter();
        writer.WriteString(1, "hello");
        writer.WriteString(2, "世界");
        writer.WriteString(3, "");  // 空字符串不写入

        var data = writer.ToArray();
        var reader = new ProtoReader(data);

        // field 1: string "hello"
        var (fn1, wt1) = reader.ReadTag();
        Assert.Equal(1, fn1);
        Assert.Equal(2, wt1); // length-delimited
        Assert.Equal("hello", reader.ReadString());

        // field 2: string "世界"
        var (fn2, wt2) = reader.ReadTag();
        Assert.Equal(2, fn2);
        Assert.Equal(2, wt2);
        Assert.Equal("世界", reader.ReadString());

        // 没有 field 3（空字符串跳过）
        Assert.True(reader.IsEnd);
    }

    [Fact]
    [DisplayName("Int32字段_编码解码正确")]
    public void Int32Field_RoundTrip()
    {
        var writer = new ProtoWriter();
        writer.WriteInt32(1, 42);
        writer.WriteInt32(2, -1); // 负数编码为10字节varint
        writer.WriteInt32(3, 0);  // 0不写入

        var data = writer.ToArray();
        var reader = new ProtoReader(data);

        var (fn1, wt1) = reader.ReadTag();
        Assert.Equal(1, fn1);
        Assert.Equal(0, wt1); // varint
        Assert.Equal(42, reader.ReadInt32());

        var (fn2, wt2) = reader.ReadTag();
        Assert.Equal(2, fn2);
        Assert.Equal(-1, reader.ReadInt32());

        Assert.True(reader.IsEnd);
    }

    [Fact]
    [DisplayName("SInt32_ZigZag编码解码正确")]
    public void SInt32_ZigZag_RoundTrip()
    {
        var writer = new ProtoWriter();
        writer.WriteSInt32(1, 0);   // 不写入
        writer.WriteSInt32(2, 1);
        writer.WriteSInt32(3, -1);
        writer.WriteSInt32(4, Int32.MinValue);
        writer.WriteSInt32(5, Int32.MaxValue);

        var data = writer.ToArray();
        var reader = new ProtoReader(data);

        // field 2: sint32 = 1
        var (fn2, _) = reader.ReadTag();
        Assert.Equal(2, fn2);
        Assert.Equal(1, reader.ReadSInt32());

        // field 3: sint32 = -1
        var (fn3, _) = reader.ReadTag();
        Assert.Equal(3, fn3);
        Assert.Equal(-1, reader.ReadSInt32());

        // field 4: sint32 = Int32.MinValue
        var (fn4, _) = reader.ReadTag();
        Assert.Equal(4, fn4);
        Assert.Equal(Int32.MinValue, reader.ReadSInt32());

        // field 5: sint32 = Int32.MaxValue
        var (fn5, _) = reader.ReadTag();
        Assert.Equal(5, fn5);
        Assert.Equal(Int32.MaxValue, reader.ReadSInt32());

        Assert.True(reader.IsEnd);
    }

    [Fact]
    [DisplayName("Bool字段_编码解码正确")]
    public void BoolField_RoundTrip()
    {
        var writer = new ProtoWriter();
        writer.WriteBool(1, true);
        writer.WriteBool(2, false); // false不写入

        var data = writer.ToArray();
        var reader = new ProtoReader(data);

        var (fn1, _) = reader.ReadTag();
        Assert.Equal(1, fn1);
        Assert.True(reader.ReadBool());

        Assert.True(reader.IsEnd);
    }

    [Fact]
    [DisplayName("Bytes字段_编码解码正确")]
    public void BytesField_RoundTrip()
    {
        var testData = new Byte[] { 0x01, 0x02, 0x03, 0xFF };
        var writer = new ProtoWriter();
        writer.WriteBytes(1, testData);
        writer.WriteBytes(2, null);    // null不写入
        writer.WriteBytes(3, []);      // 空不写入

        var data = writer.ToArray();
        var reader = new ProtoReader(data);

        var (fn1, wt1) = reader.ReadTag();
        Assert.Equal(1, fn1);
        Assert.Equal(2, wt1);
        Assert.Equal(testData, reader.ReadBytes());

        Assert.True(reader.IsEnd);
    }

    [Fact]
    [DisplayName("Map字段_编码解码正确")]
    public void MapField_RoundTrip()
    {
        var map = new Dictionary<String, String>
        {
            ["key1"] = "value1",
            ["key2"] = "value2",
        };

        var writer = new ProtoWriter();
        writer.WriteMap(1, map);

        var data = writer.ToArray();
        var reader = new ProtoReader(data);

        var result = new Dictionary<String, String>();
        while (!reader.IsEnd)
        {
            var (fn, wt) = reader.ReadTag();
            Assert.Equal(1, fn);
            Assert.Equal(2, wt);
            var (k, v) = reader.ReadMapEntry();
            result[k] = v;
        }

        Assert.Equal(2, result.Count);
        Assert.Equal("value1", result["key1"]);
        Assert.Equal("value2", result["key2"]);
    }

    [Fact]
    [DisplayName("SkipField_正确跳过未知字段")]
    public void SkipField_Works()
    {
        var writer = new ProtoWriter();
        writer.WriteInt32(1, 42);       // varint
        writer.WriteString(2, "skip");  // length-delimited
        writer.WriteInt32(3, 99);       // varint

        var data = writer.ToArray();
        var reader = new ProtoReader(data);

        // 读field 1
        var (fn1, wt1) = reader.ReadTag();
        Assert.Equal(1, fn1);
        Assert.Equal(42, reader.ReadInt32());

        // 跳过field 2
        var (fn2, wt2) = reader.ReadTag();
        Assert.Equal(2, fn2);
        reader.SkipField(wt2);

        // 读field 3
        var (fn3, wt3) = reader.ReadTag();
        Assert.Equal(3, fn3);
        Assert.Equal(99, reader.ReadInt32());

        Assert.True(reader.IsEnd);
    }
    #endregion

    #region Timestamp/Duration
    [Fact]
    [DisplayName("Timestamp_编码解码正确")]
    public void Timestamp_RoundTrip()
    {
        var time = new DateTime(2024, 6, 15, 12, 30, 45, DateTimeKind.Utc);

        var writer = new ProtoWriter();
        writer.WriteTimestamp(1, time);

        var data = writer.ToArray();
        var reader = new ProtoReader(data);

        var (fn, wt) = reader.ReadTag();
        Assert.Equal(1, fn);
        Assert.Equal(2, wt);

        var result = reader.ReadTimestamp();
        // 允许毫秒级误差（因为Timestamp精度是纳秒/100）
        Assert.Equal(time.Year, result.Year);
        Assert.Equal(time.Month, result.Month);
        Assert.Equal(time.Day, result.Day);
        Assert.Equal(time.Hour, result.Hour);
        Assert.Equal(time.Minute, result.Minute);
        Assert.Equal(time.Second, result.Second);
    }

    [Fact]
    [DisplayName("Duration_编码解码正确")]
    public void Duration_RoundTrip()
    {
        var duration = TimeSpan.FromSeconds(90);

        var writer = new ProtoWriter();
        writer.WriteDuration(1, duration);

        var data = writer.ToArray();
        var reader = new ProtoReader(data);

        var (fn, _) = reader.ReadTag();
        Assert.Equal(1, fn);

        var result = reader.ReadDuration();
        Assert.Equal(90, (Int32)result.TotalSeconds);
    }
    #endregion

    #region 嵌套消息
    [Fact]
    [DisplayName("嵌套消息_编码解码正确")]
    public void NestedMessage_RoundTrip()
    {
        var resource = new GrpcResource
        {
            ResourceNamespace = "test-ns",
            Name = "test-topic",
        };

        var writer = new ProtoWriter();
        writer.WriteMessage(1, resource);

        var data = writer.ToArray();
        var reader = new ProtoReader(data);

        var (fn, wt) = reader.ReadTag();
        Assert.Equal(1, fn);
        Assert.Equal(2, wt);

        var result = reader.ReadMessage<GrpcResource>();
        Assert.Equal("test-ns", result.ResourceNamespace);
        Assert.Equal("test-topic", result.Name);
    }

    [Fact]
    [DisplayName("GrpcMessage_完整消息编码解码")]
    public void GrpcMessage_FullRoundTrip()
    {
        var msg = new GrpcMessage
        {
            Topic = new GrpcResource { ResourceNamespace = "ns", Name = "topic1" },
            SystemProperties = new GrpcSystemProperties
            {
                Tag = "tagA",
                MessageId = "msg-001",
                MessageType = GrpcMessageType.NORMAL,
                BornTimestamp = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc),
                BornHost = "localhost",
                QueueId = 3,
                QueueOffset = 12345,
            },
            Body = new Byte[] { 0x48, 0x65, 0x6C, 0x6C, 0x6F },
        };
        msg.UserProperties["user_key"] = "user_value";

        var writer = new ProtoWriter();
        msg.WriteTo(writer);
        var data = writer.ToArray();

        var reader = new ProtoReader(data);
        var result = new GrpcMessage();
        result.ReadFrom(reader);

        Assert.Equal("ns", result.Topic.ResourceNamespace);
        Assert.Equal("topic1", result.Topic.Name);
        Assert.Equal("tagA", result.SystemProperties.Tag);
        Assert.Equal("msg-001", result.SystemProperties.MessageId);
        Assert.Equal(GrpcMessageType.NORMAL, result.SystemProperties.MessageType);
        Assert.Equal("localhost", result.SystemProperties.BornHost);
        Assert.Equal(3, result.SystemProperties.QueueId);
        Assert.Equal(12345, result.SystemProperties.QueueOffset);
        Assert.Equal(new Byte[] { 0x48, 0x65, 0x6C, 0x6C, 0x6F }, result.Body);
        Assert.Equal("user_value", result.UserProperties["user_key"]);
    }
    #endregion

    #region GrpcClient帧编码
    [Fact]
    [DisplayName("gRPC帧编码_正确")]
    public void GrpcFrame_Encode()
    {
        var data = new Byte[] { 0x01, 0x02, 0x03 };
        var frame = GrpcClient.FrameEncode(data);

        Assert.Equal(8, frame.Length);
        Assert.Equal(0, frame[0]);  // 不压缩
        Assert.Equal(0, frame[1]);  // 长度高位
        Assert.Equal(0, frame[2]);
        Assert.Equal(0, frame[3]);
        Assert.Equal(3, frame[4]);  // 长度=3
        Assert.Equal(0x01, frame[5]);
        Assert.Equal(0x02, frame[6]);
        Assert.Equal(0x03, frame[7]);
    }

    [Fact]
    [DisplayName("gRPC帧解码_正确")]
    public void GrpcFrame_Decode()
    {
        var data = new Byte[] { 0x01, 0x02, 0x03 };
        var frame = GrpcClient.FrameEncode(data);
        var decoded = GrpcClient.FrameDecode(frame);

        Assert.Equal(data, decoded);
    }

    [Fact]
    [DisplayName("gRPC帧_空数据编解码")]
    public void GrpcFrame_EmptyData()
    {
        var frame = GrpcClient.FrameEncode(null);
        Assert.Equal(5, frame.Length);
        Assert.Equal(0, frame[4]); // 长度0

        var decoded = GrpcClient.FrameDecode(frame);
        Assert.Empty(decoded);
    }

    [Fact]
    [DisplayName("gRPC帧解码_数据不足返回空")]
    public void GrpcFrame_Decode_TooShort()
    {
        var decoded = GrpcClient.FrameDecode(new Byte[] { 0, 0, 0 });
        Assert.Empty(decoded);
    }
    #endregion

    #region 服务消息
    [Fact]
    [DisplayName("QueryRouteRequest_编码解码正确")]
    public void QueryRouteRequest_RoundTrip()
    {
        var request = new QueryRouteRequest
        {
            Topic = new GrpcResource { ResourceNamespace = "ns", Name = "test" },
            Endpoints = new GrpcEndpoints
            {
                Scheme = AddressScheme.IPv4,
                Addresses = [new GrpcAddress { Host = "127.0.0.1", Port = 8081 }],
            },
        };

        var writer = new ProtoWriter();
        request.WriteTo(writer);
        var data = writer.ToArray();

        var reader = new ProtoReader(data);
        var result = new QueryRouteRequest();
        result.ReadFrom(reader);

        Assert.Equal("ns", result.Topic.ResourceNamespace);
        Assert.Equal("test", result.Topic.Name);
        Assert.Equal(AddressScheme.IPv4, result.Endpoints.Scheme);
        Assert.Single(result.Endpoints.Addresses);
        Assert.Equal("127.0.0.1", result.Endpoints.Addresses[0].Host);
        Assert.Equal(8081, result.Endpoints.Addresses[0].Port);
    }

    [Fact]
    [DisplayName("SendMessageRequest_编码解码正确")]
    public void SendMessageRequest_RoundTrip()
    {
        var request = new SendMessageRequest();
        request.Messages.Add(new GrpcMessage
        {
            Topic = new GrpcResource { Name = "topic1" },
            Body = new Byte[] { 1, 2, 3 },
            SystemProperties = new GrpcSystemProperties
            {
                Tag = "tag1",
                MessageType = GrpcMessageType.NORMAL,
            },
        });

        var writer = new ProtoWriter();
        request.WriteTo(writer);
        var data = writer.ToArray();

        var reader = new ProtoReader(data);
        var result = new SendMessageRequest();
        result.ReadFrom(reader);

        Assert.Single(result.Messages);
        Assert.Equal("topic1", result.Messages[0].Topic.Name);
        Assert.Equal(new Byte[] { 1, 2, 3 }, result.Messages[0].Body);
        Assert.Equal("tag1", result.Messages[0].SystemProperties.Tag);
    }

    [Fact]
    [DisplayName("GrpcStatus_编码解码正确")]
    public void GrpcStatus_RoundTrip()
    {
        var status = new GrpcStatus
        {
            Code = GrpcCode.OK,
            Message = "Success",
        };

        var writer = new ProtoWriter();
        status.WriteTo(writer);
        var data = writer.ToArray();

        var reader = new ProtoReader(data);
        var result = new GrpcStatus();
        result.ReadFrom(reader);

        Assert.Equal(GrpcCode.OK, result.Code);
        Assert.Equal("Success", result.Message);
    }

    [Fact]
    [DisplayName("GrpcMessageQueue_含AcceptMessageTypes编解码")]
    public void GrpcMessageQueue_WithTypes_RoundTrip()
    {
        var mq = new GrpcMessageQueue
        {
            Topic = new GrpcResource { Name = "topic1" },
            Id = 2,
            Permission = GrpcPermission.READ_WRITE,
            Broker = new GrpcBroker
            {
                Name = "broker-a",
                Id = 0,
                Endpoints = new GrpcEndpoints
                {
                    Scheme = AddressScheme.IPv4,
                    Addresses = [new GrpcAddress { Host = "10.0.0.1", Port = 8081 }],
                },
            },
            AcceptMessageTypes = [GrpcMessageType.NORMAL, GrpcMessageType.DELAY],
        };

        var writer = new ProtoWriter();
        mq.WriteTo(writer);
        var data = writer.ToArray();

        var reader = new ProtoReader(data);
        var result = new GrpcMessageQueue();
        result.ReadFrom(reader);

        Assert.Equal("topic1", result.Topic.Name);
        Assert.Equal(2, result.Id);
        Assert.Equal(GrpcPermission.READ_WRITE, result.Permission);
        Assert.Equal("broker-a", result.Broker.Name);
        Assert.Equal(2, result.AcceptMessageTypes.Count);
        Assert.Contains(GrpcMessageType.NORMAL, result.AcceptMessageTypes);
        Assert.Contains(GrpcMessageType.DELAY, result.AcceptMessageTypes);
    }
    #endregion
}
