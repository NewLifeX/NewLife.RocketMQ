using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using NewLife.Buffers;
using NewLife.RocketMQ.Grpc;
using NewLife.Serialization;
using Xunit;

namespace XUnitTest.Protocol;

/// <summary>gRPC 健壮性测试：WriteMap 动态扩容、读路径边界校验、帧边界</summary>
public class GrpcRobustnessTests
{
    [Fact]
    [DisplayName("WriteMap_长值_动态扩容正确编解码")]
    public void WriteMap_LongValue_DynamicExpand()
    {
        // 值超过固定 512 字节缓冲，旧实现会溢出失败
        var map = new Dictionary<String, String>
        {
            ["key"] = new String('x', 1000),
            [new String('k', 300)] = "value",
        };

        var msg = new GrpcMessage
        {
            Topic = new GrpcResource { Name = "t" },
            SystemProperties = new GrpcSystemProperties { Tag = "a" },
            Body = [0x01],
        };
        foreach (var kv in map) msg.UserProperties[kv.Key] = kv.Value;

        var data = ProtoExtensions.Serialize(msg);

        // 解码回读，验证长 key/value 完整保留
        var reader = new SpanReader(data);
        var result = new GrpcMessage();
        result.Read(ref reader);

        Assert.Equal("t", result.Topic.Name);
        Assert.Equal(1000, result.UserProperties["key"].Length);
        Assert.Equal("value", result.UserProperties[new String('k', 300)]);
    }

    [Fact]
    [DisplayName("ReadProtoString_长度超过剩余_抛异常")]
    public void ReadProtoString_TooLong_Throws()
    {
        // 声明长度 100，但只有 1 字节数据
        var data = new Byte[] { 0x64, 0x41 }; // len=100, 1字节 'A'
        var reader = new SpanReader(data);

        // SpanReader 是 ref struct，不能用于 lambda，手动 try-catch 断言
        var threw = false;
        try
        {
            reader.ReadProtoString();
        }
        catch (EndOfStreamException)
        {
            threw = true;
        }
        Assert.True(threw);
    }

    [Fact]
    [DisplayName("ReadProtoMessage_长度超过剩余_抛异常")]
    public void ReadProtoMessage_TooLong_Throws()
    {
        var data = new Byte[] { 0x64, 0x41 }; // len=100, 1字节数据
        var reader = new SpanReader(data);

        var threw = false;
        try
        {
            reader.ReadProtoMessage<GrpcResource>();
        }
        catch (EndOfStreamException)
        {
            threw = true;
        }
        Assert.True(threw);
    }

    [Fact]
    [DisplayName("FrameDecode_帧长符号扩展_安全返回空")]
    public void FrameDecode_HugeLength_Safe()
    {
        // 4 字节长度 = 0x80000000（>= 2^31），旧实现符号扩展为负数被误判
        var frame = new Byte[] { 0x00, 0x80, 0x00, 0x00, 0x00, 0x01 };

        var result = GrpcClient.FrameDecode(frame);
        Assert.Empty(result);
    }

    [Fact]
    [DisplayName("FrameDecode_正常帧_解码正确")]
    public void FrameDecode_NormalFrame_Decodes()
    {
        var body = new Byte[] { 0x01, 0x02, 0x03 };
        var frame = GrpcClient.FrameEncode(body);

        var result = GrpcClient.FrameDecode(frame);
        Assert.Equal(body, result);
    }
}
