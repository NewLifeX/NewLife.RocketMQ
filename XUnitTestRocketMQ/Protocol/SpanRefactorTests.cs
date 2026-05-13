using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Text;
using NewLife;
using NewLife.Buffers;
using NewLife.Data;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Grpc;
using NewLife.RocketMQ.Protocol;
using Xunit;

namespace XUnitTestRocketMQ;

/// <summary>SpanReader/SpanWriter重构验证测试</summary>
/// <remarks>
/// 验证 Command、MessageExt、ProtoExtensions 使用 SpanReader/SpanWriter 重构后的正确性。
/// 包含：二进制协议往返测试、边界条件测试、跨框架兼容性测试。
/// </remarks>
[DisplayName("SpanReader/SpanWriter重构测试")]
public class SpanRefactorTests
{
    #region Command 二进制协议测试
    [Fact]
    [DisplayName("Command_RocketMQ二进制格式_写入后读取一致")]
    public void Command_RocketMQ_WriteRead_RoundTrip()
    {
        // 构造一个Command，使用ROCKETMQ序列化写入后再读取
        var cmd = new Command
        {
            Header = new Header
            {
                Code = 34,
                Flag = 0,
                Language = "DOTNET",
                Version = MQVersion.V4_8_0,
                Opaque = 100,
                SerializeTypeCurrentRPC = "ROCKETMQ",
                Remark = "Test",
            }
        };
        cmd.Header.GetExtFields()["key1"] = "value1";
        cmd.Header.GetExtFields()["key2"] = "value2";

        // 写入到流
        var ms = new MemoryStream();
        cmd.Write(ms, null);
        ms.Position = 0;

        // 读取
        var cmd2 = new Command();
        var ok = cmd2.Read(ms);

        Assert.True(ok);
        Assert.Equal(cmd.Header.Code, cmd2.Header.Code);
        Assert.Equal(cmd.Header.Flag, cmd2.Header.Flag);
        Assert.Equal(cmd.Header.Language, cmd2.Header.Language);
        Assert.Equal(cmd.Header.Version, cmd2.Header.Version);
        Assert.Equal(cmd.Header.Opaque, cmd2.Header.Opaque);
        Assert.Equal(cmd.Header.Remark, cmd2.Header.Remark);

        var ext = cmd2.Header.GetExtFields();
        Assert.Equal(2, ext.Count);
        Assert.Equal("value1", ext["key1"]);
        Assert.Equal("value2", ext["key2"]);
    }

    [Fact]
    [DisplayName("Command_RocketMQ二进制格式_无备注无扩展字段")]
    public void Command_RocketMQ_NoRemarkNoExt_RoundTrip()
    {
        var cmd = new Command
        {
            Header = new Header
            {
                Code = 0,
                Flag = 1,
                Language = "JAVA",
                Version = MQVersion.V5_2_0,
                Opaque = 0,
                SerializeTypeCurrentRPC = "ROCKETMQ",
            }
        };

        var ms = new MemoryStream();
        cmd.Write(ms, null);
        ms.Position = 0;

        var cmd2 = new Command();
        var ok = cmd2.Read(ms);

        Assert.True(ok);
        Assert.Equal(0, cmd2.Header.Code);
        Assert.Equal(1, cmd2.Header.Flag);
        Assert.Null(cmd2.Header.Remark);

        var ext = cmd2.Header.GetExtFields();
        Assert.Empty(ext);
    }

    [Fact]
    [DisplayName("Command_带Body的消息_写入读取一致")]
    public void Command_WithPayload_RoundTrip()
    {
        var body = Encoding.UTF8.GetBytes("{\"test\":\"hello\"}");
        var cmd = new Command
        {
            Header = new Header
            {
                Code = 310,
                Flag = 0,
                Language = "DOTNET",
                Version = MQVersion.V4_8_0,
                Opaque = 1,
                SerializeTypeCurrentRPC = "ROCKETMQ",
                Remark = "SEND",
            },
            Payload = new ArrayPacket(body),
        };

        var ms = new MemoryStream();
        cmd.Write(ms, null);
        ms.Position = 0;

        var cmd2 = new Command();
        var ok = cmd2.Read(ms);

        Assert.True(ok);
        Assert.Equal(310, cmd2.Header.Code);
        var pk = cmd2.Payload;
        Assert.NotNull(pk);
        Assert.Equal("{\"test\":\"hello\"}", pk.ToStr());
    }

    [Fact]
    [DisplayName("Command_中文备注_SpanWriter编码正确")]
    public void Command_ChineseRemark_RoundTrip()
    {
        var cmd = new Command
        {
            Header = new Header
            {
                Code = 100,
                Flag = 0,
                Language = "DOTNET",
                Version = MQVersion.V4_8_0,
                Opaque = 5,
                SerializeTypeCurrentRPC = "ROCKETMQ",
                Remark = "测试备注",
            }
        };

        var ms = new MemoryStream();
        cmd.Write(ms, null);
        ms.Position = 0;

        var cmd2 = new Command();
        var ok = cmd2.Read(ms);

        Assert.True(ok);
        Assert.Equal("测试备注", cmd2.Header.Remark);
    }
    #endregion

    #region MessageExt SpanReader解码测试
    [Fact]
    [DisplayName("MessageExt_5x消息ID_SpanWriter创建SpanReader解析往返")]
    public void MessageExt_5xId_CreateParse_RoundTrip()
    {
        var mac = new Byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06 };
        var pid = 54321;
        var counter = 999999;

        var id = MessageExt.CreateMessageId5x(2, mac, pid, counter);

        Assert.NotNull(id);
        Assert.Equal(32, id.Length);
        Assert.StartsWith("01", id);

        var ok = MessageExt.TryParseMessageId5x(id, out var ver, out var parsedMac, out var parsedPid, out var parsedCounter);
        Assert.True(ok);
        Assert.Equal(2, ver);
        Assert.Equal(mac, parsedMac);
        Assert.Equal(pid, parsedPid);
        Assert.Equal(counter, parsedCounter);
    }

    [Fact]
    [DisplayName("MessageExt_5xID_空MAC使用随机字节")]
    public void MessageExt_5xId_NullMac_UsesRandom()
    {
        var id1 = MessageExt.CreateMessageId5x(1, null, 100, 200);
        var id2 = MessageExt.CreateMessageId5x(1, null, 100, 200);

        // 两次生成应不同（随机MAC）
        Assert.NotNull(id1);
        Assert.NotNull(id2);
        Assert.Equal(32, id1.Length);
        Assert.Equal(32, id2.Length);

        // 前缀和版本相同
        Assert.StartsWith("0101", id1);
        Assert.StartsWith("0101", id2);
        // MAC部分不同（极小概率相同）
        Assert.NotEqual(id1, id2);
    }

    [Fact]
    [DisplayName("MessageExt_IsMessageId5x_正确识别5x格式")]
    public void MessageExt_IsMessageId5x_CorrectDetection()
    {
        var mac = new Byte[] { 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF };
        var id5x = MessageExt.CreateMessageId5x(1, mac, 1000, 2000);
        Assert.True(MessageExt.IsMessageId5x(id5x));

        // 4.x格式（前缀不是01）
        Assert.False(MessageExt.IsMessageId5x("AABBCCDD00001111000000000000FFFF"));
        // 长度不对
        Assert.False(MessageExt.IsMessageId5x("0101AABB"));
        // null
        Assert.False(MessageExt.IsMessageId5x(null));
    }
    #endregion

    #region ProtoExtensions SpanReader/SpanWriter重构测试
    [Fact]
    [DisplayName("SpanWriter_Fixed32_使用扩展方法正确编解码")]
    public void SpanWriter_Fixed32_ExtensionMethod()
    {
        var buf = new Byte[128];
        var writer = new SpanWriter(buf);
        writer.WriteFixed32(1, 0x12345678);
        writer.WriteFixed32(2, UInt32.MaxValue);
        writer.WriteFixed32(3, 1);

        var data = writer.WrittenSpan.ToArray();
        var reader = new SpanReader(data);

        var (fn1, wt1) = reader.ReadTag();
        Assert.Equal(1, fn1);
        Assert.Equal(5, wt1); // wireType 5 = 32-bit
        Assert.Equal(0x12345678U, reader.ReadFixed32());

        var (fn2, wt2) = reader.ReadTag();
        Assert.Equal(2, fn2);
        Assert.Equal(UInt32.MaxValue, reader.ReadFixed32());

        var (fn3, _) = reader.ReadTag();
        Assert.Equal(3, fn3);
        Assert.Equal(1U, reader.ReadFixed32());

        Assert.True(reader.Available <= 0);
    }

    [Fact]
    [DisplayName("SpanWriter_Fixed64_使用扩展方法正确编解码")]
    public void SpanWriter_Fixed64_ExtensionMethod()
    {
        var buf = new Byte[128];
        var writer = new SpanWriter(buf);
        writer.WriteFixed64(1, 0x123456789ABCDEF0);

        var data = writer.WrittenSpan.ToArray();
        var reader = new SpanReader(data);

        var (fn1, wt1) = reader.ReadTag();
        Assert.Equal(1, fn1);
        Assert.Equal(1, wt1); // wireType 1 = 64-bit
        Assert.Equal(0x123456789ABCDEF0UL, reader.ReadFixed64());
        Assert.True(reader.Available <= 0);
    }

    [Fact]
    [DisplayName("SpanWriter_Float_编码解码正确")]
    public void SpanWriter_Float_RoundTrip()
    {
        var buf = new Byte[128];
        var writer = new SpanWriter(buf);
        writer.WriteFloat(1, 3.14f);
        writer.WriteFloat(2, -1.5f);
        writer.WriteFloat(3, Single.MaxValue);
        writer.WriteFloat(4, Single.MinValue);

        var data = writer.WrittenSpan.ToArray();
        var reader = new SpanReader(data);

        var (fn1, _) = reader.ReadTag();
        Assert.Equal(1, fn1);
        Assert.Equal(3.14f, reader.ReadFloat());

        var (fn2, _) = reader.ReadTag();
        Assert.Equal(2, fn2);
        Assert.Equal(-1.5f, reader.ReadFloat());

        var (fn3, _) = reader.ReadTag();
        Assert.Equal(3, fn3);
        Assert.Equal(Single.MaxValue, reader.ReadFloat());

        var (fn4, _) = reader.ReadTag();
        Assert.Equal(4, fn4);
        Assert.Equal(Single.MinValue, reader.ReadFloat());

        Assert.True(reader.Available <= 0);
    }

    [Fact]
    [DisplayName("SpanWriter_Double_编码解码正确")]
    public void SpanWriter_Double_RoundTrip()
    {
        var buf = new Byte[128];
        var writer = new SpanWriter(buf);
        writer.WriteDouble(1, 3.141592653589793);
        writer.WriteDouble(2, Double.MaxValue);
        writer.WriteDouble(3, Double.Epsilon);

        var data = writer.WrittenSpan.ToArray();
        var reader = new SpanReader(data);

        var (fn1, _) = reader.ReadTag();
        Assert.Equal(1, fn1);
        Assert.Equal(3.141592653589793, reader.ReadProtoDouble());

        var (fn2, _) = reader.ReadTag();
        Assert.Equal(2, fn2);
        Assert.Equal(Double.MaxValue, reader.ReadProtoDouble());

        var (fn3, _) = reader.ReadTag();
        Assert.Equal(3, fn3);
        Assert.Equal(Double.Epsilon, reader.ReadProtoDouble());

        Assert.True(reader.Available <= 0);
    }

    [Fact]
    [DisplayName("SpanWriter_Timestamp_编码解码正确")]
    public void SpanWriter_Timestamp_RoundTrip()
    {
        var buf = new Byte[128];
        var writer = new SpanWriter(buf);
        var time = new DateTime(2025, 6, 15, 12, 30, 45, DateTimeKind.Utc);
        writer.WriteTimestamp(1, time);

        var data = writer.WrittenSpan.ToArray();
        var reader = new SpanReader(data);

        var (fn1, _) = reader.ReadTag();
        Assert.Equal(1, fn1);
        var parsed = reader.ReadTimestamp();

        Assert.Equal(time.Year, parsed.Year);
        Assert.Equal(time.Month, parsed.Month);
        Assert.Equal(time.Day, parsed.Day);
        Assert.Equal(time.Hour, parsed.Hour);
        Assert.Equal(time.Minute, parsed.Minute);
        Assert.Equal(time.Second, parsed.Second);
    }

    [Fact]
    [DisplayName("SpanWriter_Duration_编码解码正确")]
    public void SpanWriter_Duration_RoundTrip()
    {
        var buf = new Byte[128];
        var writer = new SpanWriter(buf);
        var duration = TimeSpan.FromSeconds(3600) + TimeSpan.FromMilliseconds(500);
        writer.WriteDuration(1, duration);

        var data = writer.WrittenSpan.ToArray();
        var reader = new SpanReader(data);

        var (fn1, _) = reader.ReadTag();
        Assert.Equal(1, fn1);
        var parsed = reader.ReadDuration();

        Assert.Equal(3600, (Int32)parsed.TotalSeconds);
        Assert.Equal(500, parsed.Milliseconds);
    }

    [Fact]
    [DisplayName("SpanWriter_嵌套消息_子缓冲区编解码")]
    public void SpanWriter_NestedMessage_SubBuffer_RoundTrip()
    {
        var resource = new GrpcResource
        {
            Name = "test_topic",
            ResourceNamespace = "ns1",
        };

        var buf = new Byte[256];
        var writer = new SpanWriter(buf);
        writer.WriteMessage(1, resource);

        var data = writer.WrittenSpan.ToArray();
        var reader = new SpanReader(data);

        var (fn1, wt1) = reader.ReadTag();
        Assert.Equal(1, fn1);
        Assert.Equal(2, wt1); // length-delimited

        var parsed = reader.ReadProtoMessage<GrpcResource>();
        Assert.Equal("test_topic", parsed.Name);
        Assert.Equal("ns1", parsed.ResourceNamespace);
    }

    [Fact]
    [DisplayName("SpanWriter_Map字段_多个entry编解码")]
    public void SpanWriter_Map_MultiEntry_RoundTrip()
    {
        var map = new Dictionary<String, String>
        {
            ["host"] = "10.0.0.1",
            ["port"] = "8080",
            ["env"] = "production",
        };

        var buf = new Byte[512];
        var writer = new SpanWriter(buf);
        writer.WriteMap(1, map);

        var data = writer.WrittenSpan.ToArray();
        var reader = new SpanReader(data);

        var result = new Dictionary<String, String>();
        while (reader.Available > 0)
        {
            var (fn, wt) = reader.ReadTag();
            if (fn == 0) break;
            Assert.Equal(1, fn);
            Assert.Equal(2, wt);
            var (k, v) = reader.ReadMapEntry();
            result[k] = v;
        }

        Assert.Equal(3, result.Count);
        Assert.Equal("10.0.0.1", result["host"]);
        Assert.Equal("8080", result["port"]);
        Assert.Equal("production", result["env"]);
    }

    [Fact]
    [DisplayName("SpanWriter_RepeatedString_多值编解码")]
    public void SpanWriter_RepeatedString_RoundTrip()
    {
        var values = new List<String> { "alpha", "beta", "gamma" };

        var buf = new Byte[256];
        var writer = new SpanWriter(buf);
        writer.WriteRepeatedString(1, values);

        var data = writer.WrittenSpan.ToArray();
        var reader = new SpanReader(data);

        var result = new List<String>();
        while (reader.Available > 0)
        {
            var (fn, _) = reader.ReadTag();
            if (fn == 0) break;
            Assert.Equal(1, fn);
            result.Add(reader.ReadProtoString());
        }

        Assert.Equal(values, result);
    }
    #endregion

    #region 边界测试
    [Fact]
    [DisplayName("SpanReader_读取超出边界_抛出异常")]
    public void SpanReader_ReadBeyondLimit_ThrowsException()
    {
        var data = new Byte[] { 0x01, 0x02 };
        var reader = new SpanReader(data);

        // 读取2字节OK
        reader.ReadBytes(2);

        // 再读1字节应失败
        var thrown = false;
        try
        {
            reader.ReadBytes(1);
        }
        catch
        {
            thrown = true;
        }
        Assert.True(thrown);
    }

    [Fact]
    [DisplayName("SpanReader_空数据_Available为0")]
    public void SpanReader_EmptyData_AvailableZero()
    {
        var reader = new SpanReader([]);
        Assert.True(reader.Available <= 0);
    }

    [Fact]
    [DisplayName("ProtoExtensions_Serialize_空消息返回空")]
    public void ProtoExtensions_Serialize_NullReturnsEmpty()
    {
        var data = ProtoExtensions.Serialize(null);
        Assert.Empty(data);
    }

    [Fact]
    [DisplayName("SpanWriter_Int32负数_10字节varint编码")]
    public void SpanWriter_NegativeInt32_10ByteVarint()
    {
        var buf = new Byte[64];
        var writer = new SpanWriter(buf);
        writer.WriteInt32(1, -1);

        var data = writer.WrittenSpan.ToArray();
        var reader = new SpanReader(data);

        var (fn, _) = reader.ReadTag();
        Assert.Equal(1, fn);
        Assert.Equal(-1, reader.ReadProtoInt32());
        Assert.True(reader.Available <= 0);
    }

    [Fact]
    [DisplayName("SpanWriter_SInt64_ZigZag边界值")]
    public void SpanWriter_SInt64_ZigZag_BoundaryValues()
    {
        var buf = new Byte[128];
        var writer = new SpanWriter(buf);
        writer.WriteSInt64(1, Int64.MinValue);
        writer.WriteSInt64(2, Int64.MaxValue);
        writer.WriteSInt64(3, -1);
        writer.WriteSInt64(4, 1);

        var data = writer.WrittenSpan.ToArray();
        var reader = new SpanReader(data);

        var (fn1, _) = reader.ReadTag();
        Assert.Equal(1, fn1);
        Assert.Equal(Int64.MinValue, reader.ReadSInt64());

        var (fn2, _) = reader.ReadTag();
        Assert.Equal(2, fn2);
        Assert.Equal(Int64.MaxValue, reader.ReadSInt64());

        var (fn3, _) = reader.ReadTag();
        Assert.Equal(3, fn3);
        Assert.Equal(-1L, reader.ReadSInt64());

        var (fn4, _) = reader.ReadTag();
        Assert.Equal(4, fn4);
        Assert.Equal(1L, reader.ReadSInt64());

        Assert.True(reader.Available <= 0);
    }

    [Fact]
    [DisplayName("SpanReader_SkipField_未知wireType抛异常")]
    public void SpanReader_SkipField_UnknownWireType_Throws()
    {
        var reader = new SpanReader(new Byte[] { 0x08, 0x01 }); // field 1, varint, value 1
        reader.ReadTag();
        var thrown = false;
        try
        {
            reader.SkipField(3); // wireType 3 is deprecated/unknown
        }
        catch (InvalidDataException)
        {
            thrown = true;
        }
        Assert.True(thrown);
    }
    #endregion

    #region 综合场景测试
    [Fact]
    [DisplayName("SpanWriter_各种字段类型_完整编解码")]
    public void SpanWriter_CompleteMessage_RoundTrip()
    {
        var buf = new Byte[256];
        var writer = new SpanWriter(buf);
        // 写入各种类型的字段
        writer.WriteString(1, "TAG_test");   // tag
        writer.WriteString(2, "key_123");    // keys
        writer.WriteString(3, "msg_001");    // message_id
        writer.WriteString(4, "body_crc");   // body_digest.checksum
        writer.WriteEnum(5, (Int32)GrpcMessageType.NORMAL);
        writer.WriteTimestamp(6, new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc));
        writer.WriteInt32(7, 3);             // born_host.port  
        writer.WriteInt32(8, 0);             // queue_id

        var data = writer.WrittenSpan.ToArray();
        Assert.True(data.Length > 0);

        // 确认能正确读取
        var reader = new SpanReader(data);
        while (reader.Available > 0)
        {
            var (fn, wt) = reader.ReadTag();
            if (fn == 0) break;
            reader.SkipField(wt);
        }
    }

    [Fact]
    [DisplayName("SpanWriter_PackedEnum_编解码")]
    public void SpanWriter_PackedEnum_RoundTrip()
    {
        var enums = new List<Int32> { 1, 2, 4, 8, 16 };

        var buf = new Byte[128];
        var writer = new SpanWriter(buf);
        writer.WritePackedEnum(1, enums);

        var data = writer.WrittenSpan.ToArray();
        var reader = new SpanReader(data);

        var (fn, wt) = reader.ReadTag();
        Assert.Equal(1, fn);
        Assert.Equal(2, wt); // packed = length-delimited

        // 读取packed数据
        var packedLen = (Int32)reader.ReadRawVarint();
        var packedData = reader.ReadBytes(packedLen).ToArray();
        var subReader = new SpanReader(packedData);

        var result = new List<Int32>();
        while (subReader.Available > 0)
        {
            result.Add((Int32)subReader.ReadRawVarint());
        }

        Assert.Equal(enums, result);
    }

    [Fact]
    [DisplayName("SpanWriter_大数据量写入_Serialize正确处理")]
    public void SpanWriter_LargeData_SerializeHandles()
    {
        var resource = new GrpcResource
        {
            ResourceNamespace = new String('N', 500),
            Name = new String('T', 500),
        };

        var data = ProtoExtensions.Serialize(resource);
        Assert.True(data.Length > 1000);

        var reader = new SpanReader(data);
        var result = new GrpcResource();
        result.Read(ref reader);

        Assert.Equal(resource.ResourceNamespace, result.ResourceNamespace);
        Assert.Equal(resource.Name, result.Name);
    }

    [Fact]
    [DisplayName("SpanReader_SkipField_各线路类型正确跳过")]
    public void SpanReader_SkipField_AllWireTypes()
    {
        var buf = new Byte[256];
        var writer = new SpanWriter(buf);
        writer.WriteInt32(1, 42);         // varint (wireType 0)
        writer.WriteFixed64(2, 100);      // 64-bit (wireType 1)
        writer.WriteString(3, "skip_me"); // length-delimited (wireType 2)
        writer.WriteFixed32(4, 200);      // 32-bit (wireType 5)
        writer.WriteInt32(5, 999);        // 这是目标字段

        var data = writer.WrittenSpan.ToArray();
        var reader = new SpanReader(data);

        // 跳过字段1-4
        for (var i = 0; i < 4; i++)
        {
            var (_, wt) = reader.ReadTag();
            reader.SkipField(wt);
        }

        // 读取字段5
        var (fn5, _) = reader.ReadTag();
        Assert.Equal(5, fn5);
        Assert.Equal(999, reader.ReadProtoInt32());

        Assert.True(reader.Available <= 0);
    }

    [Fact]
    [DisplayName("Command_JSON格式_不受SpanWriter影响")]
    public void Command_JSON_NotAffected_ByRefactoring()
    {
        // JSON格式应继续使用JSON序列化，不受二进制重构影响
        var cmd = new Command
        {
            Header = new Header
            {
                Code = 105,
                Flag = 0,
                Language = "JAVA",
                Version = MQVersion.V5_2_0,
                Opaque = 0,
                SerializeTypeCurrentRPC = "JSON",
            }
        };
        cmd.Header.GetExtFields()["topic"] = "TestTopic";

        var ms = new MemoryStream();
        cmd.Write(ms, null);
        ms.Position = 0;

        var cmd2 = new Command();
        var ok = cmd2.Read(ms);

        Assert.True(ok);
        Assert.Equal(105, cmd2.Header.Code);

        var ext = cmd2.Header.GetExtFields();
        Assert.Equal("TestTopic", ext["topic"]);
    }
    #endregion
}
