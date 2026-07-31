using System;
using System.ComponentModel;
using NewLife.Buffers;
using NewLife.RocketMQ.Grpc;
using NewLife.Serialization;
using Xunit;

namespace XUnitTest.Protocol;

/// <summary>
/// gRPC 协议 golden bytes 交叉测试（C2 你编我解）。
/// 使用 Apache 官方 proto（apache/rocketmq-apis）编码的字节流反喂本解码器，
/// 验证枚举值与字段号与官方协议一致，防止协议漂移。
/// </summary>
/// <remarks>
/// 官方 proto 关键约定（definition.proto）：
/// - 所有枚举以 XXX_UNSPECIFIED=0 起编，真实值从 1 开始
/// - SystemProperties.message_type = field 6，body_encoding = field 5，priority = field 22
/// - MessageQueue.permission = field 3，accept_message_types = field 5（packed）
/// - Endpoints.scheme = field 1，Digest.type = field 1
/// - EndTransactionRequest.source = field 4，resolution = field 5
/// - Settings.request_timeout = field 4，publishing = field 5，subscription = field 6
/// </remarks>
public class GrpcGoldenBytesTests
{
    #region 辅助
    /// <summary>用官方字节流解码指定消息</summary>
    private static T Decode<T>(params Byte[] data) where T : ISpanSerializable, new()
    {
        var reader = new SpanReader(data);
        var msg = new T();
        msg.Read(ref reader);
        return msg;
    }
    #endregion

    #region GrpcMessageType（官方 NORMAL=1/FIFO=2/DELAY=3/TRANSACTION=4，field 6，tag=0x30）
    [Theory]
    [InlineData(0x01, GrpcMessageType.NORMAL)]
    [InlineData(0x02, GrpcMessageType.FIFO)]
    [InlineData(0x03, GrpcMessageType.DELAY)]
    [InlineData(0x04, GrpcMessageType.TRANSACTION)]
    [DisplayName("MessageType_官方字节流_解析正确")]
    public void MessageType_OfficialBytes(Byte raw, GrpcMessageType expected)
    {
        var sys = Decode<GrpcSystemProperties>(0x30, raw);
        Assert.Equal(expected, sys.MessageType);
    }
    #endregion

    #region GrpcEncoding（官方 IDENTITY=1/GZIP=2，field 5，tag=0x28）
    [Theory]
    [InlineData(0x01, GrpcEncoding.IDENTITY)]
    [InlineData(0x02, GrpcEncoding.GZIP)]
    [DisplayName("BodyEncoding_官方字节流_解析正确")]
    public void BodyEncoding_OfficialBytes(Byte raw, GrpcEncoding expected)
    {
        var sys = Decode<GrpcSystemProperties>(0x28, raw);
        Assert.Equal(expected, sys.BodyEncoding);
    }
    #endregion

    #region Priority（官方 field 22，tag=0xB0）
    [Fact]
    [DisplayName("Priority_官方field22字节流_解析正确")]
    public void Priority_OfficialField22()
    {
        // field 22 varint tag=176，多字节编码 [0xB0,0x01]，值 5
        var sys = Decode<GrpcSystemProperties>(0xB0, 0x01, 0x05);
        Assert.Equal(5, sys.Priority);
    }

    [Fact]
    [DisplayName("Priority_旧field20字节流_不再误读")]
    public void Priority_OldField20_Ignored()
    {
        // 旧实现错误使用 field 20（tag=160，多字节编码 [0xA0,0x01]），官方 field 20 = dead_letter_queue（message 类型）
        var sys = Decode<GrpcSystemProperties>(0xA0, 0x01, 0x07);
        Assert.Equal(0, sys.Priority);
    }
    #endregion

    #region GrpcPermission（官方 NONE=1/READ=2/WRITE=3/READ_WRITE=4，field 3，tag=0x18）
    [Theory]
    [InlineData(0x01, GrpcPermission.NONE)]
    [InlineData(0x02, GrpcPermission.READ)]
    [InlineData(0x03, GrpcPermission.WRITE)]
    [InlineData(0x04, GrpcPermission.READ_WRITE)]
    [DisplayName("Permission_官方字节流_解析正确")]
    public void Permission_OfficialBytes(Byte raw, GrpcPermission expected)
    {
        var mq = Decode<GrpcMessageQueue>(0x18, raw);
        Assert.Equal(expected, mq.Permission);
    }
    #endregion

    #region AcceptMessageTypes（官方 packed 枚举，field 5，tag=0x2A）
    [Fact]
    [DisplayName("AcceptMessageTypes_官方packed字节流_解析正确")]
    public void AcceptMessageTypes_OfficialPackedBytes()
    {
        // field 5 packed：len=2，内容 [NORMAL=1, DELAY=3]
        var mq = Decode<GrpcMessageQueue>(0x2A, 0x02, 0x01, 0x03);
        Assert.Equal([GrpcMessageType.NORMAL, GrpcMessageType.DELAY], mq.AcceptMessageTypes);
    }
    #endregion

    #region AddressScheme（官方 IPv4=1/IPv6=2/DOMAIN_NAME=3，field 1，tag=0x08）
    [Theory]
    [InlineData(0x01, AddressScheme.IPv4)]
    [InlineData(0x02, AddressScheme.IPv6)]
    [InlineData(0x03, AddressScheme.DOMAIN_NAME)]
    [DisplayName("AddressScheme_官方字节流_解析正确")]
    public void AddressScheme_OfficialBytes(Byte raw, AddressScheme expected)
    {
        var ep = Decode<GrpcEndpoints>(0x08, raw);
        Assert.Equal(expected, ep.Scheme);
    }
    #endregion

    #region GrpcDigestType（官方 CRC32=1/MD5=2/SHA1=3，field 1，tag=0x08）
    [Theory]
    [InlineData(0x01, GrpcDigestType.CRC32)]
    [InlineData(0x02, GrpcDigestType.MD5)]
    [InlineData(0x03, GrpcDigestType.SHA1)]
    [DisplayName("DigestType_官方字节流_解析正确")]
    public void DigestType_OfficialBytes(Byte raw, GrpcDigestType expected)
    {
        var digest = Decode<GrpcDigest>(0x08, raw);
        Assert.Equal(expected, digest.Type);
    }
    #endregion

    #region 事务枚举（官方 SOURCE_CLIENT=1/SOURCE_SERVER_CHECK=2，COMMIT=1/ROLLBACK=2）
    [Theory]
    [InlineData(0x01, GrpcTransactionSource.SOURCE_CLIENT)]
    [InlineData(0x02, GrpcTransactionSource.SOURCE_SERVER_CHECK)]
    [DisplayName("TransactionSource_官方字节流_解析正确")]
    public void TransactionSource_OfficialBytes(Byte raw, GrpcTransactionSource expected)
    {
        // field 4，tag=0x20
        var req = Decode<GrpcEndTransactionRequest>(0x20, raw);
        Assert.Equal(expected, req.Source);
    }

    [Theory]
    [InlineData(0x01, GrpcTransactionResolution.COMMIT)]
    [InlineData(0x02, GrpcTransactionResolution.ROLLBACK)]
    [DisplayName("TransactionResolution_官方字节流_解析正确")]
    public void TransactionResolution_OfficialBytes(Byte raw, GrpcTransactionResolution expected)
    {
        // field 5，tag=0x28
        var req = Decode<GrpcEndTransactionRequest>(0x28, raw);
        Assert.Equal(expected, req.Resolution);
    }
    #endregion

    #region GrpcSettings（官方 request_timeout=field 4，publishing=field 5，subscription=field 6）
    [Fact]
    [DisplayName("RequestTimeout_官方field4字节流_解析正确")]
    public void RequestTimeout_OfficialField4()
    {
        // field 4 length-delimited（tag=0x22），Duration 内容 [seconds=10]
        var settings = Decode<GrpcSettings>(0x22, 0x02, 0x08, 0x0A);
        Assert.NotNull(settings.RequestTimeout);
        Assert.Equal(TimeSpan.FromSeconds(10), settings.RequestTimeout);
    }

    [Fact]
    [DisplayName("RequestTimeout_旧field3字节流_不再误读")]
    public void RequestTimeout_OldField3_Ignored()
    {
        // 旧实现错误使用 field 3（tag=0x1A），官方 field 3 = user_agent（string）
        var settings = Decode<GrpcSettings>(0x1A, 0x02, 0x08, 0x0A);
        Assert.Null(settings.RequestTimeout);
    }

    [Fact]
    [DisplayName("Publishing_官方field5字节流_解析正确")]
    public void Publishing_OfficialField5()
    {
        // field 5 length-delimited（tag=0x2A），空 Publishing 消息
        var settings = Decode<GrpcSettings>(0x2A, 0x00);
        Assert.NotNull(settings.Publishing);
    }

    [Fact]
    [DisplayName("Subscription_官方field6字节流_解析正确")]
    public void Subscription_OfficialField6()
    {
        // field 6 length-delimited（tag=0x32），空 Subscription 消息
        var settings = Decode<GrpcSettings>(0x32, 0x00);
        Assert.NotNull(settings.Subscription);
    }
    #endregion

    #region 写侧 golden bytes（我编你解）
    [Fact]
    [DisplayName("MessageType_本库编码_与官方字节一致")]
    public void MessageType_OurEncoding_MatchesOfficial()
    {
        var sys = new GrpcSystemProperties { MessageType = GrpcMessageType.FIFO };
        var buf = new Byte[64];
        var writer = new SpanWriter(buf);
        sys.Write(ref writer);
        var data = writer.WrittenSpan.ToArray();

        // 官方编码 NORMAL=1/FIFO=2：field 6 varint tag=0x30，值 2
        Assert.Contains(data, b => b == 0x30);
    }

    [Fact]
    [DisplayName("Priority_本库编码_field22与官方一致")]
    public void Priority_OurEncoding_UsesField22()
    {
        var sys = new GrpcSystemProperties { Priority = 9 };
        var buf = new Byte[64];
        var writer = new SpanWriter(buf);
        sys.Write(ref writer);
        var data = writer.WrittenSpan.ToArray();

        // field 22 varint tag = 22<<3 = 176 = 0xB0
        Assert.Contains(data, b => b == 0xB0);
        // 不应再出现旧 field 20 tag（0xA0）
        Assert.DoesNotContain(data, b => b == 0xA0);
    }
    #endregion
}
