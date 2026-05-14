using System;
using System.ComponentModel;
using NewLife;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Protocol;
using Xunit;

namespace XUnitTest.Producers;

/// <summary>消息压缩功能测试</summary>
public class CompressionTests
{
    [Fact]
    [DisplayName("压缩阈值_默认值为4096")]
    public void CompressOverBytes_DefaultValue()
    {
        using var producer = new Producer();
        Assert.Equal(4096, producer.CompressOverBytes);
    }

    [Fact]
    [DisplayName("压缩阈值_可自定义设置")]
    public void CompressOverBytes_CanBeSet()
    {
        using var producer = new Producer
        {
            CompressOverBytes = 1024,
        };
        Assert.Equal(1024, producer.CompressOverBytes);
    }

    [Fact]
    [DisplayName("压缩阈值_设为0禁用压缩")]
    public void CompressOverBytes_ZeroDisablesCompression()
    {
        using var producer = new Producer
        {
            CompressOverBytes = 0,
        };
        Assert.Equal(0, producer.CompressOverBytes);
    }

    // F052: IMessageCompressor 接口与 MessageCompressorRegistry 注册表测试

    [Fact]
    [DisplayName("MessageCompressorRegistry_类型0已注册ZLIB")]
    public void Registry_Type0_IsZlib()
    {
        var c = MessageCompressorRegistry.Get(0);
        Assert.NotNull(c);
        Assert.IsType<ZlibMessageCompressor>(c);
    }

    [Fact]
    [DisplayName("MessageCompressorRegistry_类型3已注册ZLIB别名")]
    public void Registry_Type3_IsZlibAlias()
    {
        var c = MessageCompressorRegistry.Get(3);
        Assert.NotNull(c);
        Assert.IsType<ZlibMessageCompressor>(c);
    }

    [Fact]
    [DisplayName("MessageCompressorRegistry_未注册类型返回null")]
    public void Registry_UnknownType_ReturnsNull()
    {
        var c = MessageCompressorRegistry.Get(99);
        Assert.Null(c);
    }

    [Fact]
    [DisplayName("MessageCompressorRegistry_GetOrThrow未注册类型抛出NotSupportedException")]
    public void Registry_GetOrThrow_UnknownType_Throws()
    {
        Assert.Throws<NotSupportedException>(() => MessageCompressorRegistry.GetOrThrow(1));
    }

    [Fact]
    [DisplayName("MessageCompressorRegistry_GetOrThrow_LZ4提示安装扩展包")]
    public void Registry_GetOrThrow_Lz4_HintInMessage()
    {
        var ex = Assert.Throws<NotSupportedException>(() => MessageCompressorRegistry.GetOrThrow(1));
        Assert.Contains("LZ4", ex.Message);
    }

    [Fact]
    [DisplayName("MessageCompressorRegistry_GetOrThrow_ZSTD提示安装扩展包")]
    public void Registry_GetOrThrow_Zstd_HintInMessage()
    {
        var ex = Assert.Throws<NotSupportedException>(() => MessageCompressorRegistry.GetOrThrow(2));
        Assert.Contains("ZSTD", ex.Message);
    }

    [Fact]
    [DisplayName("MessageCompressorRegistry_可注册自定义压缩器")]
    public void Registry_Register_CustomCompressor()
    {
        var fake = new FakeCompressor();
        MessageCompressorRegistry.Register(7, fake);
        var result = MessageCompressorRegistry.Get(7);
        Assert.Same(fake, result);
        // 恢复：注销自定义注册（注册 null 会抛出，直接覆盖为 zlib 避免污染其他测试）
        MessageCompressorRegistry.Register(7, new ZlibMessageCompressor());
    }

    [Fact]
    [DisplayName("ZlibMessageCompressor_压缩解压往返正确")]
    public void Zlib_CompressDecompress_RoundTrip()
    {
        var original = new Byte[5000];
        for (var i = 0; i < original.Length; i++) original[i] = (Byte)(i % 128);

        var compressor = new ZlibMessageCompressor();
        var compressed = compressor.Compress(original);

        Assert.NotNull(compressed);
        Assert.True(compressed.Length < original.Length); // 重复数据应能被压缩

        var decompressed = compressor.Decompress(compressed);
        Assert.Equal(original, decompressed);
    }

    [Fact]
    [DisplayName("ZlibMessageCompressor_输出含RFC1950_ZLIB头部")]
    public void Zlib_Compress_HasZlibHeader()
    {
        var compressor = new ZlibMessageCompressor();
        var data = new Byte[100];
        var compressed = compressor.Compress(data);

        // RFC1950 ZLIB头：CMF=0x78（CM=8,CINFO=7）, FLG=0x9C 或类似校验字节
        Assert.Equal(0x78, compressed[0]);
    }

    [Fact]
    [DisplayName("ZlibMessageCompressor_空数据不抛出")]
    public void Zlib_Compress_EmptyData()
    {
        var compressor = new ZlibMessageCompressor();
        var result = compressor.Compress(new Byte[0]);
        Assert.NotNull(result);
    }

    [Fact]
    [DisplayName("ZlibMessageCompressor_解压null不抛出")]
    public void Zlib_Decompress_Null()
    {
        var compressor = new ZlibMessageCompressor();
        var result = compressor.Decompress(null);
        Assert.Null(result);
    }

    [Fact]
    [DisplayName("ZlibMessageCompressor_兼容RawDeflate格式")]
    public void Zlib_Decompress_RawDeflate()
    {
        // 使用 NewLife.Core 的 .Compress() 产出 RAW DEFLATE（无 ZLIB 头），测试兼容性
        var original = System.Text.Encoding.UTF8.GetBytes("Hello RocketMQ LZ4 ZSTD Compress");
        var rawDeflated = original.Compress();  // NewLife.Core DeflateStream 输出 RAW DEFLATE

        var compressor = new ZlibMessageCompressor();
        var decompressed = compressor.Decompress(rawDeflated);

        Assert.Equal(original, decompressed);
    }

    private class FakeCompressor : IMessageCompressor
    {
        public Byte[] Compress(Byte[] data) => data;
        public Byte[] Decompress(Byte[] data) => data;
    }
}
