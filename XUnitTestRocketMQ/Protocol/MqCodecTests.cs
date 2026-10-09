using System;
using System.Buffers;
using System.ComponentModel;
using System.Threading.Tasks;
using NewLife;
using NewLife.Data;
using NewLife.Messaging;
using NewLife.RocketMQ.Protocol;
using Xunit;

namespace XUnitTest.Protocol;

[DisplayName("MqCodec 帧编解码")]
public class MqCodecTests
{
    private static Command CreateCommand()
    {
        var cmd = new Command
        {
            Header = new Header
            {
                Code = (Int32)RequestCode.SEND_MESSAGE_V2,
                Opaque = 1234,
                Language = LanguageCode.DOTNET + "",
                Version = MQVersion.V4_8_0,
                Remark = "TEST",
            },
        };
        cmd.SetBody(new ArrayPacket("hello rocketmq".GetBytes()));

        return cmd;
    }

    [Fact]
    [DisplayName("Build后TryParse_完整帧_字段与体长正确")]
    public void RoundTrip_JsonFrame()
    {
        var codec = new MqCodec();
        var cmd = CreateCommand();

        var frame = codec.Build(cmd);
        Assert.NotNull(frame);
        Assert.True(frame.Total > 8);

        var rs = codec.TryParse(new ReadOnlySequence<Byte>(frame.ToArray()));
        Assert.True(rs.HasValue);

        var parsed = Assert.IsType<Command>(rs.Value.Message);
        Assert.Equal((Int32)RequestCode.SEND_MESSAGE_V2, parsed.Header.Code);
        Assert.Equal(1234, parsed.Header.Opaque);
        Assert.Equal("TEST", parsed.Header.Remark);
        Assert.False(parsed.Reply);

        // 主体由帧层绑定，编解码器不消费
        Assert.Null(parsed.Payload);
        Assert.Equal("hello rocketmq".GetBytes().Length, rs.Value.BodyLength);
        Assert.Equal(frame.Total, rs.Value.HeaderSize + rs.Value.BodyLength);
    }

    [Fact]
    [DisplayName("TryParse_半包_返回null")]
    public void TryParse_PartialFrame_ReturnsNull()
    {
        var codec = new MqCodec();
        var bytes = codec.Build(CreateCommand()).ToArray();

        // 帧头未到齐
        Assert.Null(codec.TryParse(new ReadOnlySequence<Byte>(bytes, 0, 4)));
        // 帧头到齐但头部数据未到齐
        Assert.Null(codec.TryParse(new ReadOnlySequence<Byte>(bytes, 0, 8)));

        // 完整帧可解析
        Assert.True(codec.TryParse(new ReadOnlySequence<Byte>(bytes)).HasValue);
    }

    [Fact]
    [DisplayName("TryParse_坏帧_返回Invalid")]
    public void TryParse_InvalidFrame_ReportsInvalid()
    {
        var codec = new MqCodec();

        // TotalLength 小于 4 + 头部长度
        AssertInvalid(codec, new Byte[] { 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x10 });
        // 头部长度为 0
        AssertInvalid(codec, new Byte[] { 0x00, 0x00, 0x00, 0x20, 0x00, 0x00, 0x00, 0x00 });
        // 单帧超过长度上限（16MB + 1）
        AssertInvalid(codec, new Byte[] { 0x01, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x08 });
    }

    [Fact]
    [DisplayName("BuildHeader_声明体长_可续接流式体")]
    public void BuildHeader_DeclaresBodyLength()
    {
        var codec = new MqCodec();
        var header = codec.BuildHeader(CreateCommand(), 100);
        Assert.NotNull(header);

        // 头部后接 100 字节主体，组成完整帧
        var headerBytes = header.ToArray();
        var full = new Byte[headerBytes.Length + 100];
        headerBytes.CopyTo(full, 0);

        var rs = codec.TryParse(new ReadOnlySequence<Byte>(full));
        Assert.True(rs.HasValue);
        Assert.Equal(100, rs.Value.BodyLength);
        Assert.Equal(full.Length, rs.Value.HeaderSize + rs.Value.BodyLength);
    }

    [Fact]
    [DisplayName("Match_按Opaque配对_且仅应答消息参与")]
    public void Match_ByOpaque()
    {
        var codec = new MqCodec();
        var request = new Command { Header = new Header { Opaque = 7 } };

        Assert.True(codec.Match(request, new Command { Reply = true, Header = new Header { Opaque = 7 } }));

        // Opaque 不同不配对
        Assert.False(codec.Match(request, new Command { Reply = true, Header = new Header { Opaque = 8 } }));

        // 服务端推送的请求不得匹配等待中的请求
        Assert.False(codec.Match(request, new Command { Reply = false, Header = new Header { Opaque = 7 } }));
    }

    [Fact]
    [DisplayName("Build后TryParse_ROCKETMQ二进制帧_往返一致")]
    public void RoundTrip_RocketMqFrame()
    {
        var codec = new MqCodec();
        var cmd = CreateCommand();
        cmd.Header.SerializeTypeCurrentRPC = SerializeType.ROCKETMQ + "";
        var ext = cmd.Header.GetExtFields();
        ext["keyA"] = "valueA";
        ext["keyB"] = "valueB";

        using var frame = codec.Build(cmd);
        Assert.NotNull(frame);

        var rs = codec.TryParse(new ReadOnlySequence<Byte>(frame.ToArray()));
        Assert.True(rs.HasValue);

        var parsed = Assert.IsType<Command>(rs.Value.Message);
        Assert.Equal("ROCKETMQ", parsed.Header.SerializeTypeCurrentRPC);
        Assert.Equal((Int32)RequestCode.SEND_MESSAGE_V2, parsed.Header.Code);
        Assert.Equal(1234, parsed.Header.Opaque);
        Assert.Equal("TEST", parsed.Header.Remark);
        Assert.Equal("valueA", parsed.Header.GetExtFields()["keyA"]);
        Assert.Equal("valueB", parsed.Header.GetExtFields()["keyB"]);
        Assert.Equal("hello rocketmq".GetBytes().Length, rs.Value.BodyLength);
    }

    [Fact]
    [DisplayName("Build后TryParse_空体帧_体长为零")]
    public void RoundTrip_EmptyBody()
    {
        var codec = new MqCodec();
        var cmd = new Command
        {
            Header = new Header
            {
                Code = (Int32)RequestCode.PULL_MESSAGE,
                Opaque = 99,
                Language = LanguageCode.DOTNET + "",
                Remark = "EMPTY",
            },
        };

        using var frame = codec.Build(cmd);
        Assert.NotNull(frame);

        var rs = codec.TryParse(new ReadOnlySequence<Byte>(frame.ToArray()));
        Assert.True(rs.HasValue);
        Assert.NotNull(rs.Value.Message);
        Assert.Equal(0, rs.Value.BodyLength);
        Assert.Equal(frame.Total, rs.Value.HeaderSize + rs.Value.BodyLength);
        Assert.Null(rs.Value.Message.Payload);
    }

    [Fact]
    [DisplayName("BuildHeader_体长越界_抛异常")]
    public void BuildHeader_BodyLengthOutOfRange_Throws()
    {
        var codec = new MqCodec();
        var cmd = CreateCommand();

        Assert.Throws<ArgumentOutOfRangeException>(() => { codec.BuildHeader(cmd, -1); });
        Assert.Throws<ArgumentOutOfRangeException>(() => { codec.BuildHeader(cmd, Int32.MaxValue); });
    }

    [Fact]
    [DisplayName("Build_流式体_抛异常")]
    public async Task Build_StreamingBody_Throws()
    {
        var codec = new MqCodec();
        using var frame = codec.Build(CreateCommand());
        Assert.NotNull(frame);

        // 只投递帧头+头部与部分主体，制造流式体绑定
        var pipe = new Pipe();
        var partial = (Int32)frame.Total - 3;
        pipe.Writer.Append(new ArrayPacket(frame.ToArray(), 0, partial));

        var pump = new MessagePump(codec);
        Assert.True(pump.TryRead(pipe.Reader, out var msg));
        Assert.True(msg.Body is { IsStreaming: true });

        // 流式体不在 Payload，整帧构建必须拒绝，否则会写出截断帧
        Assert.Throws<InvalidOperationException>(() => { codec.Build(msg); });

        // 收尾：补齐余量并丢弃未读体，释放消息
        pipe.Writer.Append(new ArrayPacket(frame.ToArray(), partial, (Int32)frame.Total - partial));
        pipe.Writer.Complete();
        await MessagePump.DiscardAsync(msg);
        msg.TryDispose();
    }

    private static void AssertInvalid(MqCodec codec, Byte[] data)
    {
        var rs = codec.TryParse(new ReadOnlySequence<Byte>(data));
        Assert.True(rs.HasValue);
        Assert.True(rs.Value.Invalid);
    }
}
