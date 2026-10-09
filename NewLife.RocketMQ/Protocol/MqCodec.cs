using System;
using System.Buffers;
using System.IO;
using NewLife.Data;
using NewLife.Messaging;

namespace NewLife.RocketMQ.Protocol;

/// <summary>编码器</summary>
/// <remarks>
/// 帧格式（大端序）：[4字节 TotalLength][4字节 OriHeaderLength][HeaderData][BodyData]，
/// 其中 TotalLength = 4 + 头部长度 + 主体长度，OriHeaderLength 的高8位为序列化类型、低24位为头部长度。
/// 无状态、可跨连接共享；请求-响应按 Header.Opaque 配对。
/// </remarks>
public class MqCodec : IMessageCodec, IMessageMatcher
{
    /// <summary>单帧长度上限。与 RocketMQ Java 客户端的帧解码器保持一致</summary>
    private const Int32 _maxFrameLength = 16 * 1024 * 1024;

    /// <summary>头部长度上限。与 Command.Read 保持一致</summary>
    private const Int32 _maxHeaderLength = 8 * 1024;

    /// <summary>定界并构造消息。解析帧头与头部数据，主体由帧层绑定</summary>
    /// <param name="buffer">帧首窗口（只读序列，可跨段）</param>
    /// <returns>解析结果；帧头或头部未到齐返回 null（不消费、不产生对象）；长度非法返回损坏帧</returns>
    public ParseResult? TryParse(ReadOnlySequence<Byte> buffer)
    {
        // 帧头（TotalLength + OriHeaderLength）未到齐
        if (buffer.Length < 8) return null;

        var reader = new SequenceReader<Byte>(buffer);
        reader.TryReadBigEndian(out Int32 totalLength);
        reader.TryReadBigEndian(out Int32 oriHeaderLength);

        // 帧头已完整但长度非法属损坏帧，必须与“数据不足”区分：立即上报，由会话层按协议错误关闭连接，
        // 不能当作不足默默等待，否则会僵死到残余上限才断开
        var headerLength = oriHeaderLength & 0xFFFFFF;
        if (headerLength <= 0 || headerLength > _maxHeaderLength) return new ParseResult { Invalid = true };
        if (totalLength < 4 + headerLength || totalLength > _maxFrameLength) return new ParseResult { Invalid = true };

        // 头部未到齐：等待更多数据，不产生对象
        if (buffer.Length < 8 + headerLength) return null;

        // 头部很小（≤8KB），跨段时复制一次以便解析；主体零拷贝交给帧层
        var headerData = buffer.Slice(8, headerLength).ToArray();
        var type = (SerializeType)((oriHeaderLength >> 24) & 0xFF);

        var cmd = new Command();
        try
        {
            if (!cmd.TryReadHeader(headerData, 0, headerLength, type)) return new ParseResult { Invalid = true };
        }
        catch
        {
            // 头部解码失败：帧内容损坏
            return new ParseResult { Invalid = true };
        }

        return new ParseResult
        {
            Message = cmd,
            HeaderSize = 8 + headerLength,
            BodyLength = totalLength - 4 - headerLength,
        };
    }

    /// <summary>整帧构建（帧头 + 头部 + 主体）</summary>
    /// <param name="message">消息</param>
    /// <returns>整帧拥有句柄，调用方负责 Dispose；消息类型不支持时返回 null</returns>
    public IOwnerPacket? Build(IMessage message)
    {
        if (message is not Command cmd) return null;

        // 流式体不在 Payload，整帧构建只会写出声明体长为 0 的截断帧
        if (cmd.Body is { IsStreaming: true }) throw new InvalidOperationException("流式消息体无法整帧构建，请使用 BuildHeader + 流式发送");

        var ms = new MemoryStream();
        cmd.Write(ms);
        ms.Position = 0;

        return new OwnerPacket(ms);
    }

    /// <summary>仅构建帧头与头部数据，声明主体长度（头 + 流式体发送）</summary>
    /// <param name="message">消息</param>
    /// <param name="bodyLength">主体字节数</param>
    /// <returns>头部拥有句柄，调用方负责 Dispose；消息类型不支持时返回 null</returns>
    public IOwnerPacket? BuildHeader(IMessage message, Int64 bodyLength)
    {
        if (message is not Command cmd) return null;
        if (bodyLength < 0 || bodyLength > Int32.MaxValue - _maxHeaderLength - 8) throw new ArgumentOutOfRangeException(nameof(bodyLength));

        var ms = new MemoryStream();
        cmd.WriteHeader(ms, bodyLength);
        ms.Position = 0;

        return new OwnerPacket(ms);
    }

    /// <summary>判断响应是否匹配请求。仅应答消息参与配对，按 Header.Opaque 相等判定</summary>
    /// <param name="request">挂起的请求消息</param>
    /// <param name="response">收到的响应消息</param>
    /// <returns>是否配对</returns>
    /// <remarks>服务端推送的请求不得匹配等待中的请求，因此先校验应答标志</remarks>
    public Boolean Match(IMessage request, IMessage response)
    {
        if (request is not Command req || response is not Command res) return false;

        // 仅应答类消息参与配对
        if (!res.Reply) return false;

        return req.Header != null && res.Header != null && req.Header.Opaque == res.Header.Opaque;
    }
}
