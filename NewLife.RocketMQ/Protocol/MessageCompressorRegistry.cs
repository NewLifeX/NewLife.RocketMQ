using System;
using System.Collections.Generic;

namespace NewLife.RocketMQ.Protocol;

/// <summary>消息压缩器注册表。按 RocketMQ SysFlag 压缩类型编号（0–7）路由到对应 <see cref="IMessageCompressor"/>。</summary>
/// <remarks>
/// 默认已注册：
/// <list type="table">
/// <item><term>0</term><description>ZLIB（RocketMQ 默认，发送使用此类型）</description></item>
/// <item><term>3</term><description>ZLIB（0x300 别名，5.x 偶尔上报）</description></item>
/// </list>
/// 注册 LZ4（类型1）或 ZSTD（类型2）：
/// <code>
/// // 在 NewLife.RocketMQ.Extensions 项目中
/// MessageCompressorRegistry.Register(1, new LZ4MessageCompressor());
/// MessageCompressorRegistry.Register(2, new ZstdMessageCompressor());
/// </code>
/// </remarks>
public static class MessageCompressorRegistry
{
    private static readonly Dictionary<Int32, IMessageCompressor> _compressors = new();

    static MessageCompressorRegistry()
    {
        var zlib = new ZlibMessageCompressor();
        _compressors[0] = zlib;  // 默认 ZLIB
        _compressors[3] = zlib;  // 0x300（= 3）：5.x 的 ZLIB 别名
    }

    /// <summary>注册压缩器。可覆盖已有注册（用于单元测试或替换实现）</summary>
    /// <param name="type">压缩类型编号（SysFlag 第 8-10 位，0-7）</param>
    /// <param name="compressor">压缩器实例</param>
    public static void Register(Int32 type, IMessageCompressor compressor)
    {
        if (compressor == null) throw new ArgumentNullException(nameof(compressor));
        lock (_compressors)
        {
            _compressors[type] = compressor;
        }
    }

    /// <summary>获取压缩器，不存在则返回 null</summary>
    /// <param name="type">压缩类型编号</param>
    /// <returns>压缩器实例，或 null</returns>
    public static IMessageCompressor Get(Int32 type)
    {
        lock (_compressors)
        {
            _compressors.TryGetValue(type, out var c);
            return c;
        }
    }

    /// <summary>获取压缩器，不存在则抛出 <see cref="NotSupportedException"/></summary>
    /// <param name="type">压缩类型编号</param>
    /// <returns>压缩器实例</returns>
    /// <exception cref="NotSupportedException">类型未注册时抛出，提示用户安装扩展包</exception>
    public static IMessageCompressor GetOrThrow(Int32 type)
    {
        var c = Get(type);
        if (c == null)
        {
            var hint = type switch
            {
                1 => "LZ4 压缩需引用 NewLife.RocketMQ.Extensions 并调用 MessageCompressorRegistry.Register(1, new LZ4MessageCompressor())",
                2 => "ZSTD 压缩需引用 NewLife.RocketMQ.Extensions 并调用 MessageCompressorRegistry.Register(2, new ZstdMessageCompressor())",
                _ => $"压缩类型 {type} 未注册，请调用 MessageCompressorRegistry.Register({type}, ...) 提供实现",
            };
            throw new NotSupportedException(hint);
        }
        return c;
    }
}
