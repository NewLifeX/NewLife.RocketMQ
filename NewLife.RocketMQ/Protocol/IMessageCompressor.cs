using System;
using System.IO;
using System.IO.Compression;

namespace NewLife.RocketMQ.Protocol;

/// <summary>消息体压缩/解压接口。通过 <see cref="MessageCompressorRegistry"/> 注册后可按类型编号路由。</summary>
/// <remarks>
/// RocketMQ 在 SysFlag 第 8–10 位编码压缩类型（COMPRESSION_TYPE_COMPARATOR = 0x700）：
/// 0/0x300 = ZLIB（默认），0x100 = LZ4，0x200 = ZSTD。
/// 零依赖实现：仅内置 ZLIB（<see cref="ZlibMessageCompressor"/>）；
/// LZ4/ZSTD 请在 NewLife.RocketMQ.Extensions 项目中注册（F052 扩展包）。
/// </remarks>
public interface IMessageCompressor
{
    /// <summary>压缩消息体</summary>
    /// <param name="data">原始字节</param>
    /// <returns>压缩后字节</returns>
    Byte[] Compress(Byte[] data);

    /// <summary>解压消息体</summary>
    /// <param name="data">压缩字节（ZLIB 格式需含 2 字节头部）</param>
    /// <returns>原始字节</returns>
    Byte[] Decompress(Byte[] data);
}

/// <summary>基于 ZLIB/DEFLATE 的消息体压缩器（对应 RocketMQ 压缩类型 0/3，默认）</summary>
/// <remarks>
/// 写入时生成标准 RFC1950 ZLIB 格式（含 2 字节 CMF/FLG 头 + Adler-32 校验尾）；
/// 读取时自动检测 RFC1950 ZLIB 头部，兼容 RAW DEFLATE 格式。
/// </remarks>
public class ZlibMessageCompressor : IMessageCompressor
{
    /// <summary>压缩，输出 RFC1950 ZLIB 格式（2字节头 + deflate + Adler-32尾）</summary>
    /// <param name="data">原始字节</param>
    /// <returns>压缩后字节</returns>
    public Byte[] Compress(Byte[] data)
    {
        if (data == null) return null;
        if (data.Length == 0) return data;

        using var ms = new MemoryStream();

        // 写入 RFC1950 ZLIB 头（CMF=0x78 DEFLATE+32K窗口，FLG=0x9C 校验位）
        ms.WriteByte(0x78);
        ms.WriteByte(0x9C);

        // 写入 DEFLATE 压缩数据（不包含头部/尾部）
        using (var ds = new DeflateStream(ms, CompressionMode.Compress, leaveOpen: true))
        {
            ds.Write(data, 0, data.Length);
        }

        // 写入 Adler-32 校验尾（大端）
        var adler = ComputeAdler32(data);
        ms.WriteByte((Byte)(adler >> 24));
        ms.WriteByte((Byte)(adler >> 16));
        ms.WriteByte((Byte)(adler >> 8));
        ms.WriteByte((Byte)adler);

        return ms.ToArray();
    }

    /// <summary>解压，自动兼容 RFC1950 ZLIB 格式（剥离2字节头）和 RAW DEFLATE 格式</summary>
    /// <param name="data">压缩字节</param>
    /// <returns>原始字节</returns>
    public Byte[] Decompress(Byte[] data)
    {
        if (data == null) return null;
        if (data.Length == 0) return data;

        var offset = 0;
        var length = data.Length;

        // 检测并跳过 RFC1950 ZLIB 2字节头部
        if (data.Length >= 2)
        {
            var cmf = data[0];
            var flg = data[1];
            var hasZlibHeader =
                (cmf & 0x0F) == 8 &&
                (cmf >> 4) <= 7 &&
                (((cmf << 8) + flg) % 31) == 0;

            if (hasZlibHeader)
            {
                offset = 2;
                // 同时剥离末尾 Adler-32（4字节），若存在
                length = data.Length - 2 - 4;
                if (length < 0) length = data.Length - 2;
            }
        }

        using var ms = new MemoryStream(data, offset, Math.Max(0, length));
        using var ds = new DeflateStream(ms, CompressionMode.Decompress);
        using var output = new MemoryStream();
        ds.CopyTo(output);
        return output.ToArray();
    }

    /// <summary>计算 Adler-32 校验值</summary>
    /// <param name="data">数据</param>
    /// <returns>Adler-32 值</returns>
    private static UInt32 ComputeAdler32(Byte[] data)
    {
        const UInt32 MOD_ADLER = 65521;
        UInt32 a = 1, b = 0;
        foreach (var bt in data)
        {
            a = (a + bt) % MOD_ADLER;
            b = (b + a) % MOD_ADLER;
        }
        return (b << 16) | a;
    }
}
