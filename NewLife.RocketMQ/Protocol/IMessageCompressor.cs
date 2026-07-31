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
    /// <summary>解压输出大小上限。默认4MB（RocketMQ 消息上限），防止 zip bomb</summary>
    public const Int32 MaxDecompressSize = 4 * 1024 * 1024;

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

    /// <summary>解压，自动兼容 RFC1950 ZLIB 格式（剥离2字节头+4字节尾）和 RAW DEFLATE 格式</summary>
    /// <param name="data">压缩字节</param>
    /// <returns>原始字节</returns>
    public Byte[] Decompress(Byte[] data)
    {
        if (data == null) return null;
        if (data.Length == 0) return data;

        // 检测 RFC1950 ZLIB 2字节头部
        var hasZlibHeader = data.Length >= 2 && IsZlibHeader(data[0], data[1]);
        if (hasZlibHeader)
        {
            // 标准 ZLIB：剥离 2 字节头 + 4 字节 Adler-32 尾
            try
            {
                return DecompressCore(data, 2, data.Length - 6);
            }
            catch (InvalidDataException)
            {
                // 部分实现不含 Adler-32 尾，仅剥离头部重试
            }
            try
            {
                return DecompressCore(data, 2, data.Length - 2);
            }
            catch (InvalidDataException)
            {
                // 头部误判（概率极低），整体作为 RAW DEFLATE 重试
            }
        }

        // RAW DEFLATE
        return DecompressCore(data, 0, data.Length);
    }

    /// <summary>检测是否为 RFC1950 ZLIB 头部</summary>
    private static Boolean IsZlibHeader(Byte cmf, Byte flg)
        => (cmf & 0x0F) == 8 && (cmf >> 4) <= 7 && (((cmf << 8) + flg) % 31) == 0;

    /// <summary>核心解压。限制输出大小防止 zip bomb</summary>
    private static Byte[] DecompressCore(Byte[] data, Int32 offset, Int32 length)
    {
        if (length < 0) length = data.Length - offset;
        if (length <= 0) return [];

        using var ms = new MemoryStream(data, offset, length);
        using var ds = new DeflateStream(ms, CompressionMode.Decompress);
        using var output = new MemoryStream();

        var buf = new Byte[8192];
        var total = 0;
        Int32 read;
        while ((read = ds.Read(buf, 0, buf.Length)) > 0)
        {
            total += read;
            if (total > MaxDecompressSize) throw new InvalidDataException($"解压数据超过上限[{MaxDecompressSize}]");
            output.Write(buf, 0, read);
        }

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
