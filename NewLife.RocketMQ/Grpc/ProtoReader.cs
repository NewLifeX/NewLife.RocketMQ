using System.Text;

namespace NewLife.RocketMQ.Grpc;

/// <summary>Protobuf二进制解码器</summary>
/// <remarks>
/// 实现 Protocol Buffers 解码规范，支持 varint/fixed/length-delimited 三种线路类型。
/// 参考：https://protobuf.dev/programming-guides/encoding/
/// </remarks>
public class ProtoReader
{
    #region 属性
    private readonly Stream _stream;
    private readonly Int64 _limit;

    /// <summary>是否已到末尾</summary>
    public Boolean IsEnd => _stream.Position >= _limit;
    #endregion

    #region 构造
    /// <summary>从字节数组创建解码器</summary>
    /// <param name="data">数据</param>
    public ProtoReader(Byte[] data) : this(new MemoryStream(data)) { }

    /// <summary>从流创建解码器</summary>
    /// <param name="stream">数据流</param>
    public ProtoReader(Stream stream)
    {
        _stream = stream;
        _limit = stream.Length;
    }

    /// <summary>从流的指定区间创建解码器</summary>
    /// <param name="stream">数据流</param>
    /// <param name="length">读取长度</param>
    private ProtoReader(Stream stream, Int32 length)
    {
        _stream = stream;
        _limit = stream.Position + length;
    }
    #endregion

    #region 基础解码
    /// <summary>读取varint编码的无符号整数</summary>
    /// <returns></returns>
    public UInt64 ReadRawVarint()
    {
        var result = 0UL;
        var shift = 0;
        while (shift < 64)
        {
            var b = _stream.ReadByte();
            if (b < 0) throw new EndOfStreamException("Unexpected end of stream while reading varint");
            result |= (UInt64)(b & 0x7F) << shift;
            if ((b & 0x80) == 0) return result;
            shift += 7;
        }
        throw new InvalidDataException("Varint too long");
    }

    /// <summary>读取field tag，返回字段编号和线路类型</summary>
    /// <returns>字段编号和线路类型元组，stream结束时返回(0,0)</returns>
    public (Int32 FieldNumber, Int32 WireType) ReadTag()
    {
        if (IsEnd) return (0, 0);
        var tag = (UInt32)ReadRawVarint();
        return ((Int32)(tag >> 3), (Int32)(tag & 0x07));
    }

    /// <summary>读取固定4字节（小端序）</summary>
    /// <returns></returns>
    public UInt32 ReadRawFixed32()
    {
        var buf = new Byte[4];
        var n = _stream.Read(buf, 0, 4);
        if (n < 4) throw new EndOfStreamException("Unexpected end of stream while reading fixed32");
        return (UInt32)(buf[0] | (buf[1] << 8) | (buf[2] << 16) | (buf[3] << 24));
    }

    /// <summary>读取固定8字节（小端序）</summary>
    /// <returns></returns>
    public UInt64 ReadRawFixed64()
    {
        var lo = (UInt64)ReadRawFixed32();
        var hi = (UInt64)ReadRawFixed32();
        return lo | (hi << 32);
    }

    /// <summary>读取指定长度的原始字节</summary>
    /// <param name="length">长度</param>
    /// <returns></returns>
    public Byte[] ReadRawBytes(Int32 length)
    {
        var buf = new Byte[length];
        var n = _stream.Read(buf, 0, length);
        if (n < length) throw new EndOfStreamException($"Expected {length} bytes but only got {n}");
        return buf;
    }

    /// <summary>跳过当前字段的值</summary>
    /// <param name="wireType">线路类型</param>
    public void SkipField(Int32 wireType)
    {
        switch (wireType)
        {
            case 0: // varint
                ReadRawVarint();
                break;
            case 1: // 64-bit
                ReadRawBytes(8);
                break;
            case 2: // length-delimited
                var len = (Int32)ReadRawVarint();
                ReadRawBytes(len);
                break;
            case 5: // 32-bit
                ReadRawBytes(4);
                break;
            default:
                throw new InvalidDataException($"Unknown wire type: {wireType}");
        }
    }
    #endregion

    #region 字段读取
    /// <summary>读取int32值</summary>
    /// <returns></returns>
    public Int32 ReadInt32() => (Int32)ReadRawVarint();

    /// <summary>读取int64值</summary>
    /// <returns></returns>
    public Int64 ReadInt64() => (Int64)ReadRawVarint();

    /// <summary>读取uint32值</summary>
    /// <returns></returns>
    public UInt32 ReadUInt32() => (UInt32)ReadRawVarint();

    /// <summary>读取uint64值</summary>
    /// <returns></returns>
    public UInt64 ReadUInt64() => ReadRawVarint();

    /// <summary>读取sint32值（ZigZag解码）</summary>
    /// <returns></returns>
    public Int32 ReadSInt32()
    {
        var n = (UInt32)ReadRawVarint();
        return (Int32)((n >> 1) ^ -(Int32)(n & 1));
    }

    /// <summary>读取sint64值（ZigZag解码）</summary>
    /// <returns></returns>
    public Int64 ReadSInt64()
    {
        var n = ReadRawVarint();
        return (Int64)(n >> 1) ^ -((Int64)(n & 1));
    }

    /// <summary>读取bool值</summary>
    /// <returns></returns>
    public Boolean ReadBool() => ReadRawVarint() != 0;

    /// <summary>读取enum值</summary>
    /// <returns></returns>
    public Int32 ReadEnum() => (Int32)ReadRawVarint();

    /// <summary>读取string值（UTF-8）</summary>
    /// <returns></returns>
    public String ReadString()
    {
        var len = (Int32)ReadRawVarint();
        if (len == 0) return "";
        var buf = ReadRawBytes(len);
        return Encoding.UTF8.GetString(buf);
    }

    /// <summary>读取bytes值</summary>
    /// <returns></returns>
    public Byte[] ReadBytes()
    {
        var len = (Int32)ReadRawVarint();
        if (len == 0) return [];
        return ReadRawBytes(len);
    }

    /// <summary>读取嵌套消息</summary>
    /// <typeparam name="T">消息类型</typeparam>
    /// <returns></returns>
    public T ReadMessage<T>() where T : IProtoMessage, new()
    {
        var len = (Int32)ReadRawVarint();
        if (len == 0) return new T();

        var sub = new ProtoReader(_stream, len);
        var msg = new T();
        msg.ReadFrom(sub);

        // 确保子reader消费了所有数据
        if (!sub.IsEnd)
        {
            var skip = (Int32)(sub._limit - _stream.Position);
            if (skip > 0) _stream.Position += skip;
        }

        return msg;
    }

    /// <summary>读取fixed32值</summary>
    /// <returns></returns>
    public UInt32 ReadFixed32() => ReadRawFixed32();

    /// <summary>读取fixed64值</summary>
    /// <returns></returns>
    public UInt64 ReadFixed64() => ReadRawFixed64();

    /// <summary>读取float值</summary>
    /// <returns></returns>
    public Single ReadFloat()
    {
        var bits = ReadRawFixed32();
        var buf = BitConverter.GetBytes(bits);
        return BitConverter.ToSingle(buf, 0);
    }

    /// <summary>读取double值</summary>
    /// <returns></returns>
    public Double ReadDouble()
    {
        var bits = ReadRawFixed64();
        return BitConverter.Int64BitsToDouble((Int64)bits);
    }

    /// <summary>读取map字段中的一个entry（key=1 string, value=2 string）</summary>
    /// <returns></returns>
    public (String Key, String Value) ReadMapEntry()
    {
        var len = (Int32)ReadRawVarint();
        var sub = new ProtoReader(_stream, len);

        String key = null;
        String value = null;
        while (!sub.IsEnd)
        {
            var (fn, wt) = sub.ReadTag();
            if (fn == 0) break;
            switch (fn)
            {
                case 1: key = sub.ReadString(); break;
                case 2: value = sub.ReadString(); break;
                default: sub.SkipField(wt); break;
            }
        }
        return (key, value);
    }

    /// <summary>读取google.protobuf.Timestamp</summary>
    /// <returns></returns>
    public DateTime ReadTimestamp()
    {
        var len = (Int32)ReadRawVarint();
        if (len == 0) return DateTime.MinValue;

        var sub = new ProtoReader(_stream, len);
        var seconds = 0L;
        var nanos = 0;

        while (!sub.IsEnd)
        {
            var (fn, wt) = sub.ReadTag();
            if (fn == 0) break;
            switch (fn)
            {
                case 1: seconds = sub.ReadInt64(); break;
                case 2: nanos = sub.ReadInt32(); break;
                default: sub.SkipField(wt); break;
            }
        }

        var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        return epoch.AddSeconds(seconds).AddTicks(nanos / 100);
    }

    /// <summary>读取google.protobuf.Duration</summary>
    /// <returns></returns>
    public TimeSpan ReadDuration()
    {
        var len = (Int32)ReadRawVarint();
        if (len == 0) return TimeSpan.Zero;

        var sub = new ProtoReader(_stream, len);
        var seconds = 0L;
        var nanos = 0;

        while (!sub.IsEnd)
        {
            var (fn, wt) = sub.ReadTag();
            if (fn == 0) break;
            switch (fn)
            {
                case 1: seconds = sub.ReadInt64(); break;
                case 2: nanos = sub.ReadInt32(); break;
                default: sub.SkipField(wt); break;
            }
        }

        return TimeSpan.FromSeconds(seconds) + TimeSpan.FromTicks(nanos / 100);
    }
    #endregion
}
