using System.Text;
using NewLife.Buffers;
using NewLife.Serialization;

namespace NewLife.RocketMQ.Grpc;

/// <summary>Protobuf编解码扩展方法。为 SpanWriter/SpanReader 提供 Protocol Buffers 二进制格式的读写支持</summary>
/// <remarks>
/// 实现 Protocol Buffers 编码规范，支持 varint/fixed/length-delimited 三种线路类型。
/// 参考：https://protobuf.dev/programming-guides/encoding/
/// </remarks>
public static class ProtoExtensions
{
    #region SpanWriter 写入扩展

    /// <summary>写入varint编码的无符号64位整数</summary>
    public static void WriteRawVarint(ref this SpanWriter writer, UInt64 value)
    {
        while (value > 0x7F)
        {
            writer.Write((Byte)(value | 0x80));
            value >>= 7;
        }
        writer.Write((Byte)value);
    }

    /// <summary>写入field tag（字段编号 + 线路类型）</summary>
    /// <param name="writer">写入器</param>
    /// <param name="fieldNumber">字段编号</param>
    /// <param name="wireType">线路类型。0=varint, 1=64bit, 2=length-delimited, 5=32bit</param>
    public static void WriteTag(ref this SpanWriter writer, Int32 fieldNumber, Int32 wireType)
        => WriteRawVarint(ref writer, (UInt64)((fieldNumber << 3) | wireType));

    /// <summary>写入固定4字节（小端序）</summary>
    public static void WriteRawFixed32(ref this SpanWriter writer, UInt32 value)
    {
        writer.Write((Byte)(value & 0xFF));
        writer.Write((Byte)((value >> 8) & 0xFF));
        writer.Write((Byte)((value >> 16) & 0xFF));
        writer.Write((Byte)((value >> 24) & 0xFF));
    }

    /// <summary>写入固定8字节（小端序）</summary>
    public static void WriteRawFixed64(ref this SpanWriter writer, UInt64 value)
    {
        writer.Write((Byte)(value & 0xFF));
        writer.Write((Byte)((value >> 8) & 0xFF));
        writer.Write((Byte)((value >> 16) & 0xFF));
        writer.Write((Byte)((value >> 24) & 0xFF));
        writer.Write((Byte)((value >> 32) & 0xFF));
        writer.Write((Byte)((value >> 40) & 0xFF));
        writer.Write((Byte)((value >> 48) & 0xFF));
        writer.Write((Byte)((value >> 56) & 0xFF));
    }

    /// <summary>写入int32字段（varint编码）</summary>
    public static void WriteInt32(ref this SpanWriter writer, Int32 fieldNumber, Int32 value)
    {
        if (value == 0) return;
        WriteTag(ref writer, fieldNumber, 0);
        if (value >= 0)
            WriteRawVarint(ref writer, (UInt64)value);
        else
            WriteRawVarint(ref writer, (UInt64)(Int64)value); // 负数按10字节varint编码
    }

    /// <summary>写入int64字段（varint编码）</summary>
    public static void WriteInt64(ref this SpanWriter writer, Int32 fieldNumber, Int64 value)
    {
        if (value == 0) return;
        WriteTag(ref writer, fieldNumber, 0);
        WriteRawVarint(ref writer, (UInt64)value);
    }

    /// <summary>写入uint32字段（varint编码）</summary>
    public static void WriteUInt32(ref this SpanWriter writer, Int32 fieldNumber, UInt32 value)
    {
        if (value == 0) return;
        WriteTag(ref writer, fieldNumber, 0);
        WriteRawVarint(ref writer, value);
    }

    /// <summary>写入uint64字段（varint编码）</summary>
    public static void WriteUInt64(ref this SpanWriter writer, Int32 fieldNumber, UInt64 value)
    {
        if (value == 0) return;
        WriteTag(ref writer, fieldNumber, 0);
        WriteRawVarint(ref writer, value);
    }

    /// <summary>写入sint32字段（ZigZag + varint编码）</summary>
    public static void WriteSInt32(ref this SpanWriter writer, Int32 fieldNumber, Int32 value)
    {
        if (value == 0) return;
        WriteTag(ref writer, fieldNumber, 0);
        WriteRawVarint(ref writer, (UInt32)((value << 1) ^ (value >> 31)));
    }

    /// <summary>写入sint64字段（ZigZag + varint编码）</summary>
    public static void WriteSInt64(ref this SpanWriter writer, Int32 fieldNumber, Int64 value)
    {
        if (value == 0) return;
        WriteTag(ref writer, fieldNumber, 0);
        WriteRawVarint(ref writer, (UInt64)((value << 1) ^ (value >> 63)));
    }

    /// <summary>写入bool字段</summary>
    public static void WriteBool(ref this SpanWriter writer, Int32 fieldNumber, Boolean value)
    {
        if (!value) return;
        WriteTag(ref writer, fieldNumber, 0);
        writer.Write((Byte)1);
    }

    /// <summary>写入enum字段（varint编码）</summary>
    public static void WriteEnum(ref this SpanWriter writer, Int32 fieldNumber, Int32 value)
    {
        if (value == 0) return;
        WriteTag(ref writer, fieldNumber, 0);
        WriteRawVarint(ref writer, (UInt64)(UInt32)value);
    }

    /// <summary>写入string字段（length-delimited UTF-8）</summary>
    public static void WriteString(ref this SpanWriter writer, Int32 fieldNumber, String value)
    {
        if (String.IsNullOrEmpty(value)) return;
        WriteTag(ref writer, fieldNumber, 2);
        var bytes = Encoding.UTF8.GetBytes(value);
        WriteRawVarint(ref writer, (UInt64)bytes.Length);
        writer.Write(bytes);
    }

    /// <summary>写入bytes字段（length-delimited）</summary>
    public static void WriteBytes(ref this SpanWriter writer, Int32 fieldNumber, Byte[] value)
    {
        if (value == null || value.Length == 0) return;
        WriteTag(ref writer, fieldNumber, 2);
        WriteRawVarint(ref writer, (UInt64)value.Length);
        writer.Write(value);
    }

    /// <summary>写入嵌套消息字段（length-delimited）。使用重试缓冲区处理未知大小的子消息</summary>
    public static void WriteMessage(ref this SpanWriter writer, Int32 fieldNumber, ISpanSerializable message)
    {
        if (message == null) return;

        // 使用重试缓冲区计算子消息长度
        var size = 4096;
        while (true)
        {
            var temp = new Byte[size];
            var sub = new SpanWriter(temp);
            try
            {
                message.Write(ref sub);
                var len = sub.WrittenCount;
                WriteTag(ref writer, fieldNumber, 2);
                WriteRawVarint(ref writer, (UInt64)len);
                writer.Write(new ReadOnlySpan<Byte>(temp, 0, len));
                return;
            }
            catch (InvalidOperationException)
            {
                size = checked(size * 2);
            }
        }
    }

    /// <summary>写入fixed32字段（4字节小端序）</summary>
    public static void WriteFixed32(ref this SpanWriter writer, Int32 fieldNumber, UInt32 value)
    {
        if (value == 0) return;
        WriteTag(ref writer, fieldNumber, 5);
        WriteRawFixed32(ref writer, value);
    }

    /// <summary>写入fixed64字段（8字节小端序）</summary>
    public static void WriteFixed64(ref this SpanWriter writer, Int32 fieldNumber, UInt64 value)
    {
        if (value == 0) return;
        WriteTag(ref writer, fieldNumber, 1);
        WriteRawFixed64(ref writer, value);
    }

    /// <summary>写入float字段（fixed32小端序）</summary>
    public static void WriteFloat(ref this SpanWriter writer, Int32 fieldNumber, Single value)
    {
        if (value == 0) return;
        WriteTag(ref writer, fieldNumber, 5);
        WriteRawFixed32(ref writer, BitConverter.ToUInt32(BitConverter.GetBytes(value), 0));
    }

    /// <summary>写入double字段（fixed64小端序）</summary>
    public static void WriteDouble(ref this SpanWriter writer, Int32 fieldNumber, Double value)
    {
        if (value == 0) return;
        WriteTag(ref writer, fieldNumber, 1);
        WriteRawFixed64(ref writer, (UInt64)BitConverter.DoubleToInt64Bits(value));
    }

    /// <summary>写入map字段。map编码为 repeated message { key=1, value=2 }</summary>
    public static void WriteMap(ref this SpanWriter writer, Int32 fieldNumber, IDictionary<String, String> map)
    {
        if (map == null || map.Count == 0) return;
        foreach (var kv in map)
        {
            var entryBuf = new Byte[512];
            var entry = new SpanWriter(entryBuf);
            WriteString(ref entry, 1, kv.Key);
            WriteString(ref entry, 2, kv.Value);
            var len = entry.WrittenCount;

            WriteTag(ref writer, fieldNumber, 2);
            WriteRawVarint(ref writer, (UInt64)len);
            writer.Write(new ReadOnlySpan<Byte>(entryBuf, 0, len));
        }
    }

    /// <summary>写入repeated string字段</summary>
    public static void WriteRepeatedString(ref this SpanWriter writer, Int32 fieldNumber, IList<String> values)
    {
        if (values == null || values.Count == 0) return;
        foreach (var value in values)
            WriteString(ref writer, fieldNumber, value);
    }

    /// <summary>写入repeated enum字段（packed编码）</summary>
    public static void WritePackedEnum(ref this SpanWriter writer, Int32 fieldNumber, IList<Int32> values)
    {
        if (values == null || values.Count == 0) return;

        var subBuf = new Byte[values.Count * 5]; // varint最多5字节
        var sub = new SpanWriter(subBuf);
        foreach (var v in values)
            WriteRawVarint(ref sub, (UInt64)(UInt32)v);

        var len = sub.WrittenCount;
        WriteTag(ref writer, fieldNumber, 2);
        WriteRawVarint(ref writer, (UInt64)len);
        writer.Write(new ReadOnlySpan<Byte>(subBuf, 0, len));
    }

    /// <summary>写入repeated message字段</summary>
    public static void WriteRepeatedMessage<T>(ref this SpanWriter writer, Int32 fieldNumber, IList<T> messages)
        where T : ISpanSerializable
    {
        if (messages == null || messages.Count == 0) return;
        foreach (var msg in messages)
            WriteMessage(ref writer, fieldNumber, msg);
    }

    /// <summary>写入google.protobuf.Timestamp（seconds=1, nanos=2）</summary>
    public static void WriteTimestamp(ref this SpanWriter writer, Int32 fieldNumber, DateTime? value)
    {
        if (value == null || value.Value == DateTime.MinValue) return;

        var utc = value.Value.Kind == DateTimeKind.Utc ? value.Value : value.Value.ToUniversalTime();
        var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var ts = utc - epoch;

        var subBuf = new Byte[32];
        var sub = new SpanWriter(subBuf);
        WriteInt64(ref sub, 1, (Int64)ts.TotalSeconds);
        var nanos = (Int32)((ts.Ticks % TimeSpan.TicksPerSecond) * 100);
        WriteInt32(ref sub, 2, nanos);

        var len = sub.WrittenCount;
        WriteTag(ref writer, fieldNumber, 2);
        WriteRawVarint(ref writer, (UInt64)len);
        writer.Write(new ReadOnlySpan<Byte>(subBuf, 0, len));
    }

    /// <summary>写入google.protobuf.Duration（seconds=1, nanos=2）</summary>
    public static void WriteDuration(ref this SpanWriter writer, Int32 fieldNumber, TimeSpan? value)
    {
        if (value == null || value.Value == TimeSpan.Zero) return;

        var subBuf = new Byte[32];
        var sub = new SpanWriter(subBuf);
        WriteInt64(ref sub, 1, (Int64)value.Value.TotalSeconds);
        var nanos = (Int32)((value.Value.Ticks % TimeSpan.TicksPerSecond) * 100);
        WriteInt32(ref sub, 2, nanos);

        var len = sub.WrittenCount;
        WriteTag(ref writer, fieldNumber, 2);
        WriteRawVarint(ref writer, (UInt64)len);
        writer.Write(new ReadOnlySpan<Byte>(subBuf, 0, len));
    }

    #endregion

    #region SpanReader 读取扩展

    /// <summary>读取varint编码的无符号64位整数</summary>
    public static UInt64 ReadRawVarint(ref this SpanReader reader)
    {
        var result = 0UL;
        var shift = 0;
        while (shift < 64)
        {
            if (reader.Available <= 0) throw new EndOfStreamException("读取varint时到达数据末尾");
            var b = reader.ReadByte();
            result |= (UInt64)(b & 0x7F) << shift;
            if ((b & 0x80) == 0) return result;
            shift += 7;
        }
        throw new InvalidDataException("Varint编码超长");
    }

    /// <summary>读取field tag，返回字段编号和线路类型</summary>
    public static (Int32 FieldNumber, Int32 WireType) ReadTag(ref this SpanReader reader)
    {
        if (reader.Available <= 0) return (0, 0);
        var tag = (UInt32)ReadRawVarint(ref reader);
        return ((Int32)(tag >> 3), (Int32)(tag & 0x07));
    }

    /// <summary>读取int32值（varint解码）</summary>
    public static Int32 ReadProtoInt32(ref this SpanReader reader) => (Int32)ReadRawVarint(ref reader);

    /// <summary>读取int64值（varint解码）</summary>
    public static Int64 ReadProtoInt64(ref this SpanReader reader) => (Int64)ReadRawVarint(ref reader);

    /// <summary>读取uint32值（varint解码）</summary>
    public static UInt32 ReadProtoUInt32(ref this SpanReader reader) => (UInt32)ReadRawVarint(ref reader);

    /// <summary>读取sint32值（ZigZag解码）</summary>
    public static Int32 ReadSInt32(ref this SpanReader reader)
    {
        var n = (UInt32)ReadRawVarint(ref reader);
        return (Int32)((n >> 1) ^ -(Int32)(n & 1));
    }

    /// <summary>读取sint64值（ZigZag解码）</summary>
    public static Int64 ReadSInt64(ref this SpanReader reader)
    {
        var n = ReadRawVarint(ref reader);
        return (Int64)(n >> 1) ^ -((Int64)(n & 1));
    }

    /// <summary>读取bool值</summary>
    public static Boolean ReadBool(ref this SpanReader reader) => ReadRawVarint(ref reader) != 0;

    /// <summary>读取enum值（varint解码）</summary>
    public static Int32 ReadEnum(ref this SpanReader reader) => (Int32)ReadRawVarint(ref reader);

    /// <summary>读取string值（length-delimited UTF-8）</summary>
    public static String ReadProtoString(ref this SpanReader reader)
    {
        var len = (Int32)ReadRawVarint(ref reader);
        if (len == 0) return "";
        var buf = reader.ReadBytes(len).ToArray();
        return Encoding.UTF8.GetString(buf);
    }

    /// <summary>读取bytes值（length-delimited）</summary>
    public static Byte[] ReadProtoBytes(ref this SpanReader reader)
    {
        var len = (Int32)ReadRawVarint(ref reader);
        if (len == 0) return [];
        return reader.ReadBytes(len).ToArray();
    }

    /// <summary>读取fixed32值（小端序4字节）</summary>
    public static UInt32 ReadFixed32(ref this SpanReader reader)
    {
        var span = reader.ReadBytes(4);
        return (UInt32)(span[0] | (span[1] << 8) | (span[2] << 16) | (span[3] << 24));
    }

    /// <summary>读取fixed64值（小端序8字节）</summary>
    public static UInt64 ReadFixed64(ref this SpanReader reader)
    {
        var span = reader.ReadBytes(8);
        return (UInt64)span[0] | ((UInt64)span[1] << 8) | ((UInt64)span[2] << 16) | ((UInt64)span[3] << 24)
             | ((UInt64)span[4] << 32) | ((UInt64)span[5] << 40) | ((UInt64)span[6] << 48) | ((UInt64)span[7] << 56);
    }

    /// <summary>读取float值（小端序4字节）</summary>
    public static Single ReadFloat(ref this SpanReader reader)
    {
        var bytes = reader.ReadBytes(4).ToArray();
        return BitConverter.ToSingle(bytes, 0);
    }

    /// <summary>读取double值（小端序8字节）</summary>
    public static Double ReadProtoDouble(ref this SpanReader reader)
    {
        var bytes = reader.ReadBytes(8).ToArray();
        return BitConverter.ToDouble(bytes, 0);
    }

    /// <summary>读取嵌套消息</summary>
    public static T ReadProtoMessage<T>(ref this SpanReader reader) where T : ISpanSerializable, new()
    {
        var len = (Int32)ReadRawVarint(ref reader);
        if (len == 0) return new T();

        var subData = reader.ReadBytes(len).ToArray();
        var sub = new SpanReader(subData);
        var msg = new T();
        msg.Read(ref sub);
        return msg;
    }

    /// <summary>读取map字段中的一个entry（key=1 string, value=2 string）</summary>
    public static (String Key, String Value) ReadMapEntry(ref this SpanReader reader)
    {
        var len = (Int32)ReadRawVarint(ref reader);
        var subData = reader.ReadBytes(len).ToArray();
        var sub = new SpanReader(subData);

        String key = null;
        String value = null;
        while (sub.Available > 0)
        {
            var (fn, wt) = ReadTag(ref sub);
            if (fn == 0) break;
            switch (fn)
            {
                case 1: key = ReadProtoString(ref sub); break;
                case 2: value = ReadProtoString(ref sub); break;
                default: SkipField(ref sub, wt); break;
            }
        }
        return (key, value);
    }

    /// <summary>读取google.protobuf.Timestamp</summary>
    public static DateTime ReadTimestamp(ref this SpanReader reader)
    {
        var len = (Int32)ReadRawVarint(ref reader);
        if (len == 0) return DateTime.MinValue;

        var subData = reader.ReadBytes(len).ToArray();
        var sub = new SpanReader(subData);
        var seconds = 0L;
        var nanos = 0;

        while (sub.Available > 0)
        {
            var (fn, wt) = ReadTag(ref sub);
            if (fn == 0) break;
            switch (fn)
            {
                case 1: seconds = ReadProtoInt64(ref sub); break;
                case 2: nanos = ReadProtoInt32(ref sub); break;
                default: SkipField(ref sub, wt); break;
            }
        }

        var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        return epoch.AddSeconds(seconds).AddTicks(nanos / 100);
    }

    /// <summary>读取google.protobuf.Duration</summary>
    public static TimeSpan ReadDuration(ref this SpanReader reader)
    {
        var len = (Int32)ReadRawVarint(ref reader);
        if (len == 0) return TimeSpan.Zero;

        var subData = reader.ReadBytes(len).ToArray();
        var sub = new SpanReader(subData);
        var seconds = 0L;
        var nanos = 0;

        while (sub.Available > 0)
        {
            var (fn, wt) = ReadTag(ref sub);
            if (fn == 0) break;
            switch (fn)
            {
                case 1: seconds = ReadProtoInt64(ref sub); break;
                case 2: nanos = ReadProtoInt32(ref sub); break;
                default: SkipField(ref sub, wt); break;
            }
        }

        return TimeSpan.FromSeconds(seconds) + TimeSpan.FromTicks(nanos / 100);
    }

    /// <summary>跳过当前字段的值</summary>
    public static void SkipField(ref this SpanReader reader, Int32 wireType)
    {
        switch (wireType)
        {
            case 0: // varint
                ReadRawVarint(ref reader);
                break;
            case 1: // 64-bit
                reader.ReadBytes(8);
                break;
            case 2: // length-delimited
                var len = (Int32)ReadRawVarint(ref reader);
                reader.ReadBytes(len);
                break;
            case 5: // 32-bit
                reader.ReadBytes(4);
                break;
            default:
                throw new InvalidDataException($"未知的线路类型: {wireType}");
        }
    }

    #endregion

    #region 序列化辅助

    /// <summary>序列化 ISpanSerializable 消息为 Protobuf 字节数组。使用重试缓冲区处理未知大小的消息</summary>
    /// <param name="message">消息</param>
    /// <param name="initialCapacity">初始缓冲区大小</param>
    /// <returns>Protobuf 编码的字节数组</returns>
    public static Byte[] Serialize(ISpanSerializable message, Int32 initialCapacity = 4096)
    {
        if (message == null) return [];
        var capacity = initialCapacity;
        while (true)
        {
            var buf = new Byte[capacity];
            var writer = new SpanWriter(buf);
            try
            {
                message.Write(ref writer);
                return writer.WrittenSpan.ToArray();
            }
            catch (InvalidOperationException)
            {
                capacity = checked(capacity * 2);
            }
        }
    }

    #endregion
}
