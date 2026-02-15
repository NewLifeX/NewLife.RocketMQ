using System.Text;

namespace NewLife.RocketMQ.Grpc;

/// <summary>Protobuf二进制编码器</summary>
/// <remarks>
/// 实现 Protocol Buffers 编码规范，支持 varint/fixed/length-delimited 三种线路类型。
/// 参考：https://protobuf.dev/programming-guides/encoding/
/// </remarks>
public class ProtoWriter
{
    #region 属性
    private readonly MemoryStream _stream;

    /// <summary>当前写入的字节数</summary>
    public Int32 Length => (Int32)_stream.Length;
    #endregion

    #region 构造
    /// <summary>实例化Protobuf编码器</summary>
    public ProtoWriter() => _stream = new MemoryStream();

    /// <summary>实例化Protobuf编码器</summary>
    /// <param name="capacity">初始容量</param>
    public ProtoWriter(Int32 capacity) => _stream = new MemoryStream(capacity);
    #endregion

    #region 基础编码
    /// <summary>写入varint编码的无符号整数</summary>
    /// <param name="value">值</param>
    public void WriteRawVarint(UInt64 value)
    {
        while (value > 0x7F)
        {
            _stream.WriteByte((Byte)(value | 0x80));
            value >>= 7;
        }
        _stream.WriteByte((Byte)value);
    }

    /// <summary>写入field tag</summary>
    /// <param name="fieldNumber">字段编号</param>
    /// <param name="wireType">线路类型。0=varint, 1=64bit, 2=length-delimited, 5=32bit</param>
    public void WriteTag(Int32 fieldNumber, Int32 wireType) => WriteRawVarint((UInt64)((fieldNumber << 3) | wireType));

    /// <summary>写入固定4字节（小端序）</summary>
    /// <param name="value">值</param>
    public void WriteRawFixed32(UInt32 value)
    {
        _stream.WriteByte((Byte)value);
        _stream.WriteByte((Byte)(value >> 8));
        _stream.WriteByte((Byte)(value >> 16));
        _stream.WriteByte((Byte)(value >> 24));
    }

    /// <summary>写入固定8字节（小端序）</summary>
    /// <param name="value">值</param>
    public void WriteRawFixed64(UInt64 value)
    {
        WriteRawFixed32((UInt32)value);
        WriteRawFixed32((UInt32)(value >> 32));
    }

    /// <summary>写入原始字节</summary>
    /// <param name="data">数据</param>
    public void WriteRawBytes(Byte[] data) => _stream.Write(data, 0, data.Length);
    #endregion

    #region 字段写入
    /// <summary>写入int32字段（varint编码）</summary>
    /// <param name="fieldNumber">字段编号</param>
    /// <param name="value">值</param>
    public void WriteInt32(Int32 fieldNumber, Int32 value)
    {
        if (value == 0) return;
        WriteTag(fieldNumber, 0);
        if (value >= 0)
            WriteRawVarint((UInt64)value);
        else
            WriteRawVarint((UInt64)(Int64)value); // 负数按10字节varint编码
    }

    /// <summary>写入int64字段（varint编码）</summary>
    /// <param name="fieldNumber">字段编号</param>
    /// <param name="value">值</param>
    public void WriteInt64(Int32 fieldNumber, Int64 value)
    {
        if (value == 0) return;
        WriteTag(fieldNumber, 0);
        WriteRawVarint((UInt64)value);
    }

    /// <summary>写入uint32字段（varint编码）</summary>
    /// <param name="fieldNumber">字段编号</param>
    /// <param name="value">值</param>
    public void WriteUInt32(Int32 fieldNumber, UInt32 value)
    {
        if (value == 0) return;
        WriteTag(fieldNumber, 0);
        WriteRawVarint(value);
    }

    /// <summary>写入uint64字段（varint编码）</summary>
    /// <param name="fieldNumber">字段编号</param>
    /// <param name="value">值</param>
    public void WriteUInt64(Int32 fieldNumber, UInt64 value)
    {
        if (value == 0) return;
        WriteTag(fieldNumber, 0);
        WriteRawVarint(value);
    }

    /// <summary>写入sint32字段（ZigZag + varint编码）</summary>
    /// <param name="fieldNumber">字段编号</param>
    /// <param name="value">值</param>
    public void WriteSInt32(Int32 fieldNumber, Int32 value)
    {
        if (value == 0) return;
        WriteTag(fieldNumber, 0);
        WriteRawVarint((UInt32)((value << 1) ^ (value >> 31)));
    }

    /// <summary>写入sint64字段（ZigZag + varint编码）</summary>
    /// <param name="fieldNumber">字段编号</param>
    /// <param name="value">值</param>
    public void WriteSInt64(Int32 fieldNumber, Int64 value)
    {
        if (value == 0) return;
        WriteTag(fieldNumber, 0);
        WriteRawVarint((UInt64)((value << 1) ^ (value >> 63)));
    }

    /// <summary>写入bool字段</summary>
    /// <param name="fieldNumber">字段编号</param>
    /// <param name="value">值</param>
    public void WriteBool(Int32 fieldNumber, Boolean value)
    {
        if (!value) return;
        WriteTag(fieldNumber, 0);
        _stream.WriteByte(1);
    }

    /// <summary>写入enum字段（varint编码）</summary>
    /// <param name="fieldNumber">字段编号</param>
    /// <param name="value">值</param>
    public void WriteEnum(Int32 fieldNumber, Int32 value)
    {
        if (value == 0) return;
        WriteTag(fieldNumber, 0);
        WriteRawVarint((UInt64)(UInt32)value);
    }

    /// <summary>写入string字段（length-delimited UTF-8）</summary>
    /// <param name="fieldNumber">字段编号</param>
    /// <param name="value">值</param>
    public void WriteString(Int32 fieldNumber, String value)
    {
        if (String.IsNullOrEmpty(value)) return;
        WriteTag(fieldNumber, 2);
        var bytes = Encoding.UTF8.GetBytes(value);
        WriteRawVarint((UInt64)bytes.Length);
        WriteRawBytes(bytes);
    }

    /// <summary>写入bytes字段（length-delimited）</summary>
    /// <param name="fieldNumber">字段编号</param>
    /// <param name="value">值</param>
    public void WriteBytes(Int32 fieldNumber, Byte[] value)
    {
        if (value == null || value.Length == 0) return;
        WriteTag(fieldNumber, 2);
        WriteRawVarint((UInt64)value.Length);
        WriteRawBytes(value);
    }

    /// <summary>写入嵌套消息字段（length-delimited）</summary>
    /// <param name="fieldNumber">字段编号</param>
    /// <param name="message">消息</param>
    public void WriteMessage(Int32 fieldNumber, IProtoMessage message)
    {
        if (message == null) return;

        // 先写入到临时缓冲区计算长度
        var sub = new ProtoWriter();
        message.WriteTo(sub);
        var data = sub.ToArray();

        WriteTag(fieldNumber, 2);
        WriteRawVarint((UInt64)data.Length);
        WriteRawBytes(data);
    }

    /// <summary>写入fixed32字段（4字节小端序）</summary>
    /// <param name="fieldNumber">字段编号</param>
    /// <param name="value">值</param>
    public void WriteFixed32(Int32 fieldNumber, UInt32 value)
    {
        if (value == 0) return;
        WriteTag(fieldNumber, 5);
        WriteRawFixed32(value);
    }

    /// <summary>写入fixed64字段（8字节小端序）</summary>
    /// <param name="fieldNumber">字段编号</param>
    /// <param name="value">值</param>
    public void WriteFixed64(Int32 fieldNumber, UInt64 value)
    {
        if (value == 0) return;
        WriteTag(fieldNumber, 1);
        WriteRawFixed64(value);
    }

    /// <summary>写入float字段（fixed32）</summary>
    /// <param name="fieldNumber">字段编号</param>
    /// <param name="value">值</param>
    public void WriteFloat(Int32 fieldNumber, Single value)
    {
        if (value == 0) return;
        WriteTag(fieldNumber, 5);
        var bytes = BitConverter.GetBytes(value);
        WriteRawBytes(bytes);
    }

    /// <summary>写入double字段（fixed64）</summary>
    /// <param name="fieldNumber">字段编号</param>
    /// <param name="value">值</param>
    public void WriteDouble(Int32 fieldNumber, Double value)
    {
        if (value == 0) return;
        WriteTag(fieldNumber, 1);
        var bytes = BitConverter.GetBytes(value);
        WriteRawBytes(bytes);
    }

    /// <summary>写入map字段。map编码为 repeated message { key=1, value=2 }</summary>
    /// <param name="fieldNumber">字段编号</param>
    /// <param name="map">字典</param>
    public void WriteMap(Int32 fieldNumber, IDictionary<String, String> map)
    {
        if (map == null || map.Count == 0) return;
        foreach (var kv in map)
        {
            var entry = new ProtoWriter();
            entry.WriteString(1, kv.Key);
            entry.WriteString(2, kv.Value);
            var data = entry.ToArray();

            WriteTag(fieldNumber, 2);
            WriteRawVarint((UInt64)data.Length);
            WriteRawBytes(data);
        }
    }

    /// <summary>写入repeated string字段</summary>
    /// <param name="fieldNumber">字段编号</param>
    /// <param name="values">值列表</param>
    public void WriteRepeatedString(Int32 fieldNumber, IList<String> values)
    {
        if (values == null || values.Count == 0) return;
        foreach (var value in values)
        {
            WriteString(fieldNumber, value);
        }
    }

    /// <summary>写入repeated enum字段（packed编码）</summary>
    /// <param name="fieldNumber">字段编号</param>
    /// <param name="values">值列表</param>
    public void WritePackedEnum(Int32 fieldNumber, IList<Int32> values)
    {
        if (values == null || values.Count == 0) return;

        var sub = new ProtoWriter();
        foreach (var v in values)
        {
            sub.WriteRawVarint((UInt64)(UInt32)v);
        }
        var data = sub.ToArray();

        WriteTag(fieldNumber, 2);
        WriteRawVarint((UInt64)data.Length);
        WriteRawBytes(data);
    }

    /// <summary>写入repeated message字段</summary>
    /// <param name="fieldNumber">字段编号</param>
    /// <param name="messages">消息列表</param>
    public void WriteRepeatedMessage<T>(Int32 fieldNumber, IList<T> messages) where T : IProtoMessage
    {
        if (messages == null || messages.Count == 0) return;
        foreach (var msg in messages)
        {
            WriteMessage(fieldNumber, msg);
        }
    }

    /// <summary>写入google.protobuf.Timestamp（seconds=1, nanos=2）</summary>
    /// <param name="fieldNumber">字段编号</param>
    /// <param name="value">时间值</param>
    public void WriteTimestamp(Int32 fieldNumber, DateTime? value)
    {
        if (value == null || value.Value == DateTime.MinValue) return;

        var utc = value.Value.Kind == DateTimeKind.Utc ? value.Value : value.Value.ToUniversalTime();
        var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var ts = utc - epoch;

        var sub = new ProtoWriter();
        sub.WriteInt64(1, (Int64)ts.TotalSeconds);
        var nanos = (Int32)((ts.Ticks % TimeSpan.TicksPerSecond) * 100);
        sub.WriteInt32(2, nanos);

        var data = sub.ToArray();
        WriteTag(fieldNumber, 2);
        WriteRawVarint((UInt64)data.Length);
        WriteRawBytes(data);
    }

    /// <summary>写入google.protobuf.Duration（seconds=1, nanos=2）</summary>
    /// <param name="fieldNumber">字段编号</param>
    /// <param name="value">时长值</param>
    public void WriteDuration(Int32 fieldNumber, TimeSpan? value)
    {
        if (value == null || value.Value == TimeSpan.Zero) return;

        var sub = new ProtoWriter();
        sub.WriteInt64(1, (Int64)value.Value.TotalSeconds);
        var nanos = (Int32)((value.Value.Ticks % TimeSpan.TicksPerSecond) * 100);
        sub.WriteInt32(2, nanos);

        var data = sub.ToArray();
        WriteTag(fieldNumber, 2);
        WriteRawVarint((UInt64)data.Length);
        WriteRawBytes(data);
    }
    #endregion

    #region 输出
    /// <summary>获取编码后的字节数组</summary>
    /// <returns></returns>
    public Byte[] ToArray() => _stream.ToArray();
    #endregion
}
