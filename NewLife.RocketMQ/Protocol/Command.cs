using System.Runtime.Serialization;
using System.Xml.Serialization;
using NewLife.Buffers;
using NewLife.Collections;
using NewLife.Data;
using NewLife.Log;
using NewLife.Messaging;
using NewLife.Serialization;

namespace NewLife.RocketMQ.Protocol;

/// <summary>命令</summary>
/// <remarks>
/// Remoting 协议帧格式（大端序）：
/// [4字节 TotalLength] [4字节 OriHeaderLength] [HeaderData] [BodyData]
/// 其中 OriHeaderLength 高8位为序列化类型，低24位为头部数据长度。
/// 使用 SpanReader/SpanWriter 进行高性能零拷贝编解码。
/// </remarks>
public class Command : IAccessor, IMessage
{
    #region 属性
    /// <summary>头部</summary>
    public Header Header { get; set; }

    /// <summary>主体</summary>
    [XmlIgnore, IgnoreDataMember]
    public IPacket Payload { get; set; }

    /// <summary>原始Json</summary>
    [XmlIgnore, IgnoreDataMember]
    public String RawJson { get; private set; }
    #endregion

    #region 扩展属性
    /// <summary>是否响应</summary>
    public Boolean Reply { get; set; }

    /// <summary>是否单向</summary>
    public Boolean OneWay { get; set; }

    /// <summary>消息体。帧层流式绑定时的限长读取器，整帧模式下为空（体在 Payload）</summary>
    [XmlIgnore, IgnoreDataMember]
    public LimitedReader Body { get; private set; }

    /// <summary>消息种类。请求/单向/响应/响应+错误，与 Reply/OneWay 双向同步</summary>
    [XmlIgnore, IgnoreDataMember]
    public MessageKinds Kind
    {
        get => Reply
            ? (Header != null && Header.Code != (Int32)ResponseCode.SUCCESS ? MessageKinds.Error : MessageKinds.Response)
            : OneWay ? MessageKinds.OneWay : MessageKinds.Request;
        set
        {
            Reply = value is MessageKinds.Response or MessageKinds.Error;
            OneWay = value == MessageKinds.OneWay;
        }
    }
    #endregion

    #region 负载
    /// <summary>设置消息体。拥有句柄所有权随消息，Dispose 时归还</summary>
    /// <param name="packet">消息体数据包；置 null 表示放弃持有（所有权已转移给调用方或构建结果），不归还</param>
    public void SetBody(IPacket packet)
    {
        // 置 null 表示放弃持有：负载已随调用方交付，消息不再持有，也不能归还
        if (packet == null)
        {
            Payload = null;
            Body = null;
            return;
        }

        var old = Payload;
        Payload = packet;
        Body = null;

        // 替换旧体时归还其拥有帧
        if (old != null && !ReferenceEquals(old, packet)) old.TryDispose();
    }

    /// <summary>绑定流式消息体（帧层专用）。头部先行解析后由帧泵调用</summary>
    /// <param name="body">限长读取器；null 表示清除流式体绑定</param>
    public void BindBody(LimitedReader body)
    {
        var old = Payload;
        Payload = null;
        Body = body;
        old?.TryDispose();
    }
    #endregion

    #region 构造

    /// <summary>销毁。回收内存</summary>
    public void Dispose()
    {
        Payload.TryDispose();
        Payload = null;
        Body = null;
    }

    #endregion

    #region 读写
    /// <summary>从数据流中读取</summary>
    /// <param name="stream">数据流</param>
    /// <param name="context">上下文</param>
    /// <returns></returns>
    public Boolean Read(Stream stream, Object context = null)
    {
        try
        {
            // 先读取8字节帧头（TotalLength + OriHeaderLength）
            var headBuf = new Byte[8];
            if (!ReadFull(stream, headBuf, 0, 8)) return false;

            var reader = new SpanReader(headBuf) { IsLittleEndian = false };
            var len = reader.ReadInt32();
            if (len < 4 || len > 4 * 1024 * 1024) return false;

            var oriHeaderLen = reader.ReadInt32();
            var headerLen = oriHeaderLen & 0xFFFFFF;
            if (headerLen <= 0 || headerLen > 8 * 1024) return false;

            // 读取剩余数据（头部 + 主体）
            var bodyLen = len - 4 - headerLen;
            var dataBuf = new Byte[headerLen + (bodyLen > 0 ? bodyLen : 0)];
            if (!ReadFull(stream, dataBuf, 0, dataBuf.Length)) return false;

            // 解析头部
            var type = (SerializeType)((oriHeaderLen >> 24) & 0xFF);
            if (!TryReadHeader(dataBuf, 0, headerLen, type)) throw new NotSupportedException($"不支持[{type}]序列化");

            // 读取主体
            if (bodyLen > 0)
                Payload = new ArrayPacket(dataBuf, headerLen, bodyLen);
        }
        catch (Exception ex)
        {
            XTrace.WriteLine("序列化错误！{0}", ex.Message);
            return false;
        }

        return true;
    }

    /// <summary>解析头部数据（不含帧头）</summary>
    /// <param name="buffer">数据缓冲区</param>
    /// <param name="offset">头部起始偏移</param>
    /// <param name="headerLength">头部数据长度</param>
    /// <param name="type">序列化类型</param>
    /// <returns>是否支持该序列化类型并解析成功</returns>
    internal Boolean TryReadHeader(Byte[] buffer, Int32 offset, Int32 headerLength, SerializeType type)
    {
        if (type == SerializeType.JSON)
        {
            var json = buffer.ToStr(null, offset, headerLength);
            RawJson = json;

            var header = json.ToJsonEntity<Header>();
            if (header.SerializeTypeCurrentRPC.IsNullOrEmpty()) header.SerializeTypeCurrentRPC = type + "";

            Header = header;
            Reply = (header.Flag & 0b01) == 0b01;
            OneWay = (header.Flag & 0b10) == 0b10;
        }
        else if (type == SerializeType.ROCKETMQ)
        {
            var rd = new SpanReader(buffer, offset, headerLength) { IsLittleEndian = false };

            var header = new Header
            {
                SerializeTypeCurrentRPC = type + "",
                Code = rd.ReadUInt16(),
                Language = ((LanguageCode)rd.ReadByte()) + "",
                Version = (MQVersion)rd.ReadUInt16(),
                Opaque = rd.ReadInt32(),
                Flag = rd.ReadInt32(),
                Remark = ReadStr(ref rd, false, headerLength),
            };

            Reply = (header.Flag & 0b01) == 0b01;
            OneWay = (header.Flag & 0b10) == 0b10;

            // 读取扩展字段
            var extFieldsLength = rd.ReadInt32();
            if (extFieldsLength > 0)
            {
                if (extFieldsLength > headerLength) throw new Exception($"扩展字段长度[{extFieldsLength}]超过头部长度[{headerLength}]");

                var extFields = header.GetExtFields();
                var endPos = rd.Position + extFieldsLength;
                while (rd.Position < endPos)
                {
                    var k = ReadStr(ref rd, true, extFieldsLength);
                    var v = ReadStr(ref rd, false, extFieldsLength);
                    extFields[k + ""] = v;
                }
            }

            Header = header;
        }
        else
            return false;

        return true;
    }

    /// <summary>从SpanReader中读取字符串</summary>
    /// <param name="reader">读取器</param>
    /// <param name="useShortLength">是否使用2字节长度前缀</param>
    /// <param name="limit">长度上限</param>
    /// <returns></returns>
    private static String ReadStr(ref SpanReader reader, Boolean useShortLength, Int32 limit)
    {
        var len = useShortLength ? reader.ReadInt16() : reader.ReadInt32();
        if (len == 0) return null;
        if (len < 0) throw new Exception($"字符串长度为负数[{len}]");
        if (len > limit) throw new Exception($"字符串长度[{len}]超过限制[{limit}]");

        return reader.ReadBytes(len).ToArray().ToStr();
    }

    /// <summary>循环读取指定长度的数据。Stream.Read 不保证一次读满</summary>
    private static Boolean ReadFull(Stream stream, Byte[] buffer, Int32 offset, Int32 count)
    {
        var totalRead = 0;
        while (totalRead < count)
        {
            var read = stream.Read(buffer, offset + totalRead, count - totalRead);
            if (read <= 0) return false;
            totalRead += read;
        }
        return true;
    }

    /// <summary>向SpanWriter写入字符串</summary>
    /// <param name="writer">写入器</param>
    /// <param name="useShortLength">是否使用2字节长度前缀</param>
    /// <param name="value">字符串值</param>
    private static void WriteStr(ref SpanWriter writer, Boolean useShortLength, String value)
    {
        var buf = value?.GetBytes();
        var len = buf?.Length ?? 0;

        if (useShortLength)
            writer.Write((Int16)len);
        else
            writer.Write(len);

        if (len > 0) writer.Write(buf);
    }

    /// <summary>读取Body作为Json返回</summary>
    /// <returns></returns>
    public IDictionary<String, Object> ReadBodyAsJson()
    {
        var pk = Payload;
        if (pk == null || pk.Total == 0) return null;

        return new JsonParser(pk.ToStr()).Decode() as IDictionary<String, Object>;
    }

    /// <summary>写入命令到数据流</summary>
    /// <param name="stream">数据流</param>
    /// <param name="context">上下文</param>
    /// <returns></returns>
    public Boolean Write(Stream stream, Object context = null)
    {
        var pk = Payload;

        // 帧头 + 头部
        WriteHeader(stream, pk?.Total ?? 0);

        // 写入主体
        if (pk != null && pk.Total > 0) pk.CopyTo(stream);

        return true;
    }

    /// <summary>写入帧头与头部数据（不含主体）</summary>
    /// <param name="stream">数据流</param>
    /// <param name="bodyLength">主体长度</param>
    internal void WriteHeader(Stream stream, Int64 bodyLength)
    {
        var header = Header;
        if (Reply) header.Flag |= 0b01;
        if (OneWay) header.Flag |= 0b10;

        // 区分不同的序列化类型
        var type = header.SerializeTypeCurrentRPC.ToEnum(SerializeType.JSON);
        if (type == SerializeType.JSON)
        {
            // 计算头部
            //var json = Header.ToJson();
            var json = Header.ToJson(false, false, false);
            RawJson = json;
            var hs = json.GetBytes();

            // 计算长度
            var len = (Int32)(4 + hs.Length + bodyLength);

            // 使用SpanWriter写入8字节帧头（TotalLength + HeaderLength）
            var prefix = new Byte[8];
            var writer = new SpanWriter(prefix) { IsLittleEndian = false };
            writer.Write(len);
            writer.Write(hs.Length);
            stream.Write(prefix, 0, 8);

            // 写入头部JSON
            stream.Write(hs, 0, hs.Length);
        }
        else if (type == SerializeType.ROCKETMQ)
        {
            // 使用重试缓冲区编码 ROCKETMQ 二进制头部，超限自动扩容（避免固定缓冲越界）
            var hsSize = 8 * 1024;
            Byte[] hsBuf;
            Int32 hsLen;
            while (true)
            {
                hsBuf = new Byte[hsSize];
                var writer = new SpanWriter(hsBuf) { IsLittleEndian = false };
                try
                {
                    writer.Write((UInt16)header.Code);
                    writer.Write((Byte)header.Language.ToEnum(LanguageCode.JAVA));
                    writer.Write((UInt16)header.Version);
                    writer.Write(header.Opaque);
                    writer.Write(header.Flag);

                    WriteStr(ref writer, false, header.Remark);

                    if (header.ExtFields != null && header.ExtFields.Count > 0)
                    {
                        // 扩展字段独立编码，超限扩容重试
                        var extSize = 4 * 1024;
                        while (true)
                        {
                            var extBuf = new Byte[extSize];
                            var extWriter = new SpanWriter(extBuf) { IsLittleEndian = false };
                            try
                            {
                                foreach (var item in header.ExtFields)
                                {
                                    WriteStr(ref extWriter, true, item.Key);
                                    WriteStr(ref extWriter, false, item.Value);
                                }

                                var extLen = extWriter.Position;
                                writer.Write(extLen);
                                if (extLen > 0) writer.Write(new ReadOnlySpan<Byte>(extBuf, 0, extLen));
                                break;
                            }
                            catch (InvalidOperationException)
                            {
                                extSize = checked(extSize * 2);
                            }
                        }
                    }
                    else
                    {
                        writer.Write(0);
                    }

                    hsLen = writer.Position;
                    break;
                }
                catch (InvalidOperationException)
                {
                    hsSize = checked(hsSize * 2);
                }
            }

            // 计算长度
            var oriHeaderLen = (hsLen & 0xFFFFFF) | ((Byte)type << 24);

            var len = (Int32)(4 + hsLen + bodyLength);

            // 使用SpanWriter写入帧头
            var prefix = new Byte[8];
            var prefixWriter = new SpanWriter(prefix) { IsLittleEndian = false };
            prefixWriter.Write(len);
            prefixWriter.Write(oriHeaderLen);
            stream.Write(prefix, 0, 8);

            // 写入头部
            stream.Write(hsBuf, 0, hsLen);
        }
        else
            throw new NotSupportedException($"不支持[{type}]序列化");
    }

    /// <summary>命令转字节数组</summary>
    /// <returns></returns>
    public IPacket ToPacket()
    {
        var ms = new MemoryStream();
        Write(ms, null);

        ms.Position = 0;
        return new ArrayPacket(ms);
    }

    /// <summary>创建响应</summary>
    /// <returns></returns>
    public IMessage CreateReply()
    {
        if (Header == null || Reply || OneWay) throw new Exception("不能创建响应命令");

        var head = new Header
        {
            Opaque = Header.Opaque,
            SerializeTypeCurrentRPC = Header.SerializeTypeCurrentRPC,
            Version = Header.Version,
        };

        var cmd = new Command
        {
            Reply = true,
            Header = head,
        };

        return cmd;
    }
    #endregion

    #region 辅助
    /// <summary>友好字符串</summary>
    /// <returns></returns>
    public override String ToString()
    {
        var h = Header;
        if (h == null) return base.ToString();

        var sb = Pool.StringBuilder.Get();
        // 请求与响应
        if (Reply)
        {
            sb.Append('#');
            sb.Append((ResponseCode)h.Code);
        }
        else
        {
            sb.Append((RequestCode)h.Code);
        }

        var pk = Payload;
        sb.AppendFormat("({0})", h.Opaque);

        var ext = h.ExtFields;
        if (ext != null && ext.Count > 0) sb.AppendFormat("<{0}>", ext.ToJson());

        if (pk != null && pk.Total > 0)
        {
            sb.AppendFormat("[{0}]", pk.Total);
            sb.Append(pk.ToStr(null, 0, 256));
        }

        return sb.Return(true);
    }
    #endregion
}