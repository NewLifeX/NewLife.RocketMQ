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

    /// <summary>是否异常</summary>
    Boolean IMessage.Error { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
    #endregion

    #region 构造

    /// <summary>销毁。回收内存</summary>
    public void Dispose() => Payload.TryDispose();

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
            if (stream.Read(headBuf, 0, 8) < 8) return false;

            var reader = new SpanReader(headBuf) { IsLittleEndian = false };
            var len = reader.ReadInt32();
            if (len < 4 || len > 4 * 1024 * 1024) return false;

            var oriHeaderLen = reader.ReadInt32();
            var headerLen = oriHeaderLen & 0xFFFFFF;
            if (headerLen <= 0 || headerLen > 8 * 1024) return false;

            // 读取剩余数据（头部 + 主体）
            var bodyLen = len - 4 - headerLen;
            var dataBuf = new Byte[headerLen + (bodyLen > 0 ? bodyLen : 0)];
            if (stream.Read(dataBuf, 0, dataBuf.Length) < dataBuf.Length) return false;

            // 读取序列化类型
            var type = (SerializeType)((oriHeaderLen >> 24) & 0xFF);
            if (type == SerializeType.JSON)
            {
                var json = dataBuf.ToStr(null, 0, headerLen);
                RawJson = json;

                var header = json.ToJsonEntity<Header>();
                if (header.SerializeTypeCurrentRPC.IsNullOrEmpty()) header.SerializeTypeCurrentRPC = type + "";

                Header = header;
                Reply = (header.Flag & 0b01) == 0b01;
                OneWay = (header.Flag & 0b10) == 0b10;

                // 读取主体
                if (bodyLen > 0)
                    Payload = new ArrayPacket(dataBuf, headerLen, bodyLen);
            }
            else if (type == SerializeType.ROCKETMQ)
            {
                var rd = new SpanReader(dataBuf, 0, headerLen) { IsLittleEndian = false };

                var header = new Header
                {
                    SerializeTypeCurrentRPC = type + "",
                    Code = rd.ReadUInt16(),
                    Language = ((LanguageCode)rd.ReadByte()) + "",
                    Version = (MQVersion)rd.ReadUInt16(),
                    Opaque = rd.ReadInt32(),
                    Flag = rd.ReadInt32(),
                    Remark = ReadStr(ref rd, false, headerLen),
                };

                Reply = (header.Flag & 0b01) == 0b01;
                OneWay = (header.Flag & 0b10) == 0b10;

                // 读取扩展字段
                var extFieldsLength = rd.ReadInt32();
                if (extFieldsLength > 0)
                {
                    if (extFieldsLength > headerLen) throw new Exception($"扩展字段长度[{extFieldsLength}]超过头部长度[{headerLen}]");

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

                // 读取主体
                if (bodyLen > 0)
                    Payload = new ArrayPacket(dataBuf, headerLen, bodyLen);
            }
            else
                throw new NotSupportedException($"不支持[{type}]序列化");
        }
        catch (Exception ex)
        {
            XTrace.WriteLine("序列化错误！{0}", ex.Message);
            return false;
        }

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
        if (len > limit) throw new Exception($"字符串长度[{len}]超过限制[{limit}]");

        return reader.ReadBytes(len).ToArray().ToStr();
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
        var header = Header;
        if (Reply) header.Flag |= 0b01;
        if (OneWay) header.Flag |= 0b10;
        var pk = Payload;

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
            var len = 4 + hs.Length;
            if (pk != null) len += pk.Total;

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
            // 使用SpanWriter编码ROCKETMQ二进制头部
            var hsBuf = new Byte[8 * 1024];
            var writer = new SpanWriter(hsBuf) { IsLittleEndian = false };

            writer.Write((UInt16)header.Code);
            writer.Write((Byte)header.Language.ToEnum(LanguageCode.JAVA));
            writer.Write((UInt16)header.Version);
            writer.Write(header.Opaque);
            writer.Write(header.Flag);

            WriteStr(ref writer, false, header.Remark);

            if (header.ExtFields != null && header.ExtFields.Count > 0)
            {
                // 先编码扩展字段到临时缓冲区
                var extBuf = new Byte[4 * 1024];
                var extWriter = new SpanWriter(extBuf) { IsLittleEndian = false };
                foreach (var item in header.ExtFields)
                {
                    WriteStr(ref extWriter, true, item.Key);
                    WriteStr(ref extWriter, false, item.Value);
                }

                var extLen = extWriter.Position;
                writer.Write(extLen);
                if (extLen > 0) writer.Write(new ReadOnlySpan<Byte>(extBuf, 0, extLen));
            }
            else
            {
                writer.Write(0);
            }

            // 计算长度
            var hsLen = writer.Position;
            var oriHeaderLen = (hsLen & 0xFFFFFF) | ((Byte)type << 24);

            var len = 4 + hsLen;
            if (pk != null) len += pk.Total;

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

        // 写入主体
        if (pk != null && pk.Total > 0) pk.CopyTo(stream);

        return true;
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

    Boolean IMessage.Read(IPacket pk) => Read(pk.GetStream());
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