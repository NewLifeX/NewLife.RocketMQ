using System.Runtime.Serialization;
using System.Xml.Serialization;
using NewLife.Collections;
using NewLife.Data;
using NewLife.Log;
using NewLife.Messaging;
using NewLife.Serialization;

namespace NewLife.RocketMQ.Protocol;

/// <summary>命令</summary>
public class Command : IAccessor, IMessage
{
    #region 属性
    /// <summary>头部</summary>
    public Header Header { get; set; }

    /// <summary>主体</summary>
    [XmlIgnore, IgnoreDataMember]
    public IPacket Payload { get; set; }
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
    /// <param name="stream"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    public Boolean Read(Stream stream, Object context = null)
    {
        var bn = new Binary
        {
            Stream = stream,
            IsLittleEndian = false,
        };

        try
        {
            var len = bn.Read<Int32>();
            if (len < 4 || len > 4 * 1024 * 1024) return false;

            // 读取头部
            var oriHeaderLen = bn.Read<Int32>();
            var headerLen = oriHeaderLen & 0xFFFFFF;
            if (headerLen <= 0 || headerLen > 8 * 1024) return false;

            // 读取序列化类型
            var type = (SerializeType)((oriHeaderLen >> 24) & 0xFF);
            if (type == SerializeType.JSON)
            {
                var json = bn.ReadBytes(headerLen).ToStr();
                var header = json.ToJsonEntity<Header>();
                if (header.SerializeTypeCurrentRPC.IsNullOrEmpty()) header.SerializeTypeCurrentRPC = type + "";

                Header = header;
                Reply = (header.Flag & 0b01) == 0b01;
                OneWay = (header.Flag & 0b10) == 0b10;

                //  读取主体
                if (len > 4 + headerLen)
                {
                    Payload = (ArrayPacket)bn.ReadBytes(len - 4 - headerLen);
                }
            }
            else if (type == SerializeType.ROCKETMQ)
            {
                var header = new Header
                {
                    SerializeTypeCurrentRPC = type + "",
                    Code = bn.ReadUInt16(),
                    Language = ((LanguageCode)bn.ReadByte()) + "",
                    Version = (MQVersion)bn.ReadUInt16(),
                    Opaque = bn.ReadInt32(),
                    Flag = bn.ReadInt32(),
                    Remark = ReadStr(bn, false, headerLen),
                };

                Reply = (header.Flag & 0b01) == 0b01;
                OneWay = (header.Flag & 0b10) == 0b10;

                // 读取扩展字段
                var extFieldsLength = bn.ReadInt32();
                if (extFieldsLength > 0)
                {
                    if (extFieldsLength > headerLen) throw new Exception($"扩展字段长度[{extFieldsLength}]超过头部长度[{headerLen}]");

                    var extFields = header.GetExtFields();
                    var endIndex = stream.Position + extFieldsLength;
                    while (stream.Position < endIndex)
                    {
                        var k = ReadStr(bn, true, extFieldsLength);
                        var v = ReadStr(bn, false, extFieldsLength);
                        extFields[k + ""] = v;
                    }
                }

                Header = header;
            }
            else
                throw new NotSupportedException($"不支持[{type}]序列化");
        }
        catch
        {
            XTrace.WriteLine("序列化错误！");
            return false;
        }

        return true;
    }

    private String ReadStr(Binary bn, Boolean useShortLength, Int32 limit)
    {
        var len = useShortLength ? bn.ReadInt16() : bn.ReadInt32();
        if (len == 0) return null;
        if (len > limit) throw new Exception($"字符串长度[{len}]超过限制[{limit}]");

        return bn.ReadBytes(len).ToStr();
    }

    private void WriteStr(Binary bn, Boolean useShortLength, String value)
    {
        var buf = value?.GetBytes();
        var len = buf?.Length ?? 0;

        if (useShortLength)
            bn.Write((Int16)len);
        else
            bn.Write(len);

        if (len > 0) bn.Write(buf, 0, buf.Length);
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
    /// <param name="stream"></param>
    /// <param name="context"></param>
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
            var hs = json.GetBytes();

            // 计算长度
            var len = 4 + hs.Length;
            if (pk != null) len += pk.Total;

            // 写入总长度
            var bn = new Binary
            {
                Stream = stream,
                IsLittleEndian = false,
            };
            bn.Write(len);

            // 写入头部
            bn.Write(hs.Length);
            stream.Write(hs);
        }
        else if (type == SerializeType.ROCKETMQ)
        {
            var bn = new Binary
            {
                //Stream = stream,
                IsLittleEndian = false,
            };

            bn.WriteUInt16((UInt16)header.Code);
            bn.WriteByte((Byte)header.Language.ToEnum(LanguageCode.JAVA));
            bn.WriteUInt16((UInt16)header.Version);
            bn.WriteInt32(header.Opaque);
            bn.WriteInt32(header.Flag);

            WriteStr(bn, false, header.Remark);

            if (header.ExtFields != null)
            {
                var ext = new Binary { IsLittleEndian = false };
                foreach (var item in header.ExtFields)
                {
                    WriteStr(ext, true, item.Key);
                    WriteStr(ext, false, item.Value);
                }

                var buf = ext.GetBytes();
                bn.WriteInt32(buf.Length);
                if (buf.Length > 0) bn.Write(buf, 0, buf.Length);
            }
            else
            {
                bn.WriteInt32(0);
            }

            // 计算长度
            var hs = bn.GetBytes();
            var oriHeaderLen = (hs.Length & 0xFFFFFF) | ((Byte)type << 24);

            var len = 4 + hs.Length;
            if (pk != null) len += pk.Total;

            // 写入长度
            var prefix = new Byte[8];
            prefix.Write((UInt32)len, 0, false);
            prefix.Write((UInt32)oriHeaderLen, 4, false);
            stream.Write(prefix);

            // 写入头部
            stream.Write(hs);
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

        return sb.Put(true);
    }
    #endregion
}