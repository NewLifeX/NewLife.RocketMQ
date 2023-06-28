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
    public Packet Payload { get; set; }
    #endregion

    #region 扩展属性
    /// <summary>是否响应</summary>
    public Boolean Reply => Header != null && ((Header.Flag & 1) == 1);

    /// <summary>是否单向</summary>
    public Boolean OneWay { get; set; }

    /// <summary>是否异常</summary>
    Boolean IMessage.Error { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
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
            var hlen = bn.Read<Int32>();
            if (hlen <= 0 || hlen > 8 * 1024) return false;

            var json = bn.ReadBytes(hlen).ToStr();
            Header = json.ToJsonEntity<Header>();

            //  读取主体
            if (len > 4 + hlen)
            {
                Payload = bn.ReadBytes(len - 4 - hlen);
            }
        }
        catch
        {
            XTrace.WriteLine("序列化错误！");
            return false;
        }

        return true;
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
        // 计算头部
        //var json = Header.ToJson();
        var json = JsonWriter.ToJson(Header, false, false, false);
        var hs = json.GetBytes();
        var pk = Payload;

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

        // 写入主体
        if (pk != null && pk.Total > 0) pk.CopyTo(stream);

        return true;
    }

    /// <summary>命令转字节数组</summary>
    /// <returns></returns>
    public Packet ToPacket()
    {
        var ms = new MemoryStream();
        Write(ms, null);
        ms.Position = 0;

        return new Packet(ms);
    }

    /// <summary>创建响应</summary>
    /// <returns></returns>
    public IMessage CreateReply()
    {
        if (Header == null || Reply || Header.Flag == 2) throw new Exception("不能创建响应命令");

        var head = new Header
        {
            Flag = 1,
            Opaque = Header.Opaque,
        };

        var cmd = new Command
        {
            Header = head,
        };

        return cmd;
    }

    Boolean IMessage.Read(Packet pk) => Read(pk.GetStream());
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
        if ((h.Flag & 1) == 1)
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