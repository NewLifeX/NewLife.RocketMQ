using System.Net;
using NewLife.Collections;
using NewLife.Data;
using NewLife.Serialization;

namespace NewLife.RocketMQ.Protocol;

/// <summary>消息扩展</summary>
public class MessageExt : Message, IAccessor
{
    #region 属性
    /// <summary>队列编号</summary>
    public Int32 QueueId { get; set; }

    /// <summary>存储大小</summary>
    public Int32 StoreSize { get; set; }

    /// <summary>CRC校验</summary>
    public Int32 BodyCRC { get; set; }

    /// <summary>队列偏移</summary>
    public Int64 QueueOffset { get; set; }

    /// <summary>提交日志偏移</summary>
    public Int64 CommitLogOffset { get; set; }

    /// <summary>系统标记</summary>
    public Int32 SysFlag { get; set; }

    /// <summary>生产时间</summary>
    public Int64 BornTimestamp { get; set; }

    /// <summary>生产主机</summary>
    public String BornHost { get; set; }

    /// <summary>存储时间</summary>
    public Int64 StoreTimestamp { get; set; }

    /// <summary>存储主机</summary>
    public String StoreHost { get; set; }

    /// <summary>重新消费次数</summary>
    public Int32 ReconsumeTimes { get; set; }

    /// <summary>准备事务偏移</summary>
    public Int64 PreparedTransactionOffset { get; set; }

    /// <summary>属性</summary>
    public IDictionary<String, String> Properties { get; set; }

    /// <summary>消息编号</summary>
    public String MsgId { get; set; }
    #endregion

    #region 构造
    /// <summary>友好字符串</summary>
    /// <returns></returns>
    public override String ToString() => $"[{CommitLogOffset}]{base.ToString()}";
    #endregion

    #region 读写
    /// <summary>从数据流中读取</summary>
    /// <param name="stream"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    public Boolean Read(Stream stream, Object context = null)
    {
        var bn = context as Binary;

        // 读取
        StoreSize = bn.Read<Int32>();
        if (StoreSize <= 0) return false;

        var n = bn.Read<Int32>();
        BodyCRC = bn.Read<Int32>();
        QueueId = bn.Read<Int32>();
        Flag = bn.Read<Int32>();
        QueueOffset = bn.Read<Int64>();
        CommitLogOffset = bn.Read<Int64>();
        SysFlag = bn.Read<Int32>();

        BornTimestamp = bn.Read<Int64>();
        var buf = bn.ReadBytes(4);
        var ip = new IPAddress(buf);
        var port = bn.Read<Int32>();
        BornHost = $"{ip}:{port}";

        StoreTimestamp = bn.Read<Int64>();
        var buf2 = bn.ReadBytes(4);
        var ip2 = new IPAddress(buf2);
        var port2 = bn.Read<Int32>();
        StoreHost = $"{ip2}:{port2}";

        ReconsumeTimes = bn.Read<Int32>();
        PreparedTransactionOffset = bn.Read<Int64>();

        // 主体
        var len = bn.Read<Int32>();
        Body = bn.ReadBytes(len);
        if ((SysFlag & 1) == 1)
        {
            /*uncompress*/
            // ZLIB格式RFC1950，要去掉头部两个字节
            Body = Body.ReadBytes(2, -1).Decompress();
            //var gs = new MemoryStream(Body);
            //Body = gs.DecompressGZip().ReadBytes();
        }

        // 主题
        len = bn.Read<Byte>();
        Topic = bn.ReadBytes(len).ToStr();

        var len2 = bn.Read<Int16>();
        var str = bn.ReadBytes(len2).ToStr();
        var dic = ParseProperties(str);
        if (dic != null && dic.Count > 0) Properties = dic;

        // MsgId
        var ms = Pool.MemoryStream.Get();
        ms.Write(buf);
        ms.Write(port.GetBytes(false));
        ms.Write(CommitLogOffset.GetBytes(false));

        MsgId = ms.Put(true).ToHex(0, 16);

        return true;
    }

    /// <summary>读取所有消息</summary>
    /// <param name="body"></param>
    /// <returns></returns>
    public static IList<MessageExt> ReadAll(IPacket body)
    {
        //var ms = new MemoryStream(body);
        var ms = body.GetStream();
        var bn = new Binary
        {
            Stream = ms,
            IsLittleEndian = false,
        };

        var list = new List<MessageExt>();
        while (ms.Position < ms.Length)
        {
            var msg = new MessageExt();
            if (!msg.Read(ms, bn)) break;

            list.Add(msg);
        }

        return list;
    }

    /// <summary>写入命令到数据流</summary>
    /// <param name="stream"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    public Boolean Write(Stream stream, Object context = null) => true;
    #endregion
}