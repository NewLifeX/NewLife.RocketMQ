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

        // SysFlag第2位(0x04)标识IPv6地址。IPv4=4字节，IPv6=16字节
        var isIPv6 = (SysFlag & 4) != 0;
        var ipLen = isIPv6 ? 16 : 4;

        BornTimestamp = bn.Read<Int64>();
        var buf = bn.ReadBytes(ipLen);
        var ip = new IPAddress(buf);
        var port = bn.Read<Int32>();
        BornHost = $"{ip}:{port}";

        StoreTimestamp = bn.Read<Int64>();
        var buf2 = bn.ReadBytes(ipLen);
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
        ParseProperties(str);

        // MsgId：IPv4为16字节(4+4+8)，IPv6为28字节(16+4+8)
        var ms = Pool.MemoryStream.Get();
        ms.Write(buf);
        ms.Write(port.GetBytes(false));
        ms.Write(CommitLogOffset.GetBytes(false));

        var idLen = isIPv6 ? 28 : 16;
        MsgId = ms.Return(true).ToHex(0, idLen);

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

            // SysFlag第4位(0x10)标识批量消息，Body内嵌多条子消息
            if ((msg.SysFlag & 0x10) != 0 && msg.Body != null && msg.Body.Length > 0)
            {
                var batches = DecodeBatch(msg);
                list.AddRange(batches);
            }
            else
            {
                list.Add(msg);
            }
        }

        return list;
    }

    /// <summary>解码批量消息。BatchMessage 的 Body 内嵌多条子消息</summary>
    /// <param name="parent">父消息，批量消息的外层容器</param>
    /// <returns></returns>
    public static IList<MessageExt> DecodeBatch(MessageExt parent)
    {
        if (parent == null) throw new ArgumentNullException(nameof(parent));
        if (parent.Body == null || parent.Body.Length == 0) return [];

        var list = new List<MessageExt>();
        var ms = new MemoryStream(parent.Body);
        var bn = new Binary { Stream = ms, IsLittleEndian = false };

        while (ms.Position < ms.Length)
        {
            try
            {
                // 批量消息内部格式：
                // 4字节 TotalSize（含自身）
                // 4字节 MagicCode
                // 4字节 BodyCRC
                // 4字节 Flag
                // 4字节 Body长度 + Body
                // 1字节 Topic长度 + Topic
                // 2字节 Properties长度 + Properties
                var totalSize = bn.Read<Int32>();
                if (totalSize <= 0) break;

                var magicCode = bn.Read<Int32>();
                var bodyCrc = bn.Read<Int32>();
                var flag = bn.Read<Int32>();

                var bodyLen = bn.Read<Int32>();
                var body = bn.ReadBytes(bodyLen);

                var topicLen = bn.Read<Byte>();
                var topic = bn.ReadBytes(topicLen).ToStr();

                var propsLen = bn.Read<Int16>();
                var propsStr = propsLen > 0 ? bn.ReadBytes(propsLen).ToStr() : "";

                var sub = new MessageExt
                {
                    // 从父消息继承上下文信息
                    QueueId = parent.QueueId,
                    CommitLogOffset = parent.CommitLogOffset,
                    SysFlag = parent.SysFlag & ~0x10, // 清除批量标志
                    BornTimestamp = parent.BornTimestamp,
                    BornHost = parent.BornHost,
                    StoreTimestamp = parent.StoreTimestamp,
                    StoreHost = parent.StoreHost,

                    // 子消息自身信息
                    StoreSize = totalSize,
                    BodyCRC = bodyCrc,
                    Flag = flag,
                    Body = body,
                    Topic = topic,
                };
                sub.ParseProperties(propsStr);

                // 使用 UNIQ_KEY 作为 MsgId（如果存在）
                sub.MsgId = sub.TransactionId ?? parent.MsgId;

                list.Add(sub);
            }
            catch
            {
                break;
            }
        }

        return list;
    }

    /// <summary>写入命令到数据流</summary>
    /// <param name="stream"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    public Boolean Write(Stream stream, Object context = null) => true;
    #endregion

    #region 5.x MessageId
    /// <summary>创建5.x格式的MessageId。格式：01{VERSION}{MAC_HEX}{PID_HEX}{COUNTER_HEX}，共34个十六进制字符</summary>
    /// <param name="version">版本号，默认1</param>
    /// <param name="macBytes">MAC地址字节数组（6字节），为空时使用随机字节</param>
    /// <param name="processId">进程ID</param>
    /// <param name="counter">消息计数器</param>
    /// <returns>5.x格式的MessageId（34字符十六进制字符串）</returns>
    public static String CreateMessageId5x(Byte version, Byte[] macBytes, Int32 processId, Int32 counter)
    {
        // 格式：01 + 1字节Version + 6字节MAC + 4字节PID + 4字节Counter = 16字节 = 32 hex + 前缀"01" = 34 hex
        var ms = Pool.MemoryStream.Get();

        // 固定前缀 01
        ms.WriteByte(0x01);

        // 版本
        ms.WriteByte(version);

        // MAC地址（6字节）
        if (macBytes == null || macBytes.Length < 6)
        {
            var rand = new Byte[6];
            new Random().NextBytes(rand);
            ms.Write(rand, 0, 6);
        }
        else
        {
            ms.Write(macBytes, 0, 6);
        }

        // 进程ID（4字节，大端序）
        ms.Write(processId.GetBytes(false), 0, 4);

        // 计数器（4字节，大端序）
        ms.Write(counter.GetBytes(false), 0, 4);

        return ms.Return(true).ToHex(0, 16);
    }

    /// <summary>尝试解析5.x格式的MessageId</summary>
    /// <param name="messageId">MessageId字符串</param>
    /// <param name="version">解析出的版本号</param>
    /// <param name="macBytes">解析出的MAC地址</param>
    /// <param name="processId">解析出的进程ID</param>
    /// <param name="counter">解析出的计数器</param>
    /// <returns>是否为有效的5.x格式MessageId</returns>
    public static Boolean TryParseMessageId5x(String messageId, out Byte version, out Byte[] macBytes, out Int32 processId, out Int32 counter)
    {
        version = 0;
        macBytes = null;
        processId = 0;
        counter = 0;

        if (String.IsNullOrEmpty(messageId) || messageId.Length != 32) return false;

        // 前两个hex字符必须为"01"
        if (!messageId.StartsWith("01", StringComparison.OrdinalIgnoreCase)) return false;

        try
        {
            var bytes = messageId.ToHex();
            if (bytes == null || bytes.Length != 16) return false;

            // bytes[0] = 0x01（前缀）
            version = bytes[1];
            macBytes = new Byte[6];
            Array.Copy(bytes, 2, macBytes, 0, 6);

            // 大端序
            processId = (bytes[8] << 24) | (bytes[9] << 16) | (bytes[10] << 8) | bytes[11];
            counter = (bytes[12] << 24) | (bytes[13] << 16) | (bytes[14] << 8) | bytes[15];

            return true;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>判断是否为5.x格式的MessageId</summary>
    /// <param name="messageId">MessageId字符串</param>
    /// <returns>是否为5.x格式</returns>
    public static Boolean IsMessageId5x(String messageId)
    {
        if (String.IsNullOrEmpty(messageId)) return false;

        // 5.x格式为32个十六进制字符，前缀为"01"
        return messageId.Length == 32 && messageId.StartsWith("01", StringComparison.OrdinalIgnoreCase);
    }
    #endregion
}