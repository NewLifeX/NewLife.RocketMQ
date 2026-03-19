using System.Net;
using NewLife.Buffers;
using NewLife.Collections;
using NewLife.Data;
using NewLife.Serialization;

namespace NewLife.RocketMQ.Protocol;

/// <summary>消息扩展</summary>
/// <remarks>
/// RocketMQ 消息二进制格式（大端序）：
/// StoreSize(4) + MagicCode(4) + BodyCRC(4) + QueueId(4) + Flag(4) +
/// QueueOffset(8) + CommitLogOffset(8) + SysFlag(4) + BornTimestamp(8) +
/// BornHost(4/16+4) + StoreTimestamp(8) + StoreHost(4/16+4) +
/// ReconsumeTimes(4) + PreparedTransactionOffset(8) +
/// BodyLength(4) + Body + TopicLength(1) + Topic +
/// PropertiesLength(2) + Properties
/// 使用 SpanReader 进行高性能解码。
/// </remarks>
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

    /// <summary>Pop检查点信息。Pop消费模式下由Broker在消息属性中返回，Ack/ChangeInvisibleTime操作时需传入此值</summary>
    public String PopCheckPoint
    {
        get => Properties.TryGetValue("POP_CK", out var str) ? str : null;
        set => Properties["POP_CK"] = value;
    }
    #endregion

    #region 构造
    /// <summary>友好字符串</summary>
    /// <returns></returns>
    public override String ToString() => $"[{CommitLogOffset}]{base.ToString()}";
    #endregion

    #region 读写
    /// <summary>从SpanReader中读取消息</summary>
    /// <param name="reader">SpanReader引用</param>
    /// <returns></returns>
    public Boolean Read(ref SpanReader reader)
    {
        // 读取
        StoreSize = reader.ReadInt32();
        if (StoreSize <= 0) return false;

        var n = reader.ReadInt32(); // MagicCode
        BodyCRC = reader.ReadInt32();
        QueueId = reader.ReadInt32();
        Flag = reader.ReadInt32();
        QueueOffset = reader.ReadInt64();
        CommitLogOffset = reader.ReadInt64();
        SysFlag = reader.ReadInt32();

        // SysFlag第2位(0x04)标识IPv6地址。IPv4=4字节，IPv6=16字节
        var isIPv6 = (SysFlag & 4) != 0;
        var ipLen = isIPv6 ? 16 : 4;

        BornTimestamp = reader.ReadInt64();
        var buf = reader.ReadBytes(ipLen).ToArray();
        var ip = new IPAddress(buf);
        var port = reader.ReadInt32();
        BornHost = $"{ip}:{port}";

        StoreTimestamp = reader.ReadInt64();
        var buf2 = reader.ReadBytes(ipLen).ToArray();
        var ip2 = new IPAddress(buf2);
        var port2 = reader.ReadInt32();
        StoreHost = $"{ip2}:{port2}";

        ReconsumeTimes = reader.ReadInt32();
        PreparedTransactionOffset = reader.ReadInt64();

        // 主体
        var len = reader.ReadInt32();
        Body = reader.ReadBytes(len).ToArray();
        if ((SysFlag & 1) == 1)
        {
            /*uncompress*/
            // ZLIB格式RFC1950，要去掉头部两个字节
            Body = Body.ReadBytes(2, -1).Decompress();
            //var gs = new MemoryStream(Body);
            //Body = gs.DecompressGZip().ReadBytes();
        }

        // 主题
        len = reader.ReadByte();
        Topic = reader.ReadBytes(len).ToArray().ToStr();

        var len2 = reader.ReadInt16();
        var str = reader.ReadBytes(len2).ToArray().ToStr();
        ParseProperties(str);

        // MsgId：IPv4为16字节(4+4+8)，IPv6为28字节(16+4+8)
        var idLen = isIPv6 ? 28 : 16;
        var idBuf = new Byte[idLen];
        var idWriter = new SpanWriter(idBuf) { IsLittleEndian = false };
        idWriter.Write(buf);
        idWriter.Write(port);
        idWriter.Write(CommitLogOffset);
        MsgId = idBuf.ToHex(0, idLen);

        return true;
    }

    /// <summary>从数据流中读取（向后兼容）</summary>
    /// <param name="stream">数据流</param>
    /// <param name="context">上下文</param>
    /// <returns></returns>
    public Boolean Read(Stream stream, Object context = null)
    {
        // 读取剩余数据到缓冲区
        var remaining = (Int32)(stream.Length - stream.Position);
        if (remaining <= 0) return false;

        var buf = new Byte[remaining];
        var n = stream.Read(buf, 0, remaining);

        var reader = new SpanReader(buf, 0, n) { IsLittleEndian = false };
        var rs = Read(ref reader);

        // 将流位置设置到实际读取位置
        stream.Position = stream.Length - remaining + reader.Position;

        return rs;
    }

    /// <summary>读取所有消息</summary>
    /// <param name="body">消息数据包</param>
    /// <returns></returns>
    public static IList<MessageExt> ReadAll(IPacket body)
    {
        var buf = body.ReadBytes();
        var reader = new SpanReader(buf) { IsLittleEndian = false };

        var list = new List<MessageExt>();
        while (reader.FreeCapacity > 0)
        {
            var msg = new MessageExt();
            if (!msg.Read(ref reader)) break;

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
        var reader = new SpanReader(parent.Body) { IsLittleEndian = false };

        while (reader.FreeCapacity > 0)
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
                var totalSize = reader.ReadInt32();
                if (totalSize <= 0) break;

                var magicCode = reader.ReadInt32();
                var bodyCrc = reader.ReadInt32();
                var flag = reader.ReadInt32();

                var bodyLen = reader.ReadInt32();
                var body = reader.ReadBytes(bodyLen).ToArray();

                var topicLen = reader.ReadByte();
                var topic = reader.ReadBytes(topicLen).ToArray().ToStr();

                var propsLen = reader.ReadInt16();
                var propsStr = propsLen > 0 ? reader.ReadBytes(propsLen).ToArray().ToStr() : "";

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
    /// <param name="stream">数据流</param>
    /// <param name="context">上下文</param>
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
        var buf = new Byte[16];
        var writer = new SpanWriter(buf) { IsLittleEndian = false };

        // 固定前缀 01
        writer.Write((Byte)0x01);

        // 版本
        writer.Write(version);

        // MAC地址（6字节）
        if (macBytes == null || macBytes.Length < 6)
        {
            var rand = new Byte[6];
            new Random().NextBytes(rand);
            writer.Write(rand);
        }
        else
        {
            writer.Write(new ReadOnlySpan<Byte>(macBytes, 0, 6));
        }

        // 进程ID（4字节，大端序）
        writer.Write(processId);

        // 计数器（4字节，大端序）
        writer.Write(counter);

        return buf.ToHex(0, 16);
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

            var reader = new SpanReader(bytes) { IsLittleEndian = false };

            // bytes[0] = 0x01（前缀），跳过
            reader.ReadByte();
            version = reader.ReadByte();
            macBytes = reader.ReadBytes(6).ToArray();

            // 大端序
            processId = reader.ReadInt32();
            counter = reader.ReadInt32();

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