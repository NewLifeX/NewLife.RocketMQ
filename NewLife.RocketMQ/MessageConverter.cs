#if NETSTANDARD2_1_OR_GREATER
using NewLife.RocketMQ.Grpc;
using NewLife.RocketMQ.Protocol;

namespace NewLife.RocketMQ;

/// <summary>双协议消息模型转换助手。Remoting（Message/MessageExt）与 gRPC（GrpcMessage）互转</summary>
/// <remarks>
/// NewLife.RocketMQ 同时支持 Remoting 和 gRPC 双协议，两套消息模型字段高度重合。
/// 本助手提供双向转换，避免调用方手写胶水代码。
/// </remarks>
public static class MessageConverter
{
    /// <summary>gRPC 消息转 Remoting 扩展消息</summary>
    /// <param name="msg">gRPC 消息</param>
    /// <returns>Remoting 扩展消息</returns>
    public static MessageExt ToMessageExt(this GrpcMessage msg)
    {
        if (msg == null) throw new ArgumentNullException(nameof(msg));

        var sys = msg.SystemProperties;
        var ext = new MessageExt
        {
            Topic = msg.Topic?.Name,
            Body = msg.Body,
            MsgId = sys?.MessageId,
            QueueId = sys?.QueueId ?? 0,
            QueueOffset = sys?.QueueOffset ?? 0,
            BornTimestamp = sys?.BornTimestamp?.ToUnixMilliseconds() ?? 0,
            StoreTimestamp = sys?.StoreTimestamp?.ToUnixMilliseconds() ?? 0,
            BornHost = sys?.BornHost,
            StoreHost = sys?.StoreHost,
            ReconsumeTimes = sys?.DeliveryAttempt ?? 0,
        };

        // Tags / Keys
        if (!String.IsNullOrEmpty(sys?.Tag)) ext.Tags = sys.Tag;
        if (sys?.Keys is { Count: > 0 }) ext.Keys = String.Join(",", sys.Keys);

        // 用户属性
        foreach (var kv in msg.UserProperties)
        {
            ext.Properties[kv.Key] = kv.Value;
        }

        return ext;
    }

    /// <summary>gRPC 消息转 Remoting 基础消息</summary>
    /// <param name="msg">gRPC 消息</param>
    /// <returns>Remoting 基础消息</returns>
    public static Message ToMessage(this GrpcMessage msg)
    {
        if (msg == null) throw new ArgumentNullException(nameof(msg));

        var sys = msg.SystemProperties;
        var message = new Message
        {
            Topic = msg.Topic?.Name,
            Body = msg.Body,
        };

        if (!String.IsNullOrEmpty(sys?.Tag)) message.Tags = sys.Tag;
        if (sys?.Keys is { Count: > 0 }) message.Keys = String.Join(",", sys.Keys);

        foreach (var kv in msg.UserProperties)
        {
            message.Properties[kv.Key] = kv.Value;
        }

        return message;
    }

    /// <summary>Remoting 消息转 gRPC 消息</summary>
    /// <param name="msg">Remoting 消息</param>
    /// <param name="messageType">gRPC 消息类型。默认普通消息</param>
    /// <returns>gRPC 消息</returns>
    public static GrpcMessage ToGrpcMessage(this Message msg, GrpcMessageType messageType = GrpcMessageType.NORMAL)
    {
        if (msg == null) throw new ArgumentNullException(nameof(msg));

        var sys = new GrpcSystemProperties
        {
            MessageType = messageType,
            BornTimestamp = DateTime.UtcNow,
            BornHost = NetHelper.MyIP() + "",
        };

        if (!String.IsNullOrEmpty(msg.Tags)) sys.Tag = msg.Tags;
        if (!String.IsNullOrEmpty(msg.Keys)) sys.Keys = msg.Keys.Split(',').Where(e => !String.IsNullOrEmpty(e)).ToList();

        var grpcMsg = new GrpcMessage
        {
            Topic = new GrpcResource { Name = msg.Topic },
            SystemProperties = sys,
            Body = msg.Body,
        };

        // 用户属性（排除内部系统属性）
        if (msg.Properties != null)
        {
            foreach (var kv in msg.Properties)
            {
                if (kv.Key is "TAGS" or "KEYS" or "DELAY" or "WAIT" or "UNIQ_KEY" or "REPLY_TO_CLIENT" or "CORRELATION_ID" or "MSG_TYPE" or "REQUEST_TIMEOUT") continue;
                grpcMsg.UserProperties[kv.Key] = kv.Value;
            }
        }

        return grpcMsg;
    }

    /// <summary>DateTime 转 Unix 毫秒时间戳</summary>
    private static Int64 ToUnixMilliseconds(this DateTime dt)
    {
        var utc = dt.Kind == DateTimeKind.Utc ? dt : dt.ToUniversalTime();
        return (Int64)(utc - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds;
    }
}
#endif
