namespace NewLife.RocketMQ.Grpc;

#region 路由查询
/// <summary>查询路由请求</summary>
public class QueryRouteRequest : IProtoMessage
{
    /// <summary>主题</summary>
    public GrpcResource Topic { get; set; }

    /// <summary>客户端端点</summary>
    public GrpcEndpoints Endpoints { get; set; }

    /// <summary>写入</summary>
    /// <param name="writer">编码器</param>
    public void WriteTo(ProtoWriter writer)
    {
        writer.WriteMessage(1, Topic);
        writer.WriteMessage(2, Endpoints);
    }

    /// <summary>读取</summary>
    /// <param name="reader">解码器</param>
    public void ReadFrom(ProtoReader reader)
    {
        while (!reader.IsEnd)
        {
            var (fn, wt) = reader.ReadTag();
            if (fn == 0) break;
            switch (fn)
            {
                case 1: Topic = reader.ReadMessage<GrpcResource>(); break;
                case 2: Endpoints = reader.ReadMessage<GrpcEndpoints>(); break;
                default: reader.SkipField(wt); break;
            }
        }
    }
}

/// <summary>查询路由响应</summary>
public class QueryRouteResponse : IProtoMessage
{
    /// <summary>状态</summary>
    public GrpcStatus Status { get; set; }

    /// <summary>消息队列列表</summary>
    public List<GrpcMessageQueue> MessageQueues { get; set; } = [];

    /// <summary>写入</summary>
    /// <param name="writer">编码器</param>
    public void WriteTo(ProtoWriter writer)
    {
        writer.WriteMessage(1, Status);
        writer.WriteRepeatedMessage(2, MessageQueues);
    }

    /// <summary>读取</summary>
    /// <param name="reader">解码器</param>
    public void ReadFrom(ProtoReader reader)
    {
        while (!reader.IsEnd)
        {
            var (fn, wt) = reader.ReadTag();
            if (fn == 0) break;
            switch (fn)
            {
                case 1: Status = reader.ReadMessage<GrpcStatus>(); break;
                case 2: MessageQueues.Add(reader.ReadMessage<GrpcMessageQueue>()); break;
                default: reader.SkipField(wt); break;
            }
        }
    }
}
#endregion

#region 发送消息
/// <summary>发送消息请求</summary>
public class SendMessageRequest : IProtoMessage
{
    /// <summary>消息列表</summary>
    public List<GrpcMessage> Messages { get; set; } = [];

    /// <summary>写入</summary>
    /// <param name="writer">编码器</param>
    public void WriteTo(ProtoWriter writer) => writer.WriteRepeatedMessage(1, Messages);

    /// <summary>读取</summary>
    /// <param name="reader">解码器</param>
    public void ReadFrom(ProtoReader reader)
    {
        while (!reader.IsEnd)
        {
            var (fn, wt) = reader.ReadTag();
            if (fn == 0) break;
            switch (fn)
            {
                case 1: Messages.Add(reader.ReadMessage<GrpcMessage>()); break;
                default: reader.SkipField(wt); break;
            }
        }
    }
}

/// <summary>发送结果条目</summary>
public class SendResultEntry : IProtoMessage
{
    /// <summary>状态</summary>
    public GrpcStatus Status { get; set; }

    /// <summary>消息ID</summary>
    public String MessageId { get; set; }

    /// <summary>事务ID</summary>
    public String TransactionId { get; set; }

    /// <summary>偏移量</summary>
    public Int64 Offset { get; set; }

    /// <summary>写入</summary>
    /// <param name="writer">编码器</param>
    public void WriteTo(ProtoWriter writer)
    {
        writer.WriteMessage(1, Status);
        writer.WriteString(2, MessageId);
        writer.WriteString(3, TransactionId);
        writer.WriteInt64(4, Offset);
    }

    /// <summary>读取</summary>
    /// <param name="reader">解码器</param>
    public void ReadFrom(ProtoReader reader)
    {
        while (!reader.IsEnd)
        {
            var (fn, wt) = reader.ReadTag();
            if (fn == 0) break;
            switch (fn)
            {
                case 1: Status = reader.ReadMessage<GrpcStatus>(); break;
                case 2: MessageId = reader.ReadString(); break;
                case 3: TransactionId = reader.ReadString(); break;
                case 4: Offset = reader.ReadInt64(); break;
                default: reader.SkipField(wt); break;
            }
        }
    }
}

/// <summary>发送消息响应</summary>
public class SendMessageResponse : IProtoMessage
{
    /// <summary>状态</summary>
    public GrpcStatus Status { get; set; }

    /// <summary>结果条目</summary>
    public List<SendResultEntry> Entries { get; set; } = [];

    /// <summary>写入</summary>
    /// <param name="writer">编码器</param>
    public void WriteTo(ProtoWriter writer)
    {
        writer.WriteMessage(1, Status);
        writer.WriteRepeatedMessage(2, Entries);
    }

    /// <summary>读取</summary>
    /// <param name="reader">解码器</param>
    public void ReadFrom(ProtoReader reader)
    {
        while (!reader.IsEnd)
        {
            var (fn, wt) = reader.ReadTag();
            if (fn == 0) break;
            switch (fn)
            {
                case 1: Status = reader.ReadMessage<GrpcStatus>(); break;
                case 2: Entries.Add(reader.ReadMessage<SendResultEntry>()); break;
                default: reader.SkipField(wt); break;
            }
        }
    }
}
#endregion

#region 队列分配
/// <summary>查询队列分配请求</summary>
public class QueryAssignmentRequest : IProtoMessage
{
    /// <summary>主题</summary>
    public GrpcResource Topic { get; set; }

    /// <summary>消费组</summary>
    public GrpcResource Group { get; set; }

    /// <summary>客户端端点</summary>
    public GrpcEndpoints Endpoints { get; set; }

    /// <summary>写入</summary>
    /// <param name="writer">编码器</param>
    public void WriteTo(ProtoWriter writer)
    {
        writer.WriteMessage(1, Topic);
        writer.WriteMessage(2, Group);
        writer.WriteMessage(3, Endpoints);
    }

    /// <summary>读取</summary>
    /// <param name="reader">解码器</param>
    public void ReadFrom(ProtoReader reader)
    {
        while (!reader.IsEnd)
        {
            var (fn, wt) = reader.ReadTag();
            if (fn == 0) break;
            switch (fn)
            {
                case 1: Topic = reader.ReadMessage<GrpcResource>(); break;
                case 2: Group = reader.ReadMessage<GrpcResource>(); break;
                case 3: Endpoints = reader.ReadMessage<GrpcEndpoints>(); break;
                default: reader.SkipField(wt); break;
            }
        }
    }
}

/// <summary>查询队列分配响应</summary>
public class QueryAssignmentResponse : IProtoMessage
{
    /// <summary>状态</summary>
    public GrpcStatus Status { get; set; }

    /// <summary>分配结果</summary>
    public List<GrpcAssignment> Assignments { get; set; } = [];

    /// <summary>写入</summary>
    /// <param name="writer">编码器</param>
    public void WriteTo(ProtoWriter writer)
    {
        writer.WriteMessage(1, Status);
        writer.WriteRepeatedMessage(2, Assignments);
    }

    /// <summary>读取</summary>
    /// <param name="reader">解码器</param>
    public void ReadFrom(ProtoReader reader)
    {
        while (!reader.IsEnd)
        {
            var (fn, wt) = reader.ReadTag();
            if (fn == 0) break;
            switch (fn)
            {
                case 1: Status = reader.ReadMessage<GrpcStatus>(); break;
                case 2: Assignments.Add(reader.ReadMessage<GrpcAssignment>()); break;
                default: reader.SkipField(wt); break;
            }
        }
    }
}
#endregion

#region 接收消息
/// <summary>接收消息请求（Server Streaming）</summary>
public class ReceiveMessageRequest : IProtoMessage
{
    /// <summary>消费组</summary>
    public GrpcResource Group { get; set; }

    /// <summary>消息队列</summary>
    public GrpcMessageQueue MessageQueue { get; set; }

    /// <summary>过滤表达式</summary>
    public GrpcFilterExpression FilterExpression { get; set; }

    /// <summary>批量大小</summary>
    public Int32 BatchSize { get; set; }

    /// <summary>不可见时间</summary>
    public TimeSpan? InvisibleDuration { get; set; }

    /// <summary>自动续租</summary>
    public Boolean AutoRenew { get; set; }

    /// <summary>长轮询超时</summary>
    public TimeSpan? LongPollingTimeout { get; set; }

    /// <summary>写入</summary>
    /// <param name="writer">编码器</param>
    public void WriteTo(ProtoWriter writer)
    {
        writer.WriteMessage(1, Group);
        writer.WriteMessage(2, MessageQueue);
        writer.WriteMessage(3, FilterExpression);
        writer.WriteInt32(4, BatchSize);
        writer.WriteDuration(5, InvisibleDuration);
        writer.WriteBool(6, AutoRenew);
        writer.WriteDuration(7, LongPollingTimeout);
    }

    /// <summary>读取</summary>
    /// <param name="reader">解码器</param>
    public void ReadFrom(ProtoReader reader)
    {
        while (!reader.IsEnd)
        {
            var (fn, wt) = reader.ReadTag();
            if (fn == 0) break;
            switch (fn)
            {
                case 1: Group = reader.ReadMessage<GrpcResource>(); break;
                case 2: MessageQueue = reader.ReadMessage<GrpcMessageQueue>(); break;
                case 3: FilterExpression = reader.ReadMessage<GrpcFilterExpression>(); break;
                case 4: BatchSize = reader.ReadInt32(); break;
                case 5: InvisibleDuration = reader.ReadDuration(); break;
                case 6: AutoRenew = reader.ReadBool(); break;
                case 7: LongPollingTimeout = reader.ReadDuration(); break;
                default: reader.SkipField(wt); break;
            }
        }
    }
}

/// <summary>接收消息响应（oneof: status/message/delivery_timestamp）</summary>
public class ReceiveMessageResponse : IProtoMessage
{
    /// <summary>状态（oneof content = 1）</summary>
    public GrpcStatus Status { get; set; }

    /// <summary>消息（oneof content = 2）</summary>
    public GrpcMessage Message { get; set; }

    /// <summary>投递时间戳（oneof content = 3）</summary>
    public DateTime? DeliveryTimestamp { get; set; }

    /// <summary>写入</summary>
    /// <param name="writer">编码器</param>
    public void WriteTo(ProtoWriter writer)
    {
        writer.WriteMessage(1, Status);
        writer.WriteMessage(2, Message);
        writer.WriteTimestamp(3, DeliveryTimestamp);
    }

    /// <summary>读取</summary>
    /// <param name="reader">解码器</param>
    public void ReadFrom(ProtoReader reader)
    {
        while (!reader.IsEnd)
        {
            var (fn, wt) = reader.ReadTag();
            if (fn == 0) break;
            switch (fn)
            {
                case 1: Status = reader.ReadMessage<GrpcStatus>(); break;
                case 2: Message = reader.ReadMessage<GrpcMessage>(); break;
                case 3: DeliveryTimestamp = reader.ReadTimestamp(); break;
                default: reader.SkipField(wt); break;
            }
        }
    }
}
#endregion

#region 确认消息
/// <summary>确认消息条目</summary>
public class AckMessageEntry : IProtoMessage
{
    /// <summary>消息ID</summary>
    public String MessageId { get; set; }

    /// <summary>收据句柄</summary>
    public String ReceiptHandle { get; set; }

    /// <summary>写入</summary>
    /// <param name="writer">编码器</param>
    public void WriteTo(ProtoWriter writer)
    {
        writer.WriteString(1, MessageId);
        writer.WriteString(2, ReceiptHandle);
    }

    /// <summary>读取</summary>
    /// <param name="reader">解码器</param>
    public void ReadFrom(ProtoReader reader)
    {
        while (!reader.IsEnd)
        {
            var (fn, wt) = reader.ReadTag();
            if (fn == 0) break;
            switch (fn)
            {
                case 1: MessageId = reader.ReadString(); break;
                case 2: ReceiptHandle = reader.ReadString(); break;
                default: reader.SkipField(wt); break;
            }
        }
    }
}

/// <summary>确认消息请求</summary>
public class AckMessageRequest : IProtoMessage
{
    /// <summary>消费组</summary>
    public GrpcResource Group { get; set; }

    /// <summary>主题</summary>
    public GrpcResource Topic { get; set; }

    /// <summary>确认条目</summary>
    public List<AckMessageEntry> Entries { get; set; } = [];

    /// <summary>写入</summary>
    /// <param name="writer">编码器</param>
    public void WriteTo(ProtoWriter writer)
    {
        writer.WriteMessage(1, Group);
        writer.WriteMessage(2, Topic);
        writer.WriteRepeatedMessage(3, Entries);
    }

    /// <summary>读取</summary>
    /// <param name="reader">解码器</param>
    public void ReadFrom(ProtoReader reader)
    {
        while (!reader.IsEnd)
        {
            var (fn, wt) = reader.ReadTag();
            if (fn == 0) break;
            switch (fn)
            {
                case 1: Group = reader.ReadMessage<GrpcResource>(); break;
                case 2: Topic = reader.ReadMessage<GrpcResource>(); break;
                case 3: Entries.Add(reader.ReadMessage<AckMessageEntry>()); break;
                default: reader.SkipField(wt); break;
            }
        }
    }
}

/// <summary>确认消息结果条目</summary>
public class AckMessageResultEntry : IProtoMessage
{
    /// <summary>消息ID</summary>
    public String MessageId { get; set; }

    /// <summary>收据句柄</summary>
    public String ReceiptHandle { get; set; }

    /// <summary>状态</summary>
    public GrpcStatus Status { get; set; }

    /// <summary>写入</summary>
    /// <param name="writer">编码器</param>
    public void WriteTo(ProtoWriter writer)
    {
        writer.WriteString(1, MessageId);
        writer.WriteString(2, ReceiptHandle);
        writer.WriteMessage(3, Status);
    }

    /// <summary>读取</summary>
    /// <param name="reader">解码器</param>
    public void ReadFrom(ProtoReader reader)
    {
        while (!reader.IsEnd)
        {
            var (fn, wt) = reader.ReadTag();
            if (fn == 0) break;
            switch (fn)
            {
                case 1: MessageId = reader.ReadString(); break;
                case 2: ReceiptHandle = reader.ReadString(); break;
                case 3: Status = reader.ReadMessage<GrpcStatus>(); break;
                default: reader.SkipField(wt); break;
            }
        }
    }
}

/// <summary>确认消息响应</summary>
public class AckMessageResponse : IProtoMessage
{
    /// <summary>状态</summary>
    public GrpcStatus Status { get; set; }

    /// <summary>结果条目</summary>
    public List<AckMessageResultEntry> Entries { get; set; } = [];

    /// <summary>写入</summary>
    /// <param name="writer">编码器</param>
    public void WriteTo(ProtoWriter writer)
    {
        writer.WriteMessage(1, Status);
        writer.WriteRepeatedMessage(2, Entries);
    }

    /// <summary>读取</summary>
    /// <param name="reader">解码器</param>
    public void ReadFrom(ProtoReader reader)
    {
        while (!reader.IsEnd)
        {
            var (fn, wt) = reader.ReadTag();
            if (fn == 0) break;
            switch (fn)
            {
                case 1: Status = reader.ReadMessage<GrpcStatus>(); break;
                case 2: Entries.Add(reader.ReadMessage<AckMessageResultEntry>()); break;
                default: reader.SkipField(wt); break;
            }
        }
    }
}
#endregion

#region 心跳
/// <summary>心跳请求</summary>
public class HeartbeatRequest : IProtoMessage
{
    /// <summary>消费组</summary>
    public GrpcResource Group { get; set; }

    /// <summary>客户端类型</summary>
    public GrpcClientType ClientType { get; set; }

    /// <summary>写入</summary>
    /// <param name="writer">编码器</param>
    public void WriteTo(ProtoWriter writer)
    {
        writer.WriteMessage(1, Group);
        writer.WriteEnum(2, (Int32)ClientType);
    }

    /// <summary>读取</summary>
    /// <param name="reader">解码器</param>
    public void ReadFrom(ProtoReader reader)
    {
        while (!reader.IsEnd)
        {
            var (fn, wt) = reader.ReadTag();
            if (fn == 0) break;
            switch (fn)
            {
                case 1: Group = reader.ReadMessage<GrpcResource>(); break;
                case 2: ClientType = (GrpcClientType)reader.ReadEnum(); break;
                default: reader.SkipField(wt); break;
            }
        }
    }
}

/// <summary>心跳响应</summary>
public class HeartbeatResponse : IProtoMessage
{
    /// <summary>状态</summary>
    public GrpcStatus Status { get; set; }

    /// <summary>写入</summary>
    /// <param name="writer">编码器</param>
    public void WriteTo(ProtoWriter writer) => writer.WriteMessage(1, Status);

    /// <summary>读取</summary>
    /// <param name="reader">解码器</param>
    public void ReadFrom(ProtoReader reader)
    {
        while (!reader.IsEnd)
        {
            var (fn, wt) = reader.ReadTag();
            if (fn == 0) break;
            switch (fn)
            {
                case 1: Status = reader.ReadMessage<GrpcStatus>(); break;
                default: reader.SkipField(wt); break;
            }
        }
    }
}
#endregion

#region 结束事务
/// <summary>结束事务请求</summary>
public class GrpcEndTransactionRequest : IProtoMessage
{
    /// <summary>主题</summary>
    public GrpcResource Topic { get; set; }

    /// <summary>消息ID</summary>
    public String MessageId { get; set; }

    /// <summary>事务ID</summary>
    public String TransactionId { get; set; }

    /// <summary>事务来源</summary>
    public GrpcTransactionSource Source { get; set; }

    /// <summary>事务决议</summary>
    public GrpcTransactionResolution Resolution { get; set; }

    /// <summary>写入</summary>
    /// <param name="writer">编码器</param>
    public void WriteTo(ProtoWriter writer)
    {
        writer.WriteMessage(1, Topic);
        writer.WriteString(2, MessageId);
        writer.WriteString(3, TransactionId);
        writer.WriteEnum(4, (Int32)Source);
        writer.WriteEnum(5, (Int32)Resolution);
    }

    /// <summary>读取</summary>
    /// <param name="reader">解码器</param>
    public void ReadFrom(ProtoReader reader)
    {
        while (!reader.IsEnd)
        {
            var (fn, wt) = reader.ReadTag();
            if (fn == 0) break;
            switch (fn)
            {
                case 1: Topic = reader.ReadMessage<GrpcResource>(); break;
                case 2: MessageId = reader.ReadString(); break;
                case 3: TransactionId = reader.ReadString(); break;
                case 4: Source = (GrpcTransactionSource)reader.ReadEnum(); break;
                case 5: Resolution = (GrpcTransactionResolution)reader.ReadEnum(); break;
                default: reader.SkipField(wt); break;
            }
        }
    }
}

/// <summary>结束事务响应</summary>
public class GrpcEndTransactionResponse : IProtoMessage
{
    /// <summary>状态</summary>
    public GrpcStatus Status { get; set; }

    /// <summary>写入</summary>
    /// <param name="writer">编码器</param>
    public void WriteTo(ProtoWriter writer) => writer.WriteMessage(1, Status);

    /// <summary>读取</summary>
    /// <param name="reader">解码器</param>
    public void ReadFrom(ProtoReader reader)
    {
        while (!reader.IsEnd)
        {
            var (fn, wt) = reader.ReadTag();
            if (fn == 0) break;
            switch (fn)
            {
                case 1: Status = reader.ReadMessage<GrpcStatus>(); break;
                default: reader.SkipField(wt); break;
            }
        }
    }
}
#endregion

#region 死信队列
/// <summary>转发消息到死信队列请求</summary>
public class ForwardMessageToDeadLetterQueueRequest : IProtoMessage
{
    /// <summary>消费组</summary>
    public GrpcResource Group { get; set; }

    /// <summary>主题</summary>
    public GrpcResource Topic { get; set; }

    /// <summary>收据句柄</summary>
    public String ReceiptHandle { get; set; }

    /// <summary>消息ID</summary>
    public String MessageId { get; set; }

    /// <summary>投递尝试次数</summary>
    public Int32 DeliveryAttempt { get; set; }

    /// <summary>最大投递尝试次数</summary>
    public Int32 MaxDeliveryAttempts { get; set; }

    /// <summary>写入</summary>
    /// <param name="writer">编码器</param>
    public void WriteTo(ProtoWriter writer)
    {
        writer.WriteMessage(1, Group);
        writer.WriteMessage(2, Topic);
        writer.WriteString(3, ReceiptHandle);
        writer.WriteString(4, MessageId);
        writer.WriteInt32(5, DeliveryAttempt);
        writer.WriteInt32(6, MaxDeliveryAttempts);
    }

    /// <summary>读取</summary>
    /// <param name="reader">解码器</param>
    public void ReadFrom(ProtoReader reader)
    {
        while (!reader.IsEnd)
        {
            var (fn, wt) = reader.ReadTag();
            if (fn == 0) break;
            switch (fn)
            {
                case 1: Group = reader.ReadMessage<GrpcResource>(); break;
                case 2: Topic = reader.ReadMessage<GrpcResource>(); break;
                case 3: ReceiptHandle = reader.ReadString(); break;
                case 4: MessageId = reader.ReadString(); break;
                case 5: DeliveryAttempt = reader.ReadInt32(); break;
                case 6: MaxDeliveryAttempts = reader.ReadInt32(); break;
                default: reader.SkipField(wt); break;
            }
        }
    }
}

/// <summary>转发消息到死信队列响应</summary>
public class ForwardMessageToDeadLetterQueueResponse : IProtoMessage
{
    /// <summary>状态</summary>
    public GrpcStatus Status { get; set; }

    /// <summary>写入</summary>
    /// <param name="writer">编码器</param>
    public void WriteTo(ProtoWriter writer) => writer.WriteMessage(1, Status);

    /// <summary>读取</summary>
    /// <param name="reader">解码器</param>
    public void ReadFrom(ProtoReader reader)
    {
        while (!reader.IsEnd)
        {
            var (fn, wt) = reader.ReadTag();
            if (fn == 0) break;
            switch (fn)
            {
                case 1: Status = reader.ReadMessage<GrpcStatus>(); break;
                default: reader.SkipField(wt); break;
            }
        }
    }
}
#endregion

#region 修改不可见时间
/// <summary>修改不可见时间请求</summary>
public class ChangeInvisibleDurationRequest : IProtoMessage
{
    /// <summary>消费组</summary>
    public GrpcResource Group { get; set; }

    /// <summary>主题</summary>
    public GrpcResource Topic { get; set; }

    /// <summary>收据句柄</summary>
    public String ReceiptHandle { get; set; }

    /// <summary>不可见时间</summary>
    public TimeSpan? InvisibleDuration { get; set; }

    /// <summary>消息ID</summary>
    public String MessageId { get; set; }

    /// <summary>写入</summary>
    /// <param name="writer">编码器</param>
    public void WriteTo(ProtoWriter writer)
    {
        writer.WriteMessage(1, Group);
        writer.WriteMessage(2, Topic);
        writer.WriteString(3, ReceiptHandle);
        writer.WriteDuration(4, InvisibleDuration);
        writer.WriteString(5, MessageId);
    }

    /// <summary>读取</summary>
    /// <param name="reader">解码器</param>
    public void ReadFrom(ProtoReader reader)
    {
        while (!reader.IsEnd)
        {
            var (fn, wt) = reader.ReadTag();
            if (fn == 0) break;
            switch (fn)
            {
                case 1: Group = reader.ReadMessage<GrpcResource>(); break;
                case 2: Topic = reader.ReadMessage<GrpcResource>(); break;
                case 3: ReceiptHandle = reader.ReadString(); break;
                case 4: InvisibleDuration = reader.ReadDuration(); break;
                case 5: MessageId = reader.ReadString(); break;
                default: reader.SkipField(wt); break;
            }
        }
    }
}

/// <summary>修改不可见时间响应</summary>
public class ChangeInvisibleDurationResponse : IProtoMessage
{
    /// <summary>状态</summary>
    public GrpcStatus Status { get; set; }

    /// <summary>新收据句柄</summary>
    public String ReceiptHandle { get; set; }

    /// <summary>写入</summary>
    /// <param name="writer">编码器</param>
    public void WriteTo(ProtoWriter writer)
    {
        writer.WriteMessage(1, Status);
        writer.WriteString(2, ReceiptHandle);
    }

    /// <summary>读取</summary>
    /// <param name="reader">解码器</param>
    public void ReadFrom(ProtoReader reader)
    {
        while (!reader.IsEnd)
        {
            var (fn, wt) = reader.ReadTag();
            if (fn == 0) break;
            switch (fn)
            {
                case 1: Status = reader.ReadMessage<GrpcStatus>(); break;
                case 2: ReceiptHandle = reader.ReadString(); break;
                default: reader.SkipField(wt); break;
            }
        }
    }
}
#endregion

#region 通知终止
/// <summary>通知客户端终止请求</summary>
public class NotifyClientTerminationRequest : IProtoMessage
{
    /// <summary>消费组</summary>
    public GrpcResource Group { get; set; }

    /// <summary>写入</summary>
    /// <param name="writer">编码器</param>
    public void WriteTo(ProtoWriter writer) => writer.WriteMessage(1, Group);

    /// <summary>读取</summary>
    /// <param name="reader">解码器</param>
    public void ReadFrom(ProtoReader reader)
    {
        while (!reader.IsEnd)
        {
            var (fn, wt) = reader.ReadTag();
            if (fn == 0) break;
            switch (fn)
            {
                case 1: Group = reader.ReadMessage<GrpcResource>(); break;
                default: reader.SkipField(wt); break;
            }
        }
    }
}

/// <summary>通知客户端终止响应</summary>
public class NotifyClientTerminationResponse : IProtoMessage
{
    /// <summary>状态</summary>
    public GrpcStatus Status { get; set; }

    /// <summary>写入</summary>
    /// <param name="writer">编码器</param>
    public void WriteTo(ProtoWriter writer) => writer.WriteMessage(1, Status);

    /// <summary>读取</summary>
    /// <param name="reader">解码器</param>
    public void ReadFrom(ProtoReader reader)
    {
        while (!reader.IsEnd)
        {
            var (fn, wt) = reader.ReadTag();
            if (fn == 0) break;
            switch (fn)
            {
                case 1: Status = reader.ReadMessage<GrpcStatus>(); break;
                default: reader.SkipField(wt); break;
            }
        }
    }
}
#endregion

#region 客户端资源上报（Telemetry）
/// <summary>Telemetry命令。客户端向Proxy上报资源信息（设置、主题订阅等）</summary>
public class TelemetryCommand : IProtoMessage
{
    /// <summary>状态</summary>
    public GrpcStatus Status { get; set; }

    /// <summary>客户端设置</summary>
    public GrpcSettings Settings { get; set; }

    /// <summary>写入</summary>
    /// <param name="writer">编码器</param>
    public void WriteTo(ProtoWriter writer)
    {
        writer.WriteMessage(1, Status);
        writer.WriteMessage(2, Settings);
    }

    /// <summary>读取</summary>
    /// <param name="reader">解码器</param>
    public void ReadFrom(ProtoReader reader)
    {
        while (!reader.IsEnd)
        {
            var (fn, wt) = reader.ReadTag();
            if (fn == 0) break;
            switch (fn)
            {
                case 1: Status = reader.ReadMessage<GrpcStatus>(); break;
                case 2: Settings = reader.ReadMessage<GrpcSettings>(); break;
                default: reader.SkipField(wt); break;
            }
        }
    }
}

/// <summary>gRPC客户端设置。用于Telemetry上报客户端配置信息</summary>
public class GrpcSettings : IProtoMessage
{
    /// <summary>客户端类型</summary>
    public GrpcClientType ClientType { get; set; }

    /// <summary>访问点</summary>
    public GrpcEndpoints AccessPoint { get; set; }

    /// <summary>请求超时（Duration，秒）</summary>
    public TimeSpan? RequestTimeout { get; set; }

    /// <summary>发布设置（生产者）</summary>
    public GrpcPublishingSettings Publishing { get; set; }

    /// <summary>订阅设置（消费者）</summary>
    public GrpcSubscriptionSettings Subscription { get; set; }

    /// <summary>写入</summary>
    /// <param name="writer">编码器</param>
    public void WriteTo(ProtoWriter writer)
    {
        writer.WriteEnum(1, (Int32)ClientType);
        writer.WriteMessage(2, AccessPoint);
        if (RequestTimeout != null) writer.WriteDuration(3, RequestTimeout.Value);
        writer.WriteMessage(4, Publishing);
        writer.WriteMessage(5, Subscription);
    }

    /// <summary>读取</summary>
    /// <param name="reader">解码器</param>
    public void ReadFrom(ProtoReader reader)
    {
        while (!reader.IsEnd)
        {
            var (fn, wt) = reader.ReadTag();
            if (fn == 0) break;
            switch (fn)
            {
                case 1: ClientType = (GrpcClientType)reader.ReadEnum(); break;
                case 2: AccessPoint = reader.ReadMessage<GrpcEndpoints>(); break;
                case 3: RequestTimeout = reader.ReadDuration(); break;
                case 4: Publishing = reader.ReadMessage<GrpcPublishingSettings>(); break;
                case 5: Subscription = reader.ReadMessage<GrpcSubscriptionSettings>(); break;
                default: reader.SkipField(wt); break;
            }
        }
    }
}

/// <summary>发布设置</summary>
public class GrpcPublishingSettings : IProtoMessage
{
    /// <summary>发布主题列表</summary>
    public List<GrpcResource> Topics { get; set; } = [];

    /// <summary>写入</summary>
    /// <param name="writer">编码器</param>
    public void WriteTo(ProtoWriter writer) => writer.WriteRepeatedMessage(1, Topics);

    /// <summary>读取</summary>
    /// <param name="reader">解码器</param>
    public void ReadFrom(ProtoReader reader)
    {
        while (!reader.IsEnd)
        {
            var (fn, wt) = reader.ReadTag();
            if (fn == 0) break;
            switch (fn)
            {
                case 1: Topics.Add(reader.ReadMessage<GrpcResource>()); break;
                default: reader.SkipField(wt); break;
            }
        }
    }
}

/// <summary>订阅设置</summary>
public class GrpcSubscriptionSettings : IProtoMessage
{
    /// <summary>消费组</summary>
    public GrpcResource Group { get; set; }

    /// <summary>订阅列表</summary>
    public List<GrpcSubscriptionEntry> Subscriptions { get; set; } = [];

    /// <summary>写入</summary>
    /// <param name="writer">编码器</param>
    public void WriteTo(ProtoWriter writer)
    {
        writer.WriteMessage(1, Group);
        writer.WriteRepeatedMessage(2, Subscriptions);
    }

    /// <summary>读取</summary>
    /// <param name="reader">解码器</param>
    public void ReadFrom(ProtoReader reader)
    {
        while (!reader.IsEnd)
        {
            var (fn, wt) = reader.ReadTag();
            if (fn == 0) break;
            switch (fn)
            {
                case 1: Group = reader.ReadMessage<GrpcResource>(); break;
                case 2: Subscriptions.Add(reader.ReadMessage<GrpcSubscriptionEntry>()); break;
                default: reader.SkipField(wt); break;
            }
        }
    }
}

/// <summary>订阅条目</summary>
public class GrpcSubscriptionEntry : IProtoMessage
{
    /// <summary>主题</summary>
    public GrpcResource Topic { get; set; }

    /// <summary>过滤表达式</summary>
    public GrpcFilterExpression Expression { get; set; }

    /// <summary>写入</summary>
    /// <param name="writer">编码器</param>
    public void WriteTo(ProtoWriter writer)
    {
        writer.WriteMessage(1, Topic);
        writer.WriteMessage(2, Expression);
    }

    /// <summary>读取</summary>
    /// <param name="reader">解码器</param>
    public void ReadFrom(ProtoReader reader)
    {
        while (!reader.IsEnd)
        {
            var (fn, wt) = reader.ReadTag();
            if (fn == 0) break;
            switch (fn)
            {
                case 1: Topic = reader.ReadMessage<GrpcResource>(); break;
                case 2: Expression = reader.ReadMessage<GrpcFilterExpression>(); break;
                default: reader.SkipField(wt); break;
            }
        }
    }
}
#endregion
