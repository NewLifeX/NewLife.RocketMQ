namespace NewLife.RocketMQ.Grpc;

/// <summary>gRPC状态</summary>
public class GrpcStatus : IProtoMessage
{
    /// <summary>状态码</summary>
    public GrpcCode Code { get; set; }

    /// <summary>状态消息</summary>
    public String Message { get; set; }

    /// <summary>写入</summary>
    /// <param name="writer">编码器</param>
    public void WriteTo(ProtoWriter writer)
    {
        writer.WriteEnum(1, (Int32)Code);
        writer.WriteString(2, Message);
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
                case 1: Code = (GrpcCode)reader.ReadEnum(); break;
                case 2: Message = reader.ReadString(); break;
                default: reader.SkipField(wt); break;
            }
        }
    }

    /// <summary>已重载</summary>
    public override String ToString() => $"{Code} {Message}";
}

/// <summary>资源描述（Topic/Group）</summary>
public class GrpcResource : IProtoMessage
{
    /// <summary>命名空间</summary>
    public String ResourceNamespace { get; set; }

    /// <summary>名称</summary>
    public String Name { get; set; }

    /// <summary>写入</summary>
    /// <param name="writer">编码器</param>
    public void WriteTo(ProtoWriter writer)
    {
        writer.WriteString(1, ResourceNamespace);
        writer.WriteString(2, Name);
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
                case 1: ResourceNamespace = reader.ReadString(); break;
                case 2: Name = reader.ReadString(); break;
                default: reader.SkipField(wt); break;
            }
        }
    }

    /// <summary>已重载</summary>
    public override String ToString() => String.IsNullOrEmpty(ResourceNamespace) ? Name : $"{ResourceNamespace}%{Name}";
}

/// <summary>地址</summary>
public class GrpcAddress : IProtoMessage
{
    /// <summary>主机</summary>
    public String Host { get; set; }

    /// <summary>端口</summary>
    public Int32 Port { get; set; }

    /// <summary>写入</summary>
    /// <param name="writer">编码器</param>
    public void WriteTo(ProtoWriter writer)
    {
        writer.WriteString(1, Host);
        writer.WriteInt32(2, Port);
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
                case 1: Host = reader.ReadString(); break;
                case 2: Port = reader.ReadInt32(); break;
                default: reader.SkipField(wt); break;
            }
        }
    }

    /// <summary>已重载</summary>
    public override String ToString() => $"{Host}:{Port}";
}

/// <summary>端点集合</summary>
public class GrpcEndpoints : IProtoMessage
{
    /// <summary>地址类型</summary>
    public AddressScheme Scheme { get; set; }

    /// <summary>地址列表</summary>
    public List<GrpcAddress> Addresses { get; set; } = [];

    /// <summary>写入</summary>
    /// <param name="writer">编码器</param>
    public void WriteTo(ProtoWriter writer)
    {
        writer.WriteEnum(1, (Int32)Scheme);
        writer.WriteRepeatedMessage(2, Addresses);
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
                case 1: Scheme = (AddressScheme)reader.ReadEnum(); break;
                case 2: Addresses.Add(reader.ReadMessage<GrpcAddress>()); break;
                default: reader.SkipField(wt); break;
            }
        }
    }
}

/// <summary>Broker信息</summary>
public class GrpcBroker : IProtoMessage
{
    /// <summary>Broker名称</summary>
    public String Name { get; set; }

    /// <summary>Broker ID</summary>
    public Int32 Id { get; set; }

    /// <summary>端点</summary>
    public GrpcEndpoints Endpoints { get; set; }

    /// <summary>写入</summary>
    /// <param name="writer">编码器</param>
    public void WriteTo(ProtoWriter writer)
    {
        writer.WriteString(1, Name);
        writer.WriteInt32(2, Id);
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
                case 1: Name = reader.ReadString(); break;
                case 2: Id = reader.ReadInt32(); break;
                case 3: Endpoints = reader.ReadMessage<GrpcEndpoints>(); break;
                default: reader.SkipField(wt); break;
            }
        }
    }

    /// <summary>已重载</summary>
    public override String ToString() => $"{Name}#{Id}";
}

/// <summary>gRPC消息队列</summary>
public class GrpcMessageQueue : IProtoMessage
{
    /// <summary>主题</summary>
    public GrpcResource Topic { get; set; }

    /// <summary>队列ID</summary>
    public Int32 Id { get; set; }

    /// <summary>权限</summary>
    public GrpcPermission Permission { get; set; }

    /// <summary>Broker</summary>
    public GrpcBroker Broker { get; set; }

    /// <summary>接受的消息类型</summary>
    public List<GrpcMessageType> AcceptMessageTypes { get; set; } = [];

    /// <summary>写入</summary>
    /// <param name="writer">编码器</param>
    public void WriteTo(ProtoWriter writer)
    {
        writer.WriteMessage(1, Topic);
        writer.WriteInt32(2, Id);
        writer.WriteEnum(3, (Int32)Permission);
        writer.WriteMessage(4, Broker);
        if (AcceptMessageTypes.Count > 0)
        {
            var enums = new List<Int32>(AcceptMessageTypes.Count);
            foreach (var t in AcceptMessageTypes)
            {
                enums.Add((Int32)t);
            }
            writer.WritePackedEnum(5, enums);
        }
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
                case 2: Id = reader.ReadInt32(); break;
                case 3: Permission = (GrpcPermission)reader.ReadEnum(); break;
                case 4: Broker = reader.ReadMessage<GrpcBroker>(); break;
                case 5:
                    if (wt == 0) // 非packed
                        AcceptMessageTypes.Add((GrpcMessageType)reader.ReadEnum());
                    else if (wt == 2) // packed
                    {
                        var data = reader.ReadBytes();
                        var sub = new ProtoReader(data);
                        while (!sub.IsEnd) AcceptMessageTypes.Add((GrpcMessageType)sub.ReadEnum());
                    }
                    break;
                default: reader.SkipField(wt); break;
            }
        }
    }
}

/// <summary>过滤表达式</summary>
public class GrpcFilterExpression : IProtoMessage
{
    /// <summary>过滤类型</summary>
    public GrpcFilterType Type { get; set; }

    /// <summary>表达式</summary>
    public String Expression { get; set; }

    /// <summary>写入</summary>
    /// <param name="writer">编码器</param>
    public void WriteTo(ProtoWriter writer)
    {
        writer.WriteEnum(1, (Int32)Type);
        writer.WriteString(2, Expression);
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
                case 1: Type = (GrpcFilterType)reader.ReadEnum(); break;
                case 2: Expression = reader.ReadString(); break;
                default: reader.SkipField(wt); break;
            }
        }
    }
}

/// <summary>摘要</summary>
public class GrpcDigest : IProtoMessage
{
    /// <summary>摘要类型</summary>
    public GrpcDigestType Type { get; set; }

    /// <summary>校验值</summary>
    public String Checksum { get; set; }

    /// <summary>写入</summary>
    /// <param name="writer">编码器</param>
    public void WriteTo(ProtoWriter writer)
    {
        writer.WriteEnum(1, (Int32)Type);
        writer.WriteString(2, Checksum);
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
                case 1: Type = (GrpcDigestType)reader.ReadEnum(); break;
                case 2: Checksum = reader.ReadString(); break;
                default: reader.SkipField(wt); break;
            }
        }
    }
}

/// <summary>消息系统属性</summary>
public class GrpcSystemProperties : IProtoMessage
{
    /// <summary>标签</summary>
    public String Tag { get; set; }

    /// <summary>Keys</summary>
    public List<String> Keys { get; set; } = [];

    /// <summary>消息ID</summary>
    public String MessageId { get; set; }

    /// <summary>消息体摘要</summary>
    public GrpcDigest BodyDigest { get; set; }

    /// <summary>消息体编码</summary>
    public GrpcEncoding BodyEncoding { get; set; }

    /// <summary>消息类型</summary>
    public GrpcMessageType MessageType { get; set; }

    /// <summary>消息出生时间</summary>
    public DateTime? BornTimestamp { get; set; }

    /// <summary>消息出生主机</summary>
    public String BornHost { get; set; }

    /// <summary>消息存储时间</summary>
    public DateTime? StoreTimestamp { get; set; }

    /// <summary>消息存储主机</summary>
    public String StoreHost { get; set; }

    /// <summary>投递时间（延迟消息）</summary>
    public DateTime? DeliveryTimestamp { get; set; }

    /// <summary>收据句柄</summary>
    public String ReceiptHandle { get; set; }

    /// <summary>队列ID</summary>
    public Int32 QueueId { get; set; }

    /// <summary>队列偏移</summary>
    public Int64 QueueOffset { get; set; }

    /// <summary>不可见时间</summary>
    public TimeSpan? InvisibleDuration { get; set; }

    /// <summary>投递尝试次数</summary>
    public Int32 DeliveryAttempt { get; set; }

    /// <summary>消息分组（FIFO）</summary>
    public String MessageGroup { get; set; }

    /// <summary>追踪上下文</summary>
    public String TraceContext { get; set; }

    /// <summary>写入</summary>
    /// <param name="writer">编码器</param>
    public void WriteTo(ProtoWriter writer)
    {
        writer.WriteString(1, Tag);
        writer.WriteRepeatedString(2, Keys);
        writer.WriteString(3, MessageId);
        writer.WriteMessage(4, BodyDigest);
        writer.WriteEnum(5, (Int32)BodyEncoding);
        writer.WriteEnum(6, (Int32)MessageType);
        writer.WriteTimestamp(7, BornTimestamp);
        writer.WriteString(8, BornHost);
        writer.WriteTimestamp(9, StoreTimestamp);
        writer.WriteString(10, StoreHost);
        writer.WriteTimestamp(11, DeliveryTimestamp);
        writer.WriteString(12, ReceiptHandle);
        writer.WriteInt32(13, QueueId);
        writer.WriteInt64(14, QueueOffset);
        writer.WriteDuration(15, InvisibleDuration);
        writer.WriteInt32(16, DeliveryAttempt);
        writer.WriteString(17, MessageGroup);
        writer.WriteString(18, TraceContext);
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
                case 1: Tag = reader.ReadString(); break;
                case 2: Keys.Add(reader.ReadString()); break;
                case 3: MessageId = reader.ReadString(); break;
                case 4: BodyDigest = reader.ReadMessage<GrpcDigest>(); break;
                case 5: BodyEncoding = (GrpcEncoding)reader.ReadEnum(); break;
                case 6: MessageType = (GrpcMessageType)reader.ReadEnum(); break;
                case 7: BornTimestamp = reader.ReadTimestamp(); break;
                case 8: BornHost = reader.ReadString(); break;
                case 9: StoreTimestamp = reader.ReadTimestamp(); break;
                case 10: StoreHost = reader.ReadString(); break;
                case 11: DeliveryTimestamp = reader.ReadTimestamp(); break;
                case 12: ReceiptHandle = reader.ReadString(); break;
                case 13: QueueId = reader.ReadInt32(); break;
                case 14: QueueOffset = reader.ReadInt64(); break;
                case 15: InvisibleDuration = reader.ReadDuration(); break;
                case 16: DeliveryAttempt = reader.ReadInt32(); break;
                case 17: MessageGroup = reader.ReadString(); break;
                case 18: TraceContext = reader.ReadString(); break;
                default: reader.SkipField(wt); break;
            }
        }
    }
}

/// <summary>gRPC消息</summary>
public class GrpcMessage : IProtoMessage
{
    /// <summary>主题</summary>
    public GrpcResource Topic { get; set; }

    /// <summary>用户属性</summary>
    public Dictionary<String, String> UserProperties { get; set; } = [];

    /// <summary>系统属性</summary>
    public GrpcSystemProperties SystemProperties { get; set; }

    /// <summary>消息体</summary>
    public Byte[] Body { get; set; }

    /// <summary>写入</summary>
    /// <param name="writer">编码器</param>
    public void WriteTo(ProtoWriter writer)
    {
        writer.WriteMessage(1, Topic);
        writer.WriteMap(2, UserProperties);
        writer.WriteMessage(3, SystemProperties);
        writer.WriteBytes(4, Body);
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
                case 2:
                    var (k, v) = reader.ReadMapEntry();
                    if (k != null) UserProperties[k] = v;
                    break;
                case 3: SystemProperties = reader.ReadMessage<GrpcSystemProperties>(); break;
                case 4: Body = reader.ReadBytes(); break;
                default: reader.SkipField(wt); break;
            }
        }
    }
}

/// <summary>队列分配</summary>
public class GrpcAssignment : IProtoMessage
{
    /// <summary>消息队列</summary>
    public GrpcMessageQueue MessageQueue { get; set; }

    /// <summary>写入</summary>
    /// <param name="writer">编码器</param>
    public void WriteTo(ProtoWriter writer) => writer.WriteMessage(1, MessageQueue);

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
                case 1: MessageQueue = reader.ReadMessage<GrpcMessageQueue>(); break;
                default: reader.SkipField(wt); break;
            }
        }
    }
}
