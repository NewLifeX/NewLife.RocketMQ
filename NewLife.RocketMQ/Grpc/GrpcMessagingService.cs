#if NETSTANDARD2_1_OR_GREATER
using NewLife.Log;

namespace NewLife.RocketMQ.Grpc;

/// <summary>RocketMQ 5.x gRPC 消息服务客户端</summary>
/// <remarks>
/// 封装 apache.rocketmq.v2.MessagingService 的 gRPC 调用，
/// 提供路由查询、消息发送/接收、确认、事务、心跳等功能。
/// 
/// 对应 RocketMQ 5.x Proxy 的 gRPC API。
/// </remarks>
public class GrpcMessagingService : IDisposable
{
    #region 常量
    private const String ServiceName = "apache.rocketmq.v2.MessagingService";
    #endregion

    #region 属性
    /// <summary>gRPC客户端</summary>
    public GrpcClient Client { get; set; }

    /// <summary>命名空间</summary>
    public String Namespace { get; set; }

    /// <summary>日志</summary>
    public ILog Log { get; set; } = Logger.Null;

    /// <summary>性能追踪</summary>
    public ITracer Tracer { get; set; }
    #endregion

    #region 构造
    /// <summary>实例化gRPC消息服务客户端</summary>
    public GrpcMessagingService() { }

    /// <summary>实例化gRPC消息服务客户端</summary>
    /// <param name="address">Proxy地址。如 http://host:8081</param>
    public GrpcMessagingService(String address)
    {
        Client = new GrpcClient(address) { Log = Log, Tracer = Tracer };
    }

    /// <summary>释放</summary>
    public void Dispose() => Client?.Dispose();
    #endregion

    #region 路由
    /// <summary>查询主题路由</summary>
    /// <param name="topic">主题名</param>
    /// <param name="cancellationToken">取消通知</param>
    /// <returns>路由信息（消息队列列表）</returns>
    public async Task<QueryRouteResponse> QueryRouteAsync(String topic, CancellationToken cancellationToken = default)
    {
        var request = new QueryRouteRequest
        {
            Topic = new GrpcResource { ResourceNamespace = Namespace, Name = topic },
        };

        return await InvokeAsync<QueryRouteRequest, QueryRouteResponse>("QueryRoute", request, cancellationToken).ConfigureAwait(false);
    }
    #endregion

    #region 发送消息
    /// <summary>发送消息</summary>
    /// <param name="topic">主题</param>
    /// <param name="body">消息体</param>
    /// <param name="tag">标签</param>
    /// <param name="keys">消息Key列表</param>
    /// <param name="properties">用户属性</param>
    /// <param name="messageGroup">消息分组（FIFO消息）</param>
    /// <param name="deliveryTimestamp">定时投递时间（延迟消息）</param>
    /// <param name="cancellationToken">取消通知</param>
    /// <returns>发送结果</returns>
    public async Task<SendMessageResponse> SendMessageAsync(
        String topic,
        Byte[] body,
        String tag = null,
        IList<String> keys = null,
        IDictionary<String, String> properties = null,
        String messageGroup = null,
        DateTime? deliveryTimestamp = null,
        CancellationToken cancellationToken = default)
    {
        var sysProps = new GrpcSystemProperties
        {
            Tag = tag,
            MessageType = GrpcMessageType.NORMAL,
            BornTimestamp = DateTime.UtcNow,
            BornHost = Environment.MachineName,
        };

        if (keys != null && keys.Count > 0)
            sysProps.Keys = new List<String>(keys);

        // 根据参数判断消息类型
        if (!String.IsNullOrEmpty(messageGroup))
        {
            sysProps.MessageType = GrpcMessageType.FIFO;
            sysProps.MessageGroup = messageGroup;
        }
        else if (deliveryTimestamp != null)
        {
            sysProps.MessageType = GrpcMessageType.DELAY;
            sysProps.DeliveryTimestamp = deliveryTimestamp;
        }

        var msg = new GrpcMessage
        {
            Topic = new GrpcResource { ResourceNamespace = Namespace, Name = topic },
            SystemProperties = sysProps,
            Body = body,
        };

        if (properties != null)
        {
            foreach (var kv in properties)
            {
                msg.UserProperties[kv.Key] = kv.Value;
            }
        }

        var request = new SendMessageRequest();
        request.Messages.Add(msg);

        return await InvokeAsync<SendMessageRequest, SendMessageResponse>("SendMessage", request, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>发送事务消息（半消息）</summary>
    /// <param name="topic">主题</param>
    /// <param name="body">消息体</param>
    /// <param name="tag">标签</param>
    /// <param name="keys">消息Key列表</param>
    /// <param name="cancellationToken">取消通知</param>
    /// <returns>发送结果</returns>
    public async Task<SendMessageResponse> SendTransactionMessageAsync(
        String topic,
        Byte[] body,
        String tag = null,
        IList<String> keys = null,
        CancellationToken cancellationToken = default)
    {
        var sysProps = new GrpcSystemProperties
        {
            Tag = tag,
            MessageType = GrpcMessageType.TRANSACTION,
            BornTimestamp = DateTime.UtcNow,
            BornHost = Environment.MachineName,
        };

        if (keys != null && keys.Count > 0)
            sysProps.Keys = new List<String>(keys);

        var msg = new GrpcMessage
        {
            Topic = new GrpcResource { ResourceNamespace = Namespace, Name = topic },
            SystemProperties = sysProps,
            Body = body,
        };

        var request = new SendMessageRequest();
        request.Messages.Add(msg);

        return await InvokeAsync<SendMessageRequest, SendMessageResponse>("SendMessage", request, cancellationToken).ConfigureAwait(false);
    }
    #endregion

    #region 接收消息
    /// <summary>查询队列分配</summary>
    /// <param name="topic">主题</param>
    /// <param name="group">消费组</param>
    /// <param name="cancellationToken">取消通知</param>
    /// <returns>分配结果</returns>
    public async Task<QueryAssignmentResponse> QueryAssignmentAsync(String topic, String group, CancellationToken cancellationToken = default)
    {
        var request = new QueryAssignmentRequest
        {
            Topic = new GrpcResource { ResourceNamespace = Namespace, Name = topic },
            Group = new GrpcResource { ResourceNamespace = Namespace, Name = group },
        };

        return await InvokeAsync<QueryAssignmentRequest, QueryAssignmentResponse>("QueryAssignment", request, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>接收消息（Server Streaming）</summary>
    /// <param name="group">消费组</param>
    /// <param name="queue">消息队列</param>
    /// <param name="filterExpression">过滤表达式</param>
    /// <param name="batchSize">批量大小</param>
    /// <param name="invisibleDuration">不可见时间</param>
    /// <param name="longPollingTimeout">长轮询超时</param>
    /// <param name="cancellationToken">取消通知</param>
    /// <returns>消息列表</returns>
    public async Task<IList<GrpcMessage>> ReceiveMessageAsync(
        String group,
        GrpcMessageQueue queue,
        GrpcFilterExpression filterExpression = null,
        Int32 batchSize = 32,
        TimeSpan? invisibleDuration = null,
        TimeSpan? longPollingTimeout = null,
        CancellationToken cancellationToken = default)
    {
        var request = new ReceiveMessageRequest
        {
            Group = new GrpcResource { ResourceNamespace = Namespace, Name = group },
            MessageQueue = queue,
            FilterExpression = filterExpression ?? new GrpcFilterExpression { Type = GrpcFilterType.TAG, Expression = "*" },
            BatchSize = batchSize,
            InvisibleDuration = invisibleDuration ?? TimeSpan.FromSeconds(30),
            LongPollingTimeout = longPollingTimeout ?? TimeSpan.FromSeconds(20),
        };

        var writer = new ProtoWriter();
        request.WriteTo(writer);
        var requestData = writer.ToArray();

        var messages = new List<GrpcMessage>();

        await foreach (var data in Client.ServerStreamingCallAsync(ServiceName, "ReceiveMessage", requestData, cancellationToken).ConfigureAwait(false))
        {
            var response = new ReceiveMessageResponse();
            response.ReadFrom(new ProtoReader(data));

            // 检查状态
            if (response.Status != null && response.Status.Code != GrpcCode.OK)
            {
                // 消息未找到不算错误
                if (response.Status.Code == GrpcCode.MESSAGE_NOT_FOUND) break;
                throw new InvalidOperationException($"ReceiveMessage error: {response.Status}");
            }

            if (response.Message != null)
                messages.Add(response.Message);
        }

        return messages;
    }
    #endregion

    #region 确认消息
    /// <summary>确认消息</summary>
    /// <param name="topic">主题</param>
    /// <param name="group">消费组</param>
    /// <param name="entries">确认条目列表</param>
    /// <param name="cancellationToken">取消通知</param>
    /// <returns>确认结果</returns>
    public async Task<AckMessageResponse> AckMessageAsync(
        String topic,
        String group,
        IList<AckMessageEntry> entries,
        CancellationToken cancellationToken = default)
    {
        var request = new AckMessageRequest
        {
            Group = new GrpcResource { ResourceNamespace = Namespace, Name = group },
            Topic = new GrpcResource { ResourceNamespace = Namespace, Name = topic },
            Entries = new List<AckMessageEntry>(entries),
        };

        return await InvokeAsync<AckMessageRequest, AckMessageResponse>("AckMessage", request, cancellationToken).ConfigureAwait(false);
    }
    #endregion

    #region 心跳
    /// <summary>发送心跳</summary>
    /// <param name="group">消费组</param>
    /// <param name="clientType">客户端类型</param>
    /// <param name="cancellationToken">取消通知</param>
    /// <returns>心跳结果</returns>
    public async Task<HeartbeatResponse> HeartbeatAsync(String group, GrpcClientType clientType, CancellationToken cancellationToken = default)
    {
        var request = new HeartbeatRequest
        {
            Group = new GrpcResource { ResourceNamespace = Namespace, Name = group },
            ClientType = clientType,
        };

        return await InvokeAsync<HeartbeatRequest, HeartbeatResponse>("Heartbeat", request, cancellationToken).ConfigureAwait(false);
    }
    #endregion

    #region 事务
    /// <summary>结束事务</summary>
    /// <param name="topic">主题</param>
    /// <param name="messageId">消息ID</param>
    /// <param name="transactionId">事务ID</param>
    /// <param name="resolution">事务决议</param>
    /// <param name="cancellationToken">取消通知</param>
    /// <returns>结束事务结果</returns>
    public async Task<GrpcEndTransactionResponse> EndTransactionAsync(
        String topic,
        String messageId,
        String transactionId,
        GrpcTransactionResolution resolution,
        CancellationToken cancellationToken = default)
    {
        var request = new GrpcEndTransactionRequest
        {
            Topic = new GrpcResource { ResourceNamespace = Namespace, Name = topic },
            MessageId = messageId,
            TransactionId = transactionId,
            Source = GrpcTransactionSource.SOURCE_CLIENT,
            Resolution = resolution,
        };

        return await InvokeAsync<GrpcEndTransactionRequest, GrpcEndTransactionResponse>("EndTransaction", request, cancellationToken).ConfigureAwait(false);
    }
    #endregion

    #region 死信队列
    /// <summary>转发消息到死信队列</summary>
    /// <param name="topic">主题</param>
    /// <param name="group">消费组</param>
    /// <param name="receiptHandle">收据句柄</param>
    /// <param name="messageId">消息ID</param>
    /// <param name="deliveryAttempt">当前投递尝试次数</param>
    /// <param name="maxDeliveryAttempts">最大投递尝试次数</param>
    /// <param name="cancellationToken">取消通知</param>
    /// <returns></returns>
    public async Task<ForwardMessageToDeadLetterQueueResponse> ForwardToDeadLetterQueueAsync(
        String topic,
        String group,
        String receiptHandle,
        String messageId,
        Int32 deliveryAttempt,
        Int32 maxDeliveryAttempts,
        CancellationToken cancellationToken = default)
    {
        var request = new ForwardMessageToDeadLetterQueueRequest
        {
            Group = new GrpcResource { ResourceNamespace = Namespace, Name = group },
            Topic = new GrpcResource { ResourceNamespace = Namespace, Name = topic },
            ReceiptHandle = receiptHandle,
            MessageId = messageId,
            DeliveryAttempt = deliveryAttempt,
            MaxDeliveryAttempts = maxDeliveryAttempts,
        };

        return await InvokeAsync<ForwardMessageToDeadLetterQueueRequest, ForwardMessageToDeadLetterQueueResponse>(
            "ForwardMessageToDeadLetterQueue", request, cancellationToken).ConfigureAwait(false);
    }
    #endregion

    #region 修改不可见时间
    /// <summary>修改消息不可见时间</summary>
    /// <param name="topic">主题</param>
    /// <param name="group">消费组</param>
    /// <param name="receiptHandle">收据句柄</param>
    /// <param name="messageId">消息ID</param>
    /// <param name="invisibleDuration">新的不可见时间</param>
    /// <param name="cancellationToken">取消通知</param>
    /// <returns></returns>
    public async Task<ChangeInvisibleDurationResponse> ChangeInvisibleDurationAsync(
        String topic,
        String group,
        String receiptHandle,
        String messageId,
        TimeSpan invisibleDuration,
        CancellationToken cancellationToken = default)
    {
        var request = new ChangeInvisibleDurationRequest
        {
            Group = new GrpcResource { ResourceNamespace = Namespace, Name = group },
            Topic = new GrpcResource { ResourceNamespace = Namespace, Name = topic },
            ReceiptHandle = receiptHandle,
            MessageId = messageId,
            InvisibleDuration = invisibleDuration,
        };

        return await InvokeAsync<ChangeInvisibleDurationRequest, ChangeInvisibleDurationResponse>(
            "ChangeInvisibleDuration", request, cancellationToken).ConfigureAwait(false);
    }
    #endregion

    #region 通知终止
    /// <summary>通知服务端客户端即将终止</summary>
    /// <param name="group">消费组</param>
    /// <param name="cancellationToken">取消通知</param>
    /// <returns></returns>
    public async Task<NotifyClientTerminationResponse> NotifyClientTerminationAsync(String group = null, CancellationToken cancellationToken = default)
    {
        var request = new NotifyClientTerminationRequest();
        if (!String.IsNullOrEmpty(group))
            request.Group = new GrpcResource { ResourceNamespace = Namespace, Name = group };

        return await InvokeAsync<NotifyClientTerminationRequest, NotifyClientTerminationResponse>(
            "NotifyClientTermination", request, cancellationToken).ConfigureAwait(false);
    }
    #endregion

    #region 客户端资源上报
    /// <summary>上报客户端设置（Telemetry）。向Proxy上报客户端资源信息，包括设置、主题订阅等</summary>
    /// <param name="settings">客户端设置</param>
    /// <param name="cancellationToken">取消通知</param>
    /// <returns>服务端返回的Telemetry命令</returns>
    public async Task<TelemetryCommand> TelemetryAsync(GrpcSettings settings, CancellationToken cancellationToken = default)
    {
        var request = new TelemetryCommand { Settings = settings };

        return await InvokeAsync<TelemetryCommand, TelemetryCommand>("Telemetry", request, cancellationToken).ConfigureAwait(false);
    }
    #endregion

    #region 辅助
    /// <summary>通用Unary调用封装</summary>
    /// <typeparam name="TRequest">请求类型</typeparam>
    /// <typeparam name="TResponse">响应类型</typeparam>
    /// <param name="method">方法名</param>
    /// <param name="request">请求消息</param>
    /// <param name="cancellationToken">取消通知</param>
    /// <returns>响应消息</returns>
    private async Task<TResponse> InvokeAsync<TRequest, TResponse>(String method, TRequest request, CancellationToken cancellationToken)
        where TRequest : IProtoMessage
        where TResponse : IProtoMessage, new()
    {
        var writer = new ProtoWriter();
        request.WriteTo(writer);
        var requestData = writer.ToArray();

        var responseData = await Client.UnaryCallAsync(ServiceName, method, requestData, cancellationToken).ConfigureAwait(false);

        var response = new TResponse();
        if (responseData != null && responseData.Length > 0)
            response.ReadFrom(new ProtoReader(responseData));

        return response;
    }
    #endregion
}
#endif
