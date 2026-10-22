using System.Diagnostics.CodeAnalysis;
using NewLife.Collections;
using NewLife.Data;
using NewLife.Log;
using NewLife.Messaging;
using NewLife.RocketMQ.Client;
using NewLife.RocketMQ.Protocol;
using NewLife.Serialization;
using Message = NewLife.RocketMQ.Protocol.Message;

namespace NewLife.RocketMQ;

/// <summary>RocketMQ事件总线。以主题为边界，通过 RocketMQ 发布与订阅事件</summary>
/// <remarks>
/// <para><b>通道映射</b>：发布走 <see cref="Producer"/>，订阅走 <see cref="Consumer"/>；消费到的批量消息在本地逐条分发到
/// <see cref="EventBus{TEvent}"/> 的订阅者。与 Redis 版事件总线（Redis Stream 消费组）语义一致：</para>
/// <list type="bullet">
/// <item><description>发布不直接唤醒本机订阅者，而是经过 Broker；本机订阅者通过消费组收到后再分发。</description></item>
/// <item><description>一个消费组内的多个节点竞争消费（每条消息只投递给组内一个节点）；需要每个节点都收到时，
/// 应让各节点使用不同的消费组，例如以本机标识构造组名。</description></item>
/// <item><description>处理器失败时返回 <see langword="false"/>，由 <see cref="Consumer"/> 走已有的 SendMessageBack
/// 重试链路（重试主题 → 死信队列），因此<b>处理器应保持幂等</b>；同一批次中部分消息失败会导致整批重投。</description></item>
/// </list>
/// <para><b>使用约束</b>：</para>
/// <list type="bullet">
/// <item><description>RocketMQ 的主题名与消费组名只允许 <c>a-z A-Z 0-9 _ - %</c>，其它字符需先转换；本类构造时会通过
/// <see cref="MqEventBusNaming"/> 自动把事件主题与消费组转换为合法名称，因此可以直接使用 <c>order.created</c> 这类事件主题。</description></item>
/// <item><description><see cref="Producer"/> 按主题绑定，同一实例不能同时用于多个主题；已在运行的生产者传入不同主题时直接报错。</description></item>
/// <item><description><see cref="Consumer"/> 需专用于本总线，其 <c>OnConsumeAsync</c> 会被本类接管。</description></item>
/// </list>
/// </remarks>
/// <typeparam name="TEvent">事件类型。支持 <see cref="MessageExt"/>、<see cref="String"/>、<see cref="Byte"/> 数组以及 POCO（JSON 序列化）</typeparam>
public class RocketMqEventBus<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicParameterlessConstructor | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields)] TEvent> : EventBus<TEvent>, ITracerFeature
{
    #region 属性
    /// <summary>发布者标识字段名。消息属性中该字段存放发布方客户端标识</summary>
    public const String ClientIdKey = "__clientId";

    /// <summary>生产者。按主题绑定</summary>
    public Producer Producer { get; }

    /// <summary>消费者。订阅本总线主题</summary>
    public Consumer Consumer { get; }

    /// <summary>事件主题，同时作为 RocketMQ 主题名</summary>
    public String Topic { get; }

    /// <summary>消费组</summary>
    public String Group { get; }

    /// <summary>是否在消息属性中附加发布者标识，便于订阅方识别来源并排除回环。默认true</summary>
    public Boolean AttachClientId { get; set; } = true;

    /// <summary>是否由本总线释放生产者与消费者。外部传入的客户端默认为 false</summary>
    public Boolean OwnClients { get; }

    /// <summary>事件体序列化器。默认取生产者的 JsonHost</summary>
    public IJsonHost JsonHost { get; set; } = JsonHelper.Default;

    /// <summary>链路追踪</summary>
    public ITracer? Tracer { get; set; }

    private readonly Object _lock = new();
    private Boolean _consuming;
    #endregion

    #region 构造
    /// <summary>使用外部提供的生产者与消费者实例化事件总线</summary>
    /// <remarks>生产者与消费者的生命周期由调用方管理，本总线不负责释放它们。</remarks>
    /// <param name="producer">生产者。其主题会被设置为 <paramref name="topic"/></param>
    /// <param name="consumer">消费者。其主题与消费组会被设置为 <paramref name="topic"/> 与 <paramref name="group"/></param>
    /// <param name="topic">事件主题。为空时取生产者的主题</param>
    /// <param name="group">消费组。为空时取消费者的消费组</param>
    public RocketMqEventBus(Producer producer, Consumer consumer, String topic, String group = null)
    {
        Producer = producer ?? throw new ArgumentNullException(nameof(producer));
        Consumer = consumer ?? throw new ArgumentNullException(nameof(consumer));

        topic ??= producer.Topic;
        if (topic.IsNullOrEmpty()) throw new ArgumentException(L.T("Topic cannot be empty."), nameof(topic));

        group ??= consumer.Group;
        if (group.IsNullOrEmpty()) throw new ArgumentException(L.T("Group cannot be empty."), nameof(group));

        // 事件主题与本机标识常含 . @ 等字符，统一转换为 RocketMQ 允许的名称（与工厂保持一致）
        topic = MqEventBusNaming.ToTopic(topic);
        group = MqEventBusNaming.ToGroup(group);

        // 生产者按主题绑定，运行中不允许换主题
        if (producer.Active && !producer.Topic.EqualIgnoreCase(topic))
            throw new InvalidOperationException(L.T("The producer is already bound to another topic."));

        Topic = topic;
        Group = group;
        JsonHost = producer.JsonHost ?? JsonHelper.Default;
        Tracer = producer.Tracer;

        producer.Topic = topic;
        consumer.Topic = topic;
        consumer.Group = group;

        // 处理器失败需要向上冒泡，才能转换为消费失败并触发回投
        ThrowOnHandlerError = true;
    }

    /// <summary>按名称服务器地址自动创建生产者与消费者</summary>
    /// <remarks>客户端由本总线创建并释放，<paramref name="configure"/> 可注入云厂商适配、ACL、SSL、日志等设置。</remarks>
    /// <param name="nameServerAddress">名称服务器地址</param>
    /// <param name="topic">事件主题</param>
    /// <param name="group">消费组</param>
    /// <param name="configure">客户端设置委托。分别对生产者与消费者调用一次</param>
    public RocketMqEventBus(String nameServerAddress, String topic, String group, Action<MqBase> configure = null)
        : this(CreateProducer(nameServerAddress, topic, configure), CreateConsumer(nameServerAddress, topic, group, configure), topic, group)
        => OwnClients = true;

    private static Producer CreateProducer(String nameServerAddress, String topic, Action<MqBase> configure)
    {
        if (nameServerAddress.IsNullOrEmpty()) throw new ArgumentException(L.T("The NameServer address is not specified."), nameof(nameServerAddress));

        var producer = new Producer { NameServerAddress = nameServerAddress, Topic = topic };
        configure?.Invoke(producer);

        return producer;
    }

    private static Consumer CreateConsumer(String nameServerAddress, String topic, String group, Action<MqBase> configure)
    {
        if (nameServerAddress.IsNullOrEmpty()) throw new ArgumentException(L.T("The NameServer address is not specified."), nameof(nameServerAddress));

        // 事件总线只关心订阅之后的事件，默认从最后位置开始消费，与 Redis 事件总线一致
        var consumer = new Consumer { NameServerAddress = nameServerAddress, Topic = topic, Group = group, FromLastOffset = true };
        configure?.Invoke(consumer);

        return consumer;
    }

    /// <summary>销毁。停止由本总线启动的消费循环；仅当客户端由本总线创建时才一并释放</summary>
    /// <param name="disposing">是否由 Dispose 调用</param>
    protected override void Dispose(Boolean disposing)
    {
        base.Dispose(disposing);

        if (!disposing) return;

        // 消费循环由本总线启动，无论客户端归属如何都要停止，避免继续向已销毁的总线分发
        if (_consuming)
        {
            _consuming = false;
            try
            {
                Consumer.Stop();
            }
            catch (Exception ex)
            {
                Log?.Error(ex.Message);
            }
        }

        if (!OwnClients) return;

        Consumer.TryDispose();
        Producer.TryDispose();
    }
    #endregion

    #region 发布
    /// <summary>发布事件到 RocketMQ</summary>
    /// <param name="event">事件</param>
    /// <param name="context">事件上下文。仅用于携带主题与客户端标识等元数据</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>固定返回 1，表示已成功投递到 Broker</returns>
    public override async Task<Int32> PublishAsync(TEvent @event, IEventContext? context = null, CancellationToken cancellationToken = default)
    {
        if (@event is null) throw new ArgumentNullException(nameof(@event));

        cancellationToken.ThrowIfCancellationRequested();

        // 待发布消息增加追踪标识
        if (@event is ITraceMessage tm && tm.TraceId.IsNullOrEmpty()) tm.TraceId = DefaultSpan.Current?.ToString();

        var message = CreateMessage(@event);
        EnsureProducer();

        using var span = Tracer?.NewSpan($"event:{Topic}:Publish", message.BodyString);
        await Producer.PublishAsync(message, null, cancellationToken).ConfigureAwait(false);

        return 1;
    }

    /// <summary>构造待发布消息。派生类可重写以改变序列化或附加自定义属性</summary>
    /// <param name="event">事件</param>
    /// <returns>RocketMQ消息</returns>
    protected virtual Message CreateMessage(TEvent @event)
    {
        if (@event is Message message)
        {
            if (message.Topic.IsNullOrEmpty()) message.Topic = Topic;
        }
        else if (@event is String str) message = new Message { Body = str.GetBytes() };
        else if (@event is Byte[] buf) message = new Message { Body = buf };
        else if (@event is IPacket pk) message = new Message { Body = pk.ReadBytes() };
        else message = new Message { Body = JsonHost.Write(@event).GetBytes() };

        message.Properties ??= new NullableDictionary<String, String>(StringComparer.OrdinalIgnoreCase);

        // 发布者标识随消息投递，订阅方据此排除回环
        if (AttachClientId) message.PutUserProperty(ClientIdKey, Runtime.ClientId);

        return message;
    }

    /// <summary>确保生产者已启动</summary>
    protected virtual void EnsureProducer()
    {
        lock (_lock)
        {
            if (Producer.Active) return;

            Tracer ??= Producer.Tracer;
            Producer.Start();
        }
    }
    #endregion

    #region 订阅
    /// <summary>订阅事件。首次订阅时启动消费循环</summary>
    /// <param name="handler">事件处理器</param>
    /// <param name="clientId">客户标识。同一标识重复订阅时覆盖前一次</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>是否订阅成功</returns>
    public override Task<Boolean> SubscribeAsync(IEventHandler<TEvent> handler, String clientId = "", CancellationToken cancellationToken = default)
    {
        if (handler == null) throw new ArgumentNullException(nameof(handler));

        EnsureConsumer();

        return base.SubscribeAsync(handler, clientId, cancellationToken);
    }

    /// <summary>确保消费者已启动</summary>
    /// <remarks>必须先挂上 <see cref="Consumer.OnConsumeAsync"/> 再启动，否则首批消息会被判为消费失败并全部回投。</remarks>
    protected virtual void EnsureConsumer()
    {
        lock (_lock)
        {
            if (_consuming) return;

            Consumer.OnConsumeAsync = OnConsumeAsync;
            Tracer ??= Consumer.Tracer;
            Consumer.Start();

            _consuming = true;
        }
    }

    /// <summary>消费到一批消息，逐条解码并分发到本地订阅者</summary>
    /// <param name="queue">来源队列</param>
    /// <param name="messages">消息数组</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>是否全部处理成功。任一消息处理异常时返回 false，由消费者回投重试</returns>
    protected virtual async Task<Boolean> OnConsumeAsync(MessageQueue queue, MessageExt[] messages, CancellationToken cancellationToken)
    {
        if (messages == null || messages.Length == 0) return true;

        var success = true;
        foreach (var message in messages)
        {
            ISpan? span = null;
            try
            {
                var @event = DecodeMessage(message, out var error);
                if (error != null)
                {
                    // 解码失败的消息无法处理，跳过而不是无限回投（避免毒消息堵塞队列）
                    WriteLog("Failed to decode message, skipping [{0}]: {1}", message.MsgId, error.Message);
                    continue;
                }
                if (@event is null) continue;

                span = Tracer?.NewSpan($"event:{Topic}:Consume", message.BodyString);
                if (span != null && @event is ITraceMessage tm && !tm.TraceId.IsNullOrEmpty()) span.Detach(tm.TraceId);

                // 每个消费者线程都可能同时分发，上下文不能复用
                var context = new RocketMqEventContext(this, message)
                {
                    Topic = Topic,
                    ClientId = AttachClientId ? message.GetUserProperty(ClientIdKey) : null,
                };
                await DispatchAsync(@event, context, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                success = false;
                span?.SetError(ex, null);
                WriteLog("Failed to process message {0}: {1}", message.MsgId, ex.Message);
            }
            finally
            {
                span?.Dispose();
            }
        }

        return success;
    }

    /// <summary>解码消息为事件对象</summary>
    /// <param name="message">消息</param>
    /// <param name="error">解码异常。非空表示该消息无法处理</param>
    /// <returns>事件对象</returns>
    protected virtual TEvent DecodeMessage(MessageExt message, out Exception error)
    {
        error = null;
        if (message == null) return default!;

        // TEvent 本身是消息类型时直接透传
        if (message is TEvent evt) return evt;

        var body = message.Body;
        if (body == null || body.Length == 0) return default!;

        if (typeof(TEvent) == typeof(String)) return (TEvent)(Object)message.BodyString;
        if (typeof(TEvent) == typeof(Byte[])) return (TEvent)(Object)body;

        try
        {
            return JsonHost.Read<TEvent>(message.BodyString, null);
        }
        catch (Exception ex)
        {
            error = ex;

            return default!;
        }
    }
    #endregion
}
