using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using NewLife.Log;
using NewLife.Messaging;
using NewLife.RocketMQ.Client;

namespace NewLife.RocketMQ;

/// <summary>RocketMQ事件总线工厂。按主题创建事件总线，可挂到事件枢纽</summary>
/// <remarks>
/// <para>两种客户端模式：</para>
/// <list type="bullet">
/// <item><description><b>自建模式</b>：只配置 <see cref="NameServerAddress"/>，由工厂为每个主题创建生产者与消费者，总线销毁时一并释放。</description></item>
/// <item><description><b>外部模式</b>：设置 <see cref="ProducerFactory"/> 与 <see cref="ConsumerFactory"/>，客户端生命周期由调用方管理。</description></item>
/// </list>
/// <para><b>消费组语义</b>：同一消费组内的多个节点竞争消费，每条消息只投递给组内一个节点。
/// 事件总线需要每个节点都收到事件，因此组名默认取 <see cref="Runtime.ClientId"/>，即每个节点各成一组。
/// 需要「整组只处理一次」的队列语义时，显式传入相同的 <c>clientId</c> 即可。</para>
/// </remarks>
public class RocketMqEventBusFactory : IEventBusFactory, IDisposable
{
    #region 属性
    /// <summary>名称服务器地址。自建模式下使用</summary>
    public String NameServerAddress { get; set; }

    /// <summary>消费组前缀。便于在 Broker 上按业务归类</summary>
    public String GroupPrefix { get; set; }

    /// <summary>客户端设置委托。自建模式下用于注入云厂商适配、ACL、SSL、日志等</summary>
    public Action<MqBase> Configure { get; set; }

    /// <summary>生产者工厂。参数为主题名，返回 null 表示不可用</summary>
    public Func<String, Producer> ProducerFactory { get; set; }

    /// <summary>消费者工厂。参数为主题名与消费组，返回 null 表示不可用</summary>
    public Func<String, String, Consumer> ConsumerFactory { get; set; }

    /// <summary>链路追踪</summary>
    public ITracer Tracer { get; set; }

    /// <summary>日志</summary>
    public ILog Log { get; set; } = Logger.Null;

    private readonly ConcurrentDictionary<String, Object> _buses = new();
    #endregion

    #region 构造
    /// <summary>实例化工厂</summary>
    public RocketMqEventBusFactory() { }

    /// <summary>以名称服务器地址实例化工厂</summary>
    /// <param name="nameServerAddress">名称服务器地址</param>
    public RocketMqEventBusFactory(String nameServerAddress) => NameServerAddress = nameServerAddress;
    #endregion

    #region 创建
    /// <summary>创建事件总线。同一主题重复调用返回同一实例</summary>
    /// <remarks>没有任何可用通道时返回 <see langword="null"/>，由 <see cref="EventHub{TEvent}"/> 回退到进程内总线。</remarks>
    /// <typeparam name="TEvent">事件类型</typeparam>
    /// <param name="topic">事件主题</param>
    /// <param name="clientId">客户标识。为空时使用当前节点标识，直接影响消费组</param>
    /// <returns>事件总线实例</returns>
    [UnconditionalSuppressMessage("Trimming", "IL2095", Justification = "Interface method carries no trimming annotations; the event type requirement is enforced by the created bus.")]
    public IEventBus<TEvent>? CreateEventBus<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicParameterlessConstructor | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields)] TEvent>(String topic, String clientId = "")
    {
        if (topic.IsNullOrEmpty()) return null;

        // 主题与组名都做 RocketMQ 命名转换，发布与订阅两侧保持一致
        var name = MqEventBusNaming.ToTopic(topic);
        var group = MqEventBusNaming.ToGroup((GroupPrefix ?? String.Empty) + (clientId.IsNullOrEmpty() ? Runtime.ClientId : clientId));

        var key = $"{typeof(TEvent).FullName}#{name}#{group}";
        if (_buses.TryGetValue(key, out var obj)) return obj as IEventBus<TEvent>;

        RocketMqEventBus<TEvent>? bus = null;
        try
        {
            bus = Create<TEvent>(name, group);
        }
        catch (Exception ex)
        {
            Log?.Error(ex.Message);

            return null;
        }

        var actual = _buses.GetOrAdd(key, bus);
        if (actual != bus) bus.Dispose();

        return actual as IEventBus<TEvent>;
    }

    private RocketMqEventBus<TEvent>? Create<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicParameterlessConstructor | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields)] TEvent>(String topic, String group)
    {
        RocketMqEventBus<TEvent> bus;
        if (ProducerFactory != null && ConsumerFactory != null)
        {
            var producer = ProducerFactory(topic);
            var consumer = ConsumerFactory(topic, group);
            if (producer == null || consumer == null) return null;

            bus = new RocketMqEventBus<TEvent>(producer, consumer, topic, group);
        }
        else
        {
            if (NameServerAddress.IsNullOrEmpty()) return null;

            bus = new RocketMqEventBus<TEvent>(NameServerAddress, topic, group, Configure);
        }

        if (Tracer != null) bus.Tracer = Tracer;
        if (Log != null && Log != Logger.Null) bus.Log = Log;

        return bus;
    }
    #endregion

    #region 释放
    /// <summary>销毁工厂，释放由其创建的事件总线</summary>
    public void Dispose()
    {
        foreach (var item in _buses.Values)
        {
            item.TryDispose();
        }

        _buses.Clear();
    }
    #endregion
}
