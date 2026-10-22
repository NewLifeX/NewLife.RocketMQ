using System.Diagnostics.CodeAnalysis;

namespace NewLife.RocketMQ;

/// <summary>事件总线扩展</summary>
public static class RocketMqEventBusExtensions
{
    /// <summary>基于已有生产者与消费者创建事件总线</summary>
    /// <remarks>生产者与消费者的生命周期由调用方管理。生产者需按主题绑定，消费者不要与其它逻辑共用。</remarks>
    /// <typeparam name="TEvent">事件类型</typeparam>
    /// <param name="producer">生产者</param>
    /// <param name="consumer">消费者</param>
    /// <param name="topic">事件主题。为空时取生产者的主题</param>
    /// <param name="group">消费组。为空时取消费者的消费组</param>
    /// <returns>事件总线实例</returns>
    public static RocketMqEventBus<TEvent> CreateEventBus<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicParameterlessConstructor | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields)] TEvent>(this Producer producer, Consumer consumer, String topic = null, String group = null)
    {
        if (producer == null) throw new ArgumentNullException(nameof(producer));
        if (consumer == null) throw new ArgumentNullException(nameof(consumer));

        return new RocketMqEventBus<TEvent>(producer, consumer, topic ?? producer.Topic, group ?? consumer.Group);
    }
}
