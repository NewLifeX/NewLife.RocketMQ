using System;
using System.ComponentModel;
using System.Threading;
using System.Threading.Tasks;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Protocol;
using Xunit;
using Message = NewLife.RocketMQ.Protocol.Message;

namespace XUnitTest.EventBus;

/// <summary>事件总线工厂测试。以外部工厂模式验证缓存与命名转换，不依赖真实 Broker</summary>
public class RocketMqEventBusFactoryTests
{
    class FakeProducer : Producer
    {
        public override Task<SendResult> PublishAsync(Message message, MessageQueue queue, CancellationToken cancellationToken = default)
            => Task.FromResult(new SendResult { Status = SendStatus.SendOK });
    }

    static RocketMqEventBusFactory CreateFactory() => new()
    {
        ProducerFactory = topic => new FakeProducer { Topic = topic },
        ConsumerFactory = (topic, group) => new Consumer { Topic = topic, Group = group },
    };

    [Fact]
    [DisplayName("工厂_主题为空_返回空")]
    public void CreateEventBus_EmptyTopic_ReturnsNull()
    {
        using var factory = CreateFactory();

        Assert.Null(factory.CreateEventBus<String>(null));
        Assert.Null(factory.CreateEventBus<String>(""));
    }

    [Fact]
    [DisplayName("工厂_无可用通道_返回空")]
    public void CreateEventBus_NoChannel_ReturnsNull()
    {
        using var factory = new RocketMqEventBusFactory();

        Assert.Null(factory.CreateEventBus<String>("order.created"));
    }

    [Fact]
    [DisplayName("工厂_同一主题重复创建_返回同一实例")]
    public void CreateEventBus_SameTopic_ReturnsSameInstance()
    {
        using var factory = CreateFactory();

        var a = factory.CreateEventBus<String>("order.created");
        var b = factory.CreateEventBus<String>("order.created");

        Assert.NotNull(a);
        Assert.Same(a, b);
        Assert.Equal("order_created", ((RocketMqEventBus<String>)a).Topic);
    }

    [Fact]
    [DisplayName("工厂_不同事件类型_各自独立总线")]
    public void CreateEventBus_DifferentEventType_SeparateBus()
    {
        using var factory = CreateFactory();

        var a = factory.CreateEventBus<String>("order.created");
        var b = factory.CreateEventBus<Int32>("order.created");

        Assert.NotNull(a);
        Assert.NotNull(b);
        Assert.NotSame(a, b);
    }

    [Fact]
    [DisplayName("工厂_外部客户端模式_总线不接管客户端释放")]
    public void CreateEventBus_ExternalClients_NotOwned()
    {
        using var factory = CreateFactory();

        var bus = factory.CreateEventBus<String>("order.created") as RocketMqEventBus<String>;

        Assert.NotNull(bus);
        Assert.False(bus.OwnClients);
    }
}
