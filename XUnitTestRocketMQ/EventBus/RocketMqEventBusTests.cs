using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Threading;
using System.Threading.Tasks;
using NewLife;
using NewLife.Messaging;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Protocol;
using NewLife.Security;
using NewLife.Serialization;
using Xunit;
using Message = NewLife.RocketMQ.Protocol.Message;

namespace XUnitTest.EventBus;

/// <summary>RocketMQ事件总线单元测试。不依赖真实 Broker，覆盖消息构造、分发与失败语义</summary>
public class RocketMqEventBusTests
{
    #region 测试辅助
    /// <summary>截获发布报文的生产者，不发起网络请求</summary>
    class FakeProducer : Producer
    {
        public Message LastMessage { get; private set; }

        public Int32 PublishCount { get; private set; }

        public override Task<SendResult> PublishAsync(Message message, MessageQueue queue, CancellationToken cancellationToken = default)
        {
            LastMessage = message;
            PublishCount++;

            return Task.FromResult(new SendResult { Status = SendStatus.SendOK, MsgId = "fake" });
        }
    }

    /// <summary>跳过网络启动、暴露受保护方法的事件总线</summary>
    class TestBus<TEvent>(Producer producer, Consumer consumer, String topic, String group = null) : RocketMqEventBus<TEvent>(producer, consumer, topic, group)
    {
        protected override void EnsureProducer() { }

        protected override void EnsureConsumer() { }

        public Task<Boolean> ConsumeAsync(MessageQueue queue, MessageExt[] messages, CancellationToken cancellationToken = default) => OnConsumeAsync(queue, messages, cancellationToken);

        public Message Build(TEvent @event) => CreateMessage(@event);
    }

    static Consumer CreateConsumer(String topic, String group = "test_group") => new() { Topic = topic, Group = group };

    static MessageExt CreateMessage(String body, String clientId = null)
    {
        var msg = new MessageExt { MsgId = Rand.NextString(8), Body = body?.GetBytes() };
        if (!clientId.IsNullOrEmpty()) msg.PutUserProperty(RocketMqEventBus<Int32>.ClientIdKey, clientId);

        return msg;
    }

    class OnlineEvent
    {
        public String UserName { get; set; }

        public Int32 Level { get; set; }
    }
    #endregion

    #region 构造
    [Fact]
    [DisplayName("构造_主题为空_抛参数异常")]
    public void Ctor_EmptyTopic_Throws()
    {
        var producer = new FakeProducer { Topic = null };
        var consumer = CreateConsumer("t");

        Assert.Throws<ArgumentException>(() => new TestBus<Int32>(producer, consumer, null, "g"));
    }

    [Fact]
    [DisplayName("构造_消费组为空_抛参数异常")]
    public void Ctor_EmptyGroup_Throws()
    {
        var producer = new FakeProducer { Topic = "t" };
        var consumer = CreateConsumer("t", null);

        Assert.Throws<ArgumentException>(() => new TestBus<Int32>(producer, consumer, "t", null));
    }

    [Fact]
    [DisplayName("构造_自建客户端_默认从最后位置消费且由总线释放")]
    public void Ctor_SelfBuilt_UsesFromLastOffset()
    {
        using var bus = new RocketMqEventBus<Int32>("localhost:9876", "nx_event_topic", "nx_event_group");

        Assert.True(bus.OwnClients);
        Assert.True(bus.Consumer.FromLastOffset);
        Assert.Equal("nx_event_topic", bus.Producer.Topic);
        Assert.Equal("nx_event_topic", bus.Consumer.Topic);
        Assert.Equal("nx_event_group", bus.Consumer.Group);
        Assert.Equal("localhost:9876", bus.Producer.NameServerAddress);
    }
    #endregion

    #region 发布
    [Fact]
    [DisplayName("发布_对象事件_序列化为JSON并附加发布者标识")]
    public async Task PublishAsync_Poco_WritesJsonAndClientId()
    {
        var producer = new FakeProducer();
        using var bus = new TestBus<OnlineEvent>(producer, CreateConsumer("t"), "nx_event_topic");

        var n = await bus.PublishAsync(new OnlineEvent { UserName = "smartStone", Level = 9 });

        Assert.Equal(1, n);
        Assert.Equal(1, producer.PublishCount);
        Assert.Equal("nx_event_topic", producer.Topic);
        Assert.Contains("smartStone", producer.LastMessage.BodyString);
        Assert.Equal(Runtime.ClientId, producer.LastMessage.GetUserProperty(RocketMqEventBus<OnlineEvent>.ClientIdKey));
    }

    [Fact]
    [DisplayName("发布_关闭标识开关_不写发布者标识")]
    public void Build_AttachClientIdDisabled_NoProperty()
    {
        var producer = new FakeProducer();
        using var bus = new TestBus<String>(producer, CreateConsumer("t"), "nx_event_topic") { AttachClientId = false };

        var msg = bus.Build("hello");

        Assert.Equal("hello", msg.BodyString);
        Assert.Null(msg.GetUserProperty(RocketMqEventBus<String>.ClientIdKey));
    }

    [Fact]
    [DisplayName("发布_字符串事件_原样写入消息体")]
    public async Task PublishAsync_String_WritesRawBody()
    {
        var producer = new FakeProducer();
        using var bus = new TestBus<String>(producer, CreateConsumer("t"), "nx_event_topic");

        await bus.PublishAsync("hello");

        Assert.Equal("hello", producer.LastMessage.BodyString);
    }
    #endregion

    #region 分发
    [Fact]
    [DisplayName("分发_字符串事件_订阅者收到事件与上下文标识")]
    public async Task ConsumeAsync_StringEvent_Dispatched()
    {
        var producer = new FakeProducer();
        using var bus = new TestBus<String>(producer, CreateConsumer("t"), "nx_event_topic");

        String received = null;
        String clientId = null;
        await bus.SubscribeAsync((String evt, IEventContext ctx) =>
        {
            received = evt;
            clientId = (ctx as EventContext)?.ClientId;
        }, "local");

        var ok = await bus.ConsumeAsync(new MessageQueue(), [CreateMessage("hello", "node-1")]);

        Assert.True(ok);
        Assert.Equal("hello", received);
        Assert.Equal("node-1", clientId);
    }

    [Fact]
    [DisplayName("分发_复杂对象_反序列化后交给订阅者")]
    public async Task ConsumeAsync_Poco_Deserialized()
    {
        var producer = new FakeProducer();
        using var bus = new TestBus<OnlineEvent>(producer, CreateConsumer("t"), "nx_event_topic");

        OnlineEvent received = null;
        await bus.SubscribeAsync((OnlineEvent evt) => received = evt, "local");

        var json = JsonHelper.Default.Write(new OnlineEvent { UserName = "smartStone", Level = 9 });
        var ok = await bus.ConsumeAsync(new MessageQueue(), [CreateMessage(json)]);

        Assert.True(ok);
        Assert.NotNull(received);
        Assert.Equal("smartStone", received.UserName);
        Assert.Equal(9, received.Level);
    }

    [Fact]
    [DisplayName("分发_处理器异常_返回失败以触发回投")]
    public async Task ConsumeAsync_HandlerThrows_ReturnsFalse()
    {
        var producer = new FakeProducer();
        using var bus = new TestBus<String>(producer, CreateConsumer("t"), "nx_event_topic");

        await bus.SubscribeAsync((String evt) => throw new InvalidOperationException("boom"), "local");

        var ok = await bus.ConsumeAsync(new MessageQueue(), [CreateMessage("hello")]);

        Assert.False(ok);
    }

    [Fact]
    [DisplayName("分发_消息体无法解码_跳过并视为成功")]
    public async Task ConsumeAsync_BadJson_SkippedAndReturnsTrue()
    {
        var producer = new FakeProducer();
        using var bus = new TestBus<OnlineEvent>(producer, CreateConsumer("t"), "nx_event_topic");

        var count = 0;
        await bus.SubscribeAsync((OnlineEvent evt) => count++, "local");

        var ok = await bus.ConsumeAsync(new MessageQueue(), [CreateMessage("{not-json")]);

        Assert.True(ok);
        Assert.Equal(0, count);
    }

    [Fact]
    [DisplayName("分发_发布者标识与本机订阅标识相同_排除回环")]
    public async Task ConsumeAsync_SameClientId_Skipped()
    {
        var producer = new FakeProducer();
        using var bus = new TestBus<String>(producer, CreateConsumer("t"), "nx_event_topic");

        var count = 0;
        await bus.SubscribeAsync((String evt) => count++, "node-1");

        var ok = await bus.ConsumeAsync(new MessageQueue(), [CreateMessage("hello", "node-1")]);

        Assert.True(ok);
        Assert.Equal(0, count);
    }

    [Fact]
    [DisplayName("分发_空批次_直接返回成功")]
    public async Task ConsumeAsync_EmptyBatch_ReturnsTrue()
    {
        var producer = new FakeProducer();
        using var bus = new TestBus<String>(producer, CreateConsumer("t"), "nx_event_topic");

        Assert.True(await bus.ConsumeAsync(new MessageQueue(), []));
        Assert.True(await bus.ConsumeAsync(new MessageQueue(), null));
    }
    #endregion
}
