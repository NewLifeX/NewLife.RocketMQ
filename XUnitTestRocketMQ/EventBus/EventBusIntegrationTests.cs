using System;
using System.ComponentModel;
using System.Threading;
using System.Threading.Tasks;
using NewLife;
using NewLife.Messaging;
using NewLife.RocketMQ;
using NewLife.Security;
using Xunit;
using XUnitTest.Integration;

namespace XUnitTest.EventBus;

/// <summary>事件总线集成测试。需要真实 Broker，验证发布订阅往返与失败回投</summary>
[Collection("RocketMQ")]
[Trait("Category", "Integration")]
public class EventBusIntegrationTests
{
    [Fact]
    [DisplayName("事件总线_发布后订阅_收到同一事件")]
    public async Task PublishAndSubscribe_String_Received()
    {
        var set = BasicTest.GetConfig();
        var topic = "nx_evt_" + Rand.NextString(6);
        var group = "nx_evt_g_" + Rand.NextString(6);

        using var bus = new RocketMqEventBus<String>(set.NameServer, topic, group);

        var source = new TaskCompletionSource<String>(TaskCreationOptions.RunContinuationsAsynchronously);
        await bus.SubscribeAsync((String evt) => source.TrySetResult(evt), "local");

        // 消费组就绪前发布的消息可能被跳过（FromLastOffset），循环发布直到订阅方收到
        var deadline = DateTime.UtcNow.AddSeconds(20);
        var value = "hello-" + Rand.NextString(4);
        while (!source.Task.IsCompleted && DateTime.UtcNow < deadline)
        {
            await bus.PublishAsync(value);
            await Task.Delay(100);
        }

        var completed = await Task.WhenAny(source.Task, Task.Delay(10_000));
        Assert.Same(source.Task, completed);
        Assert.Equal(value, await source.Task);
    }

    [Fact]
    [DisplayName("事件总线_处理器首次失败_消息被重新投递")]
    public async Task ConsumeHandler_FailsFirst_Redelivered()
    {
        var set = BasicTest.GetConfig();
        var topic = "nx_evt_" + Rand.NextString(6);
        var group = "nx_evt_g_" + Rand.NextString(6);

        using var bus = new RocketMqEventBus<String>(set.NameServer, topic, group);

        var count = 0;
        var source = new TaskCompletionSource<Int32>(TaskCreationOptions.RunContinuationsAsynchronously);
        await bus.SubscribeAsync((String evt) =>
        {
            // 首次失败触发回投，第二次成功
            if (Interlocked.Increment(ref count) == 1) throw new InvalidOperationException("first attempt fails");
            source.TrySetResult(count);
        }, "local");

        var deadline = DateTime.UtcNow.AddSeconds(20);
        while (!source.Task.IsCompleted && DateTime.UtcNow < deadline)
        {
            await bus.PublishAsync("retry-" + Rand.NextString(4));
            await Task.Delay(100);
        }

        var completed = await Task.WhenAny(source.Task, Task.Delay(60_000));
        Assert.Same(source.Task, completed);
        Assert.True(await source.Task >= 2);
    }
}
