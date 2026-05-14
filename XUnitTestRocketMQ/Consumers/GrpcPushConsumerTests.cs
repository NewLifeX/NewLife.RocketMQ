using System;
using System.ComponentModel;
using System.Threading;
using System.Threading.Tasks;
using NewLife.RocketMQ.Grpc;
using Xunit;

namespace XUnitTest.Consumers;

/// <summary>GrpcPushConsumer 单元测试</summary>
public class GrpcPushConsumerTests
{
    [Fact]
    [DisplayName("GrpcPushConsumer_默认属性值正确")]
    public void GrpcPushConsumer_DefaultProperties()
    {
        var consumer = new GrpcPushConsumer();

        Assert.Equal(32, consumer.BatchSize);
        Assert.Equal(TimeSpan.FromSeconds(30), consumer.InvisibleDuration);
        Assert.Equal(TimeSpan.FromSeconds(20), consumer.LongPollingTimeout);
        Assert.Equal(TimeSpan.FromSeconds(5), consumer.RetryInvisibleDuration);
        Assert.Equal(20, consumer.MaxConcurrentConsume);
        Assert.Null(consumer.OnMessage);

        consumer.Dispose();
    }

    [Fact]
    [DisplayName("GrpcPushConsumer_属性读写正确")]
    public void GrpcPushConsumer_Properties_ReadWrite()
    {
        var consumer = new GrpcPushConsumer
        {
            Topic = "test-topic",
            Group = "test-group",
            Endpoints = "127.0.0.1:8081",
            Namespace = "test-ns",
            BatchSize = 10,
            InvisibleDuration = TimeSpan.FromSeconds(60),
            LongPollingTimeout = TimeSpan.FromSeconds(30),
            RetryInvisibleDuration = TimeSpan.FromSeconds(10),
            MaxConcurrentConsume = 5,
        };

        Assert.Equal("test-topic", consumer.Topic);
        Assert.Equal("test-group", consumer.Group);
        Assert.Equal("127.0.0.1:8081", consumer.Endpoints);
        Assert.Equal("test-ns", consumer.Namespace);
        Assert.Equal(10, consumer.BatchSize);
        Assert.Equal(TimeSpan.FromSeconds(60), consumer.InvisibleDuration);
        Assert.Equal(TimeSpan.FromSeconds(30), consumer.LongPollingTimeout);
        Assert.Equal(TimeSpan.FromSeconds(10), consumer.RetryInvisibleDuration);
        Assert.Equal(5, consumer.MaxConcurrentConsume);

        consumer.Dispose();
    }

    [Fact]
    [DisplayName("GrpcPushConsumer_StartAsync_Topic为空抛出异常")]
    public async Task GrpcPushConsumer_StartAsync_EmptyTopic_Throws()
    {
        var consumer = new GrpcPushConsumer
        {
            Group = "test-group",
            Endpoints = "127.0.0.1:8081",
            OnMessage = _ => Task.FromResult(true),
        };

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => consumer.StartAsync());
        Assert.Contains("Topic", ex.Message);

        consumer.Dispose();
    }

    [Fact]
    [DisplayName("GrpcPushConsumer_StartAsync_Group为空抛出异常")]
    public async Task GrpcPushConsumer_StartAsync_EmptyGroup_Throws()
    {
        var consumer = new GrpcPushConsumer
        {
            Topic = "test-topic",
            Endpoints = "127.0.0.1:8081",
            OnMessage = _ => Task.FromResult(true),
        };

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => consumer.StartAsync());
        Assert.Contains("Group", ex.Message);

        consumer.Dispose();
    }

    [Fact]
    [DisplayName("GrpcPushConsumer_StartAsync_Endpoints为空抛出异常")]
    public async Task GrpcPushConsumer_StartAsync_EmptyEndpoints_Throws()
    {
        var consumer = new GrpcPushConsumer
        {
            Topic = "test-topic",
            Group = "test-group",
            OnMessage = _ => Task.FromResult(true),
        };

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => consumer.StartAsync());
        Assert.Contains("Endpoints", ex.Message);

        consumer.Dispose();
    }

    [Fact]
    [DisplayName("GrpcPushConsumer_StartAsync_OnMessage为空抛出异常")]
    public async Task GrpcPushConsumer_StartAsync_NullOnMessage_Throws()
    {
        var consumer = new GrpcPushConsumer
        {
            Topic = "test-topic",
            Group = "test-group",
            Endpoints = "127.0.0.1:8081",
        };

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => consumer.StartAsync());
        Assert.Contains("OnMessage", ex.Message);

        consumer.Dispose();
    }

    [Fact]
    [DisplayName("GrpcPushConsumer_StopAsync_未启动不抛出")]
    public async Task GrpcPushConsumer_StopAsync_NotStarted_NoThrow()
    {
        var consumer = new GrpcPushConsumer();
        // 未启动就停止不应抛出
        await consumer.StopAsync();
        consumer.Dispose();
    }

    [Fact]
    [DisplayName("GrpcPushConsumer_Dispose_多次调用不抛出")]
    public void GrpcPushConsumer_Dispose_MultipleCallsNoThrow()
    {
        var consumer = new GrpcPushConsumer();
        consumer.Dispose();
        consumer.Dispose(); // 重复 Dispose 不应抛出
    }

    [Fact]
    [DisplayName("GrpcPushConsumer_OnMessage_可设置异步回调")]
    public void GrpcPushConsumer_OnMessage_AsyncCallback()
    {
        var consumer = new GrpcPushConsumer();
        Func<GrpcMessage, Task<Boolean>> callback = async msg =>
        {
            await Task.Delay(10);
            return true;
        };

        consumer.OnMessage = callback;
        Assert.Same(callback, consumer.OnMessage);

        consumer.Dispose();
    }
}
