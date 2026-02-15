using System;
using System.ComponentModel;
using NewLife.RocketMQ;
using Xunit;

namespace XUnitTestRocketMQ;

/// <summary>顺序消费锁定测试</summary>
public class OrderConsumeTests
{
    [Fact]
    [DisplayName("OrderConsume_默认为false")]
    public void OrderConsume_DefaultFalse()
    {
        using var consumer = new Consumer();
        Assert.False(consumer.OrderConsume);
    }

    [Fact]
    [DisplayName("OrderConsume_可启用")]
    public void OrderConsume_CanBeEnabled()
    {
        using var consumer = new Consumer { OrderConsume = true };
        Assert.True(consumer.OrderConsume);
    }

    [Fact]
    [DisplayName("LockBatchMQAsync_空列表返回空")]
    public async void LockBatchMQAsync_EmptyList_ReturnsEmpty()
    {
        using var consumer = new Consumer();
        var result = await consumer.LockBatchMQAsync([]);
        Assert.Empty(result);
    }

    [Fact]
    [DisplayName("LockBatchMQAsync_Null列表返回空")]
    public async void LockBatchMQAsync_NullList_ReturnsEmpty()
    {
        using var consumer = new Consumer();
        var result = await consumer.LockBatchMQAsync(null);
        Assert.Empty(result);
    }

    [Fact]
    [DisplayName("UnlockBatchMQAsync_空列表不抛异常")]
    public async void UnlockBatchMQAsync_EmptyList_NoException()
    {
        using var consumer = new Consumer();
        await consumer.UnlockBatchMQAsync([]);
    }

    [Fact]
    [DisplayName("UnlockBatchMQAsync_Null列表不抛异常")]
    public async void UnlockBatchMQAsync_NullList_NoException()
    {
        using var consumer = new Consumer();
        await consumer.UnlockBatchMQAsync(null);
    }
}
