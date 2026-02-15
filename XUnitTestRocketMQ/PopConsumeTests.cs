using System;
using System.ComponentModel;
using NewLife.RocketMQ;
using Xunit;

namespace XUnitTestRocketMQ;

/// <summary>Pop消费模式测试</summary>
public class PopConsumeTests
{
    [Fact]
    [DisplayName("PopMessageAsync_Null的BrokerName抛出异常")]
    public async void PopMessageAsync_NullBrokerName_ThrowsException()
    {
        using var consumer = new Consumer();
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            consumer.PopMessageAsync(null));
    }

    [Fact]
    [DisplayName("PopMessageAsync_空BrokerName抛出异常")]
    public async void PopMessageAsync_EmptyBrokerName_ThrowsException()
    {
        using var consumer = new Consumer();
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            consumer.PopMessageAsync(""));
    }

    [Fact]
    [DisplayName("AckMessageAsync_无Broker连接时返回false")]
    public async void AckMessageAsync_NoBroker_ReturnsFalse()
    {
        using var consumer = new Consumer();
        // 未Start，无Broker连接
        var result = await consumer.AckMessageAsync("nonexistent", "extra", 0);
        Assert.False(result);
    }

    [Fact]
    [DisplayName("ChangeInvisibleTimeAsync_无Broker连接时返回false")]
    public async void ChangeInvisibleTimeAsync_NoBroker_ReturnsFalse()
    {
        using var consumer = new Consumer();
        var result = await consumer.ChangeInvisibleTimeAsync("nonexistent", "extra", 0, 30000);
        Assert.False(result);
    }

    [Fact]
    [DisplayName("RequestCode包含Pop消费相关码")]
    public void RequestCode_ContainsPopCodes()
    {
        Assert.Equal(200050, (Int32)NewLife.RocketMQ.Protocol.RequestCode.POP_MESSAGE);
        Assert.Equal(200051, (Int32)NewLife.RocketMQ.Protocol.RequestCode.ACK_MESSAGE);
        Assert.Equal(200052, (Int32)NewLife.RocketMQ.Protocol.RequestCode.CHANGE_MESSAGE_INVISIBLETIME);
        Assert.Equal(200151, (Int32)NewLife.RocketMQ.Protocol.RequestCode.BATCH_ACK_MESSAGE);
    }
}
