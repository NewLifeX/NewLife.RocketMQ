using System;
using System.ComponentModel;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Protocol;
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
    [DisplayName("PopMessageAsync_可指定queueId参数")]
    public async void PopMessageAsync_WithQueueId_ThrowsWhenBrokerNameNull()
    {
        using var consumer = new Consumer();
        // 验证带queueId的重载依然会在brokerName为null时抛出异常
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            consumer.PopMessageAsync(null, queueId: 0));
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
    [DisplayName("AckMessageAsync_指定queueId_无Broker连接时返回false")]
    public async void AckMessageAsync_WithQueueId_NoBroker_ReturnsFalse()
    {
        using var consumer = new Consumer();
        var result = await consumer.AckMessageAsync("nonexistent", "extra", 0, queueId: 2);
        Assert.False(result);
    }

    [Fact]
    [DisplayName("AckMessageAsync_传入MessageExt_Null消息抛出异常")]
    public async void AckMessageAsync_NullMsg_ThrowsException()
    {
        using var consumer = new Consumer();
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            consumer.AckMessageAsync("broker", (MessageExt)null));
    }

    [Fact]
    [DisplayName("AckMessageAsync_传入MessageExt_缺少POP_CK属性抛出异常")]
    public async void AckMessageAsync_MsgWithoutPopCk_ThrowsArgumentException()
    {
        using var consumer = new Consumer();
        var msg = new MessageExt { QueueId = 1, QueueOffset = 100 };
        // 没有设置 PopCheckPoint（POP_CK）
        await Assert.ThrowsAsync<ArgumentException>(() =>
            consumer.AckMessageAsync("broker", msg));
    }

    [Fact]
    [DisplayName("AckMessageAsync_传入MessageExt_无Broker连接时返回false")]
    public async void AckMessageAsync_WithMsgExt_NoBroker_ReturnsFalse()
    {
        using var consumer = new Consumer();
        var msg = new MessageExt { QueueId = 1, QueueOffset = 100 };
        msg.PopCheckPoint = "100 1700000000000 60000 1 broker-a 1";
        var result = await consumer.AckMessageAsync("nonexistent", msg);
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
    [DisplayName("ChangeInvisibleTimeAsync_指定queueId_无Broker连接时返回false")]
    public async void ChangeInvisibleTimeAsync_WithQueueId_NoBroker_ReturnsFalse()
    {
        using var consumer = new Consumer();
        var result = await consumer.ChangeInvisibleTimeAsync("nonexistent", "extra", 0, 30000, queueId: 3);
        Assert.False(result);
    }

    [Fact]
    [DisplayName("ChangeInvisibleTimeAsync_传入MessageExt_Null消息抛出异常")]
    public async void ChangeInvisibleTimeAsync_NullMsg_ThrowsException()
    {
        using var consumer = new Consumer();
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            consumer.ChangeInvisibleTimeAsync("broker", (MessageExt)null, 30000));
    }

    [Fact]
    [DisplayName("ChangeInvisibleTimeAsync_传入MessageExt_缺少POP_CK属性抛出异常")]
    public async void ChangeInvisibleTimeAsync_MsgWithoutPopCk_ThrowsArgumentException()
    {
        using var consumer = new Consumer();
        var msg = new MessageExt { QueueId = 2, QueueOffset = 200 };
        // 没有设置 PopCheckPoint（POP_CK）
        await Assert.ThrowsAsync<ArgumentException>(() =>
            consumer.ChangeInvisibleTimeAsync("broker", msg, 30000));
    }

    [Fact]
    [DisplayName("ChangeInvisibleTimeAsync_传入MessageExt_无Broker连接时返回false")]
    public async void ChangeInvisibleTimeAsync_WithMsgExt_NoBroker_ReturnsFalse()
    {
        using var consumer = new Consumer();
        var msg = new MessageExt { QueueId = 2, QueueOffset = 200 };
        msg.PopCheckPoint = "200 1700000000000 60000 1 broker-a 2";
        var result = await consumer.ChangeInvisibleTimeAsync("nonexistent", msg, 30000);
        Assert.False(result);
    }

    [Fact]
    [DisplayName("MessageExt_PopCheckPoint属性读写正常")]
    public void MessageExt_PopCheckPoint_GetSet()
    {
        var msg = new MessageExt();
        Assert.Null(msg.PopCheckPoint);

        msg.PopCheckPoint = "100 1700000000000 60000 1 broker-a 1";
        Assert.Equal("100 1700000000000 60000 1 broker-a 1", msg.PopCheckPoint);
        Assert.Equal("100 1700000000000 60000 1 broker-a 1", msg.Properties["POP_CK"]);
    }

    [Fact]
    [DisplayName("RequestCode包含Pop消费相关码")]
    public void RequestCode_ContainsPopCodes()
    {
        Assert.Equal(200050, (Int32)RequestCode.POP_MESSAGE);
        Assert.Equal(200051, (Int32)RequestCode.ACK_MESSAGE);
        Assert.Equal(200052, (Int32)RequestCode.CHANGE_MESSAGE_INVISIBLETIME);
        Assert.Equal(200151, (Int32)RequestCode.BATCH_ACK_MESSAGE);
    }
}
