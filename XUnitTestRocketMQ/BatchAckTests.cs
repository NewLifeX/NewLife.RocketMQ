using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Threading.Tasks;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Protocol;
using Xunit;

namespace XUnitTestRocketMQ;

/// <summary>批量确认Pop消息测试</summary>
public class BatchAckTests
{
    [Fact]
    [DisplayName("BatchAckMessageAsync_Null的BrokerName抛出异常")]
    public async Task BatchAckMessageAsync_NullBrokerName_ThrowsException()
    {
        using var consumer = new Consumer();
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            consumer.BatchAckMessageAsync(null, new List<(String, Int64)> { ("extra", 0) }));
    }

    [Fact]
    [DisplayName("BatchAckMessageAsync_空BrokerName抛出异常")]
    public async Task BatchAckMessageAsync_EmptyBrokerName_ThrowsException()
    {
        using var consumer = new Consumer();
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            consumer.BatchAckMessageAsync("", new List<(String, Int64)> { ("extra", 0) }));
    }

    [Fact]
    [DisplayName("BatchAckMessageAsync_空条目列表返回0")]
    public async Task BatchAckMessageAsync_EmptyEntries_ReturnsZero()
    {
        using var consumer = new Consumer();
        var result = await consumer.BatchAckMessageAsync("broker1", new List<(String, Int64)>());
        Assert.Equal(0, result);
    }

    [Fact]
    [DisplayName("BatchAckMessageAsync_Null条目列表返回0")]
    public async Task BatchAckMessageAsync_NullEntries_ReturnsZero()
    {
        using var consumer = new Consumer();
        var result = await consumer.BatchAckMessageAsync("broker1", null);
        Assert.Equal(0, result);
    }

    [Fact]
    [DisplayName("BatchAckMessageAsync_无Broker连接时返回0")]
    public async Task BatchAckMessageAsync_NoBroker_ReturnsZero()
    {
        using var consumer = new Consumer();
        // 未Start，无Broker连接
        var entries = new List<(String extraInfo, Int64 offset)>
        {
            ("extra1", 100),
            ("extra2", 200),
        };
        var result = await consumer.BatchAckMessageAsync("nonexistent", entries);
        Assert.Equal(0, result);
    }

    [Fact]
    [DisplayName("RequestCode包含BATCH_ACK_MESSAGE")]
    public void RequestCode_ContainsBatchAckMessage()
    {
        Assert.Equal(200151, (Int32)RequestCode.BATCH_ACK_MESSAGE);
    }
}
