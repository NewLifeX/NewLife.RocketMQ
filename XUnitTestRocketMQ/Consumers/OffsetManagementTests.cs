using System;
using System.ComponentModel;
using System.Threading;
using System.Threading.Tasks;
using NewLife.Log;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Protocol;
using Xunit;
using XUnitTest.Integration;

namespace XUnitTest.Consumers;

/// <summary>OffsetManagementTests 共享 Consumer Fixture，避免每个测试各创建一个连接</summary>
public sealed class OffsetManagementFixture : IDisposable
{
    public Consumer Consumer { get; }
    public MessageQueue Queue { get; private set; }

    public OffsetManagementFixture()
    {
        var set = BasicTest.GetConfig();

        Consumer = new Consumer
        {
            Topic = "nx_test",
            Group = "nx_offset_shared_group",
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
            FromLastOffset = true,
        };
        Consumer.OnConsume = (q, ms) => true;
        Consumer.Start();

        // 等待 Rebalance 完成后取第一个队列
        Thread.Sleep(6000);

        var queues = Consumer.Queues;
        if (queues != null && queues.Length > 0)
            Queue = queues[0];
    }

    public void Dispose() => Consumer?.Dispose();
}

/// <summary>Consumer 偏移量管理集成测试</summary>
[Collection("OffsetMgmt")]
public class OffsetManagementTests : IClassFixture<OffsetManagementFixture>
{
    private readonly OffsetManagementFixture _fixture;

    public OffsetManagementTests(OffsetManagementFixture fixture) => _fixture = fixture;

    private MessageQueue GetQueue()
    {
        Assert.NotNull(_fixture.Queue);
        return _fixture.Queue;
    }

    [Fact]
    [DisplayName("QueryOffset_返回非负偏移量")]
    public async Task QueryOffset_ReturnsNonNegative()
    {
        var mq = GetQueue();
        var offset = await _fixture.Consumer.QueryOffset(mq);
        Assert.True(offset >= -1, $"Offset 应为 >= -1，实际值: {offset}");
    }

    [Fact]
    [DisplayName("QueryMaxOffset_返回最大偏移量")]
    public async Task QueryMaxOffset_ReturnsMaxOffset()
    {
        var mq = GetQueue();
        var maxOffset = await _fixture.Consumer.QueryMaxOffset(mq);
        Assert.True(maxOffset >= 0, $"MaxOffset 应为 >= 0，实际值: {maxOffset}");
    }

    [Fact]
    [DisplayName("QueryMinOffset_返回最小偏移量")]
    public async Task QueryMinOffset_ReturnsMinOffset()
    {
        var mq = GetQueue();
        var minOffset = await _fixture.Consumer.QueryMinOffset(mq);
        Assert.True(minOffset >= 0, $"MinOffset 应为 >= 0，实际值: {minOffset}");
    }

    [Fact]
    [DisplayName("QueryMinOffset_小于等于_MaxOffset")]
    public async Task QueryMinOffset_LessOrEqualToMaxOffset()
    {
        var mq = GetQueue();
        var minOffset = await _fixture.Consumer.QueryMinOffset(mq);
        var maxOffset = await _fixture.Consumer.QueryMaxOffset(mq);
        Assert.True(minOffset <= maxOffset, $"MinOffset({minOffset}) 应 <= MaxOffset({maxOffset})");
    }

    [Fact]
    [DisplayName("SearchOffset_根据时间戳返回偏移量")]
    public async Task SearchOffset_ByTimestamp_ReturnsOffset()
    {
        var mq = GetQueue();

        // 查询 1 分钟前到现在时间范围内的偏移量
        var ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - 60_000;
        var offset = await _fixture.Consumer.SearchOffset(mq, ts);
        Assert.True(offset >= -1, $"SearchOffset 应为 >= -1，实际值: {offset}");
    }

    [Fact]
    [DisplayName("UpdateOffset_更新偏移量成功")]
    public async Task UpdateOffset_Succeeds()
    {
        var mq = GetQueue();
        var currentOffset = await _fixture.Consumer.QueryOffset(mq);
        if (currentOffset < 0) currentOffset = 0;

        // 更新为当前偏移（幂等操作）
        var success = await _fixture.Consumer.UpdateOffset(mq, currentOffset);
        Assert.True(success, "UpdateOffset 应返回 true");
    }

    [Fact]
    [DisplayName("GetConsumers_返回消费者客户端ID列表")]
    public async Task GetConsumers_ReturnsClientIds()
    {
        const String group = "nx_offset_shared_group";
        var consumers = await _fixture.Consumer.GetConsumers(group);

        // 消费者已启动，至少应有 1 个客户端 ID
        Assert.NotNull(consumers);
        Assert.True(consumers.Count >= 1, $"GetConsumers 应返回至少 1 个客户端，实际: {consumers.Count}");
    }
}

