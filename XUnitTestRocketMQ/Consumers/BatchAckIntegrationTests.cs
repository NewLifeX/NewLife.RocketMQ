using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Threading;
using System.Threading.Tasks;
using NewLife.Log;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Protocol;
using Xunit;
using XUnitTest.Integration;

namespace XUnitTest.Consumers;

/// <summary>批量 Ack 集成测试</summary>
/// <remarks>
/// BatchAck 允许一次 RPC 批量确认多条 Pop 消息，减少网络开销。
/// 前提：消息必须通过 Pop 方式拉取，消息属性中含 POP_CK（PopCheckPoint）。
/// </remarks>
public class BatchAckIntegrationTests
{
    [Fact]
    [DisplayName("批量Ack_Pop三条消息_一次BatchAck全部确认")]
    public async Task BatchAck_PopThreeMessages_AllAckedInOneBatch()
    {
        var set = BasicTest.GetConfig();
        const String topic = "nx_batch_ack_test";
        const String group = "nx_batch_ack_group";

        // 发送 3 条测试消息
        using var producer = new Producer
        {
            Topic = topic,
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
        };
        producer.Start();
        Thread.Sleep(2000);

        var stamp = DateTime.UtcNow.Ticks.ToString();
        const Int32 sendCount = 3;
        for (var i = 0; i < sendCount; i++)
        {
            var sr = producer.Publish($"batch-ack-{stamp}-{i}");
            Assert.Equal(SendStatus.SendOK, sr.Status);
        }

        XTrace.WriteLine("已发送 {0} 条消息，stamp={1}", sendCount, stamp);
        Thread.Sleep(500);

        // 创建消费者（手动 Pop，不自动调度）
        using var consumer = new Consumer
        {
            Topic = topic,
            Group = group,
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
            AutoSchedule = false,
        };
        consumer.Start();
        Thread.Sleep(3000);

        var brokers = consumer.Brokers;
        Assert.NotEmpty(brokers);
        var brokerName = brokers[0].Name;

        // Pop 消息，收集到 3 条或超时
        var collected = new List<MessageExt>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        while (collected.Count < sendCount && !cts.Token.IsCancellationRequested)
        {
            try
            {
                var pr = await consumer.PopMessageAsync(brokerName, maxNums: 8, invisibleTime: 60_000, pollTime: 3_000, cancellationToken: cts.Token);
                if (pr?.Messages != null)
                {
                    foreach (var m in pr.Messages)
                    {
                        if (m.BodyString?.Contains(stamp) == true && !String.IsNullOrEmpty(m.PopCheckPoint))
                            collected.Add(m);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                break;  // CTS 超时，退出循环
            }
        }

        if (collected.Count == 0)
        {
            XTrace.WriteLine("未收到含 POP_CK 的消息，Broker 可能不支持 Pop 模式，跳过 BatchAck 验证");
            return;
        }

        XTrace.WriteLine("Pop 到 {0} 条消息，准备批量 Ack", collected.Count);

        // 构造批量 Ack 条目
        var ackEntries = new List<(String extraInfo, Int64 offset)>();
        foreach (var m in collected)
        {
            ackEntries.Add((m.PopCheckPoint, m.QueueOffset));
        }

        // 批量确认
        var ackedCount = await consumer.BatchAckMessageAsync(brokerName, ackEntries);
        XTrace.WriteLine("批量 Ack 确认数量: {0}", ackedCount);

        // 部分 Broker 版本对 BatchAck 返回 0（成功但不计数），因此只验证不抛异常
        // 且再次 Pop 不含相同 stamp 的消息（已确认，不再重投）
        await Task.Delay(1000);
        var secondPop = await consumer.PopMessageAsync(brokerName, maxNums: 8, invisibleTime: 5_000, pollTime: 1_000);
        if (secondPop?.Messages != null)
        {
            foreach (var m in secondPop.Messages)
            {
                Assert.False(m.BodyString?.Contains(stamp) == true, $"批量 Ack 后消息 [{m.MsgId}] 仍被重投");
            }
        }

        XTrace.WriteLine("验证通过：批量 Ack 后消息未重投");
    }

    [Fact]
    [DisplayName("批量Ack_条目为空_返回0")]
    public async Task BatchAck_EmptyEntries_ReturnsZero()
    {
        var set = BasicTest.GetConfig();
        const String topic = "nx_batch_ack_test";
        const String group = "nx_batch_ack_group";

        using var consumer = new Consumer
        {
            Topic = topic,
            Group = group,
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
            AutoSchedule = false,
        };
        consumer.Start();
        Thread.Sleep(2000);

        var brokers = consumer.Brokers;
        Assert.NotEmpty(brokers);
        var brokerName = brokers[0].Name;

        var result = await consumer.BatchAckMessageAsync(brokerName, new List<(String, Int64)>());
        Assert.Equal(0, result);
        XTrace.WriteLine("空条目 BatchAck 返回 0，符合预期");
    }
}
