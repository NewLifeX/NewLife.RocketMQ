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

/// <summary>Pop 消费端到端集成测试</summary>
/// <remarks>
/// Pop 消费是 RocketMQ 4.9+ 引入的轻量消费模式：
/// 1. 无需客户端 Rebalance，由 Broker 自动分配
/// 2. 消息被拉取后进入不可见期（invisibleTime），需手动确认
/// 3. 超时未确认消息自动重新可见，可被再次消费
/// </remarks>
public class PopConsumeIntegrationTests
{
    [Fact]
    [DisplayName("Pop消费_发送消息_Pop后确认_消息不再重投")]
    public async Task PopConsume_SendAndAck_MessageNotRedelivered()
    {
        var set = BasicTest.GetConfig();
        const String topic = "nx_pop_test";
        const String group = "nx_pop_consumer_group";

        // 预创建 Topic 并发送测试消息
        using var producer = new Producer
        {
            Topic = topic,
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
        };
        producer.Start();
        Thread.Sleep(2000);

        var stamp = DateTime.UtcNow.Ticks.ToString();
        var body = $"pop-test-{stamp}";
        var sr = producer.Publish(body);
        Assert.Equal(SendStatus.SendOK, sr.Status);
        XTrace.WriteLine("发送消息: {0}", body);

        Thread.Sleep(500);

        // 创建消费者（不自动调度，手动 Pop）
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

        // 获取 Broker 名称
        var brokers = consumer.Brokers;
        Assert.NotEmpty(brokers);
        var brokerName = brokers[0].Name;

        // Pop 消息（最多等 10 秒，不可见时间 30 秒）
        PullResult popResult = null;
        MessageExt targetMsg = null;

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        while (!cts.Token.IsCancellationRequested)
        {
            try
            {
                popResult = await consumer.PopMessageAsync(brokerName, maxNums: 8, invisibleTime: 30_000, pollTime: 3_000, cancellationToken: cts.Token);
                if (popResult?.Messages != null)
                    targetMsg = Array.Find(popResult.Messages, m => m.BodyString?.Contains(stamp) == true);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            if (targetMsg != null) break;
        }

        Assert.NotNull(targetMsg);
        XTrace.WriteLine("Pop 到消息: MsgId={0}, Body={1}, POP_CK={2}", targetMsg.MsgId, targetMsg.BodyString, targetMsg.PopCheckPoint);

        // 若 Broker 不支持 Pop 模式（POP_CK 缺失），跳过 Ack 验证
        if (String.IsNullOrEmpty(targetMsg.PopCheckPoint))
        {
            XTrace.WriteLine("Broker 未返回 POP_CK 属性，可能不支持 Pop 模式，跳过 Ack 验证");
            return;
        }

        // 确认消费（Ack）
        var ackOk = await consumer.AckMessageAsync(brokerName, targetMsg);
        Assert.True(ackOk, "Ack 确认失败");
        XTrace.WriteLine("Ack 确认成功: MsgId={0}", targetMsg.MsgId);

        // 等 2 秒后再次 Pop，验证已确认的消息不再重投（在不可见期内且已 Ack）
        await Task.Delay(2000);
        var secondPop = await consumer.PopMessageAsync(brokerName, maxNums: 8, invisibleTime: 5_000, pollTime: 1_000);
        var duplicate = secondPop?.Messages != null ? Array.Find(secondPop.Messages, m => m.BodyString?.Contains(stamp) == true) : null;
        Assert.Null(duplicate);
        XTrace.WriteLine("验证通过：已 Ack 消息未重投");
    }

    [Fact]
    [DisplayName("Pop消费_修改不可见时间_消息延迟重投")]
    public async Task PopConsume_ChangeInvisibleTime_MessageReappearsLater()
    {
        var set = BasicTest.GetConfig();
        const String topic = "nx_pop_invisible_test";
        const String group = "nx_pop_invisible_group";

        using var producer = new Producer
        {
            Topic = topic,
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
        };
        producer.Start();

        var stamp = DateTime.UtcNow.Ticks.ToString();
        producer.Publish($"invisible-test-{stamp}");
        Thread.Sleep(500);

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

        // Pop 消息，设置不可见时间 60 秒
        MessageExt targetMsg = null;
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        while (!cts.Token.IsCancellationRequested)
        {
            try
            {
                var pr = await consumer.PopMessageAsync(brokerName, maxNums: 8, invisibleTime: 60_000, pollTime: 3_000, cancellationToken: cts.Token);
                targetMsg = pr?.Messages != null ? Array.Find(pr.Messages, m => m.BodyString?.Contains(stamp) == true) : null;
            }
            catch (OperationCanceledException)
            {
                break;
            }

            if (targetMsg != null) break;
        }

        Assert.NotNull(targetMsg);
        XTrace.WriteLine("Pop 到消息: MsgId={0}, POP_CK={1}", targetMsg.MsgId, targetMsg.PopCheckPoint);

        // 若 Broker 不支持 Pop 模式，跳过后续验证
        if (String.IsNullOrEmpty(targetMsg.PopCheckPoint))
        {
            XTrace.WriteLine("Broker 未返回 POP_CK，跳过修改不可见时间测试");
            return;
        }

        // 修改不可见时间为 2 秒（让消息很快重新可见）
        var changed = await consumer.ChangeInvisibleTimeAsync(brokerName, targetMsg, 2_000);
        XTrace.WriteLine("修改不可见时间结果: {0}", changed);

        // 等 4 秒让消息重新可见
        await Task.Delay(4000);

        // 再次 Pop，应能再次拿到该消息（因为修改为 2s 不可见且未 Ack）
        MessageExt reappeared = null;
        using var cts2 = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        while (!cts2.Token.IsCancellationRequested)
        {
            try
            {
                var pr2 = await consumer.PopMessageAsync(brokerName, maxNums: 8, invisibleTime: 5_000, pollTime: 3_000, cancellationToken: cts2.Token);
                reappeared = pr2?.Messages != null ? Array.Find(pr2.Messages, m => m.BodyString?.Contains(stamp) == true) : null;
            }
            catch (OperationCanceledException)
            {
                break;
            }

            if (reappeared != null) break;
        }

        Assert.NotNull(reappeared);
        XTrace.WriteLine("验证通过：修改不可见时间后消息重新可见: MsgId={0}", reappeared.MsgId);

        // 最终 Ack 清理
        await consumer.AckMessageAsync(brokerName, reappeared);
    }
}
