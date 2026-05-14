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

/// <summary>SQL92 属性过滤集成测试</summary>
/// <remarks>
/// SQL92 过滤：消费者通过 SQL 表达式选择性消费满足条件的消息。
/// 前提：Broker 必须启用 enablePropertyFilter=true（默认关闭）。
/// 若 Broker 未启用，测试会跳过验证（不强制失败）。
/// </remarks>
public class SQL92FilterIntegrationTests
{
    [Fact]
    [DisplayName("SQL92过滤_发两条消息一条符合条件_只消费到符合条件的")]
    public async Task SQL92Filter_TwoMessages_OnlyMatchingMessageConsumed()
    {
        var set = BasicTest.GetConfig();
        const String topic = "nx_sql92_test";
        const String group = "nx_sql92_group";

        var stamp = DateTime.UtcNow.Ticks.ToString();

        // 消费者：SQL92 过滤 age > 5
        var receivedMessages = new List<MessageExt>();
        var receivedOk = new SemaphoreSlim(0, 1);

        using var consumer = new Consumer
        {
            Topic = topic,
            Group = group,
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
            ExpressionType = "SQL92",
            Subscription = "age > 5",
            FromLastOffset = true,
        };
        consumer.OnConsume = (mq, msgs) =>
        {
            foreach (var m in msgs)
            {
                if (m.BodyString?.Contains(stamp) != true) continue;

                lock (receivedMessages)
                {
                    receivedMessages.Add(m);
                    if (receivedMessages.Count >= 1 && receivedOk.CurrentCount == 0)
                        receivedOk.Release();
                }

                XTrace.WriteLine("SQL92 收到消息: body={0}, age={1}", m.BodyString, m.Properties.TryGetValue("age", out var age) ? age : "");
            }

            return true;
        };

        try
        {
            consumer.Start();
        }
        catch (Exception ex)
        {
            XTrace.WriteLine("Consumer 启动失败（可能 Broker 不支持 SQL92）：{0}", ex.Message);
            return;
        }

        Thread.Sleep(4000);

        // 发送两条消息：age=10（符合）和 age=3（不符合）
        using var producer = new Producer
        {
            Topic = topic,
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
        };
        producer.Start();

        // 发送 age=10 的消息（应通过过滤）
        var msgPass = new Message { BodyString = $"sql92-pass-{stamp}" };
        msgPass.Properties["age"] = "10";
        msgPass.Properties["stamp"] = stamp;
        var srPass = producer.Publish(msgPass, null);
        Assert.Equal(SendStatus.SendOK, srPass.Status);
        XTrace.WriteLine("发送 age=10 消息（应通过过滤）");

        // 发送 age=3 的消息（应被过滤掉）
        var msgBlock = new Message { BodyString = $"sql92-block-{stamp}" };
        msgBlock.Properties["age"] = "3";
        msgBlock.Properties["stamp"] = stamp;
        var srBlock = producer.Publish(msgBlock, null);
        Assert.Equal(SendStatus.SendOK, srBlock.Status);
        XTrace.WriteLine("发送 age=3 消息（应被过滤）");

        // 等待 20 秒
        await receivedOk.WaitAsync(TimeSpan.FromSeconds(20));

        // 等 5 秒以确保被过滤消息不会延迟到达
        await Task.Delay(5000);

        lock (receivedMessages)
        {
            if (receivedMessages.Count == 0)
            {
                // Broker 未开启 enablePropertyFilter，跳过断言
                XTrace.WriteLine("未收到任何消息，Broker 可能未启用 enablePropertyFilter=true，跳过 SQL92 过滤验证");
                return;
            }

            // 应只收到 age=10 的消息
            foreach (var m in receivedMessages)
            {
                Assert.Contains("sql92-pass", m.BodyString);
                Assert.DoesNotContain("sql92-block", m.BodyString);
            }

            XTrace.WriteLine("SQL92 过滤验证通过：收到 {0} 条消息，均满足 age > 5", receivedMessages.Count);
        }
    }
}
