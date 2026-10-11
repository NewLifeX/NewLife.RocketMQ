using System;
using System.ComponentModel;
using System.Linq;
using System.Net.Sockets;
using System.Threading.Tasks;
using NewLife;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Grpc;
using NewLife.RocketMQ.Models;
using NewLife.RocketMQ.Protocol;
using Xunit;

namespace XUnitTest.Integration;

/// <summary>gRPC（5.x Proxy）集成测试</summary>
/// <remarks>
/// 需要本机同时运行 RocketMQ 与 gRPC Proxy，并设置环境变量 ROCKETMQ_GRPC_PROXY=http://127.0.0.1:8081。
/// 未设置或 Proxy 不可达时用例自动跳过（Skippable），不影响其他集成测试。
/// 本机启动：dotnet run --file scripts/RocketMqSetup.cs（Broker）+ bin/mqproxy -n 127.0.0.1:9876。
/// </remarks>
/// <remarks>初始化</remarks>
/// <param name="fixture">RocketMQ Fixture</param>
[Collection("RocketMQ")]
public class GrpcIntegrationTests(RocketMqFixture fixture)
{
    /// <summary>gRPC Proxy 地址（环境变量 ROCKETMQ_GRPC_PROXY）</summary>
    public static String? ProxyAddress => Environment.GetEnvironmentVariable("ROCKETMQ_GRPC_PROXY");

    private const String _topic = "grpc-integration-topic";

    /// <summary>Proxy 是否可连接</summary>
    private static Boolean ProxyAvailable()
    {
        var address = ProxyAddress;
        if (address.IsNullOrEmpty()) return false;

        try
        {
            var uri = new Uri(NewLife.RocketMQ.Grpc.GrpcClient.NormalizeAddress(address));
            using var tcp = new TcpClient();
            tcp.Connect(uri.Host, uri.IsDefaultPort ? 8081 : uri.Port);
            return true;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>建主题与基础生产者，保证主题存在（gRPC 不提供建主题接口）</summary>
    private Producer CreateTopic()
    {
        var producer = new Producer { Topic = _topic, NameServerAddress = fixture.NameServerAddress };
        producer.Start();
        producer.CreateTopic(_topic, 4);

        return producer;
    }

    [SkippableFact]
    [DisplayName("gRPC_路由查询_返回消息队列")]
    public async Task QueryRouteViaGrpc_ReturnsQueues()
    {
        Skip.IfNot(ProxyAvailable(), "未配置或无法连接 gRPC Proxy（ROCKETMQ_GRPC_PROXY）");
        fixture.EnsureAvailable();

        using var warm = CreateTopic();

        using var consumer = new Consumer
        {
            Topic = _topic,
            Group = "grpc-route-" + Guid.NewGuid().ToString("N")[..8],
            NameServerAddress = fixture.NameServerAddress,
            GrpcProxyAddress = ProxyAddress,
        };
        consumer.Start();

        var route = await consumer.QueryRouteViaGrpcAsync(_topic);

        Assert.Equal(GrpcCode.OK, route.Status?.Code);
        Assert.NotEmpty(route.MessageQueues);
    }

    [SkippableFact]
    [DisplayName("gRPC_发送消息_返回SendOK")]
    public async Task PublishViaGrpc_ReturnsSuccess()
    {
        Skip.IfNot(ProxyAvailable(), "未配置或无法连接 gRPC Proxy（ROCKETMQ_GRPC_PROXY）");
        fixture.EnsureAvailable();

        using var warm = CreateTopic();

        using var producer = new Producer
        {
            Topic = _topic,
            Group = "grpc-producer",
            NameServerAddress = fixture.NameServerAddress,
            GrpcProxyAddress = ProxyAddress,
        };
        producer.Start();

        var result = await producer.PublishAsync("gRPC 发送 " + DateTime.Now.Ticks);

        Assert.Equal(SendStatus.SendOK, result.Status);
        Assert.NotEmpty(result.MsgId);
    }

    [SkippableFact]
    [DisplayName("gRPC_同步发送_返回SendOK")]
    public void SyncPublishViaGrpc_ReturnsSuccess()
    {
        Skip.IfNot(ProxyAvailable(), "未配置或无法连接 gRPC Proxy（ROCKETMQ_GRPC_PROXY）");
        fixture.EnsureAvailable();

        using var warm = CreateTopic();

        using var producer = new Producer
        {
            Topic = _topic,
            Group = "grpc-producer",
            NameServerAddress = fixture.NameServerAddress,
            GrpcProxyAddress = ProxyAddress,
        };
        producer.Start();

        // 同步入口由 Proxy 选择队列，曾因本地队列选择而崩溃
        var result = producer.Publish("gRPC 同步发送 " + DateTime.Now.Ticks);

        Assert.Equal(SendStatus.SendOK, result.Status);
        Assert.NotEmpty(result.MsgId);
    }

    [SkippableFact]
    [DisplayName("gRPC_系统属性_不重复下发")]
    public void SyncPublishViaGrpc_SystemPropertiesFiltered()
    {
        Skip.IfNot(ProxyAvailable(), "未配置或无法连接 gRPC Proxy（ROCKETMQ_GRPC_PROXY）");
        fixture.EnsureAvailable();

        using var warm = CreateTopic();

        using var producer = new Producer
        {
            Topic = _topic,
            Group = "grpc-producer",
            NameServerAddress = fixture.NameServerAddress,
            GrpcProxyAddress = ProxyAddress,
        };
        producer.Start();

        // TAGS/KEYS 由独立字段传递，重复放入 properties 会被 Proxy 以保留键拒绝
        var message = new Message { Tags = "grpc-tag", Keys = "grpc-key" };
        message.SetBody("gRPC 系统属性 " + DateTime.Now.Ticks);
        message.Properties["customKey"] = "customValue";

        var result = producer.Publish(message, null);

        Assert.Equal(SendStatus.SendOK, result.Status);
        Assert.NotEmpty(result.MsgId);
    }

    [SkippableFact]
    [DisplayName("gRPC_延迟消息_返回SendOK")]
    public void DelayViaGrpc_ReturnsSuccess()
    {
        Skip.IfNot(ProxyAvailable(), "未配置或无法连接 gRPC Proxy（ROCKETMQ_GRPC_PROXY）");
        fixture.EnsureAvailable();

        const String delayTopic = "grpc-integration-delay-topic";
        EnsureTopicAsync(delayTopic, 1, TopicMessageType.Delay).GetAwaiter().GetResult();

        using var producer = new Producer
        {
            Topic = delayTopic,
            Group = "grpc-producer",
            NameServerAddress = fixture.NameServerAddress,
            GrpcProxyAddress = ProxyAddress,
        };
        producer.Start();

        // gRPC 用投递时间戳表达延迟，等级需换算为延迟时长
        var message = new Message();
        message.SetBody("gRPC 延迟消息 " + DateTime.Now.Ticks);

        var result = producer.PublishDelay(message, null, DelayTimeLevels.S5);

        Assert.Equal(SendStatus.SendOK, result.Status);
        Assert.NotEmpty(result.MsgId);
    }

    [SkippableFact]
    [DisplayName("gRPC_接收与确认_收到已投递消息")]
    public async Task ReceiveAndAckViaGrpc_ReturnsMessage()
    {
        Skip.IfNot(ProxyAvailable(), "未配置或无法连接 gRPC Proxy（ROCKETMQ_GRPC_PROXY）");
        fixture.EnsureAvailable();

        var body = "gRPC 接收 " + DateTime.Now.Ticks;

        // 先建主题并由 Remoting 投递标记消息
        using (var warm = CreateTopic())
        {
            var sent = await warm.PublishAsync(body);
            Assert.Equal(SendStatus.SendOK, sent.Status);
        }

        using var consumer = new Consumer
        {
            Topic = _topic,
            Group = "grpc-receive-" + Guid.NewGuid().ToString("N")[..8],
            NameServerAddress = fixture.NameServerAddress,
            GrpcProxyAddress = ProxyAddress,
        };
        consumer.Start();

        var route = await consumer.QueryRouteViaGrpcAsync(_topic);
        Assert.Equal(GrpcCode.OK, route.Status?.Code);

        // 资源上报长连接需要片刻完成 Proxy 侧注册，接收路径也会等待就绪
        // 主题被多个用例共用，需持续轮询直到出现目标消息而非收到任意消息就停
        var messages = new System.Collections.Generic.List<GrpcMessage>();
        for (var i = 0; i < 8 && !messages.Any(m => m.Body?.ToStr() == body); i++)
        {
            foreach (var queue in route.MessageQueues)
            {
                var list = await consumer.ReceiveMessageViaGrpcAsync(queue, batchSize: 8, longPollingTimeout: TimeSpan.FromSeconds(2));
                messages.AddRange(list);
            }

            if (!messages.Any(m => m.Body?.ToStr() == body)) await Task.Delay(1000);
        }

        Assert.Contains(messages, m => m.Body?.ToStr() == body);

        var target = messages.First(m => m.Body?.ToStr() == body);
        var ack = await consumer.AckMessageViaGrpcAsync(_topic,
        [
            new AckMessageEntry
            {
                ReceiptHandle = target.SystemProperties?.ReceiptHandle,
                MessageId = target.SystemProperties?.MessageId,
            },
        ]);

        Assert.Equal(GrpcCode.OK, ack.Status?.Code);
    }

    /// <summary>在所有队列中轮询，直到收到匹配消息或轮次耗尽</summary>
    private static async Task<GrpcMessage?> ReceiveUntilAsync(
        Consumer consumer,
        System.Collections.Generic.IList<GrpcMessageQueue> queues,
        Func<GrpcMessage, Boolean> match,
        Int32 rounds,
        TimeSpan invisibleDuration)
    {
        for (var i = 0; i < rounds; i++)
        {
            foreach (var queue in queues)
            {
                var list = await consumer.ReceiveMessageViaGrpcAsync(queue, batchSize: 8,
                    invisibleDuration: invisibleDuration, longPollingTimeout: TimeSpan.FromSeconds(2));
                foreach (var m in list)
                {
                    if (match(m)) return m;
                }
            }

            await Task.Delay(1000);
        }

        return null;
    }

    [SkippableFact]
    [DisplayName("gRPC_不可见时间到期_消息重投且投递次数递增")]
    public async Task InvisibleTimeout_RedeliversMessage()
    {
        Skip.IfNot(ProxyAvailable(), "未配置或无法连接 gRPC Proxy（ROCKETMQ_GRPC_PROXY）");
        fixture.EnsureAvailable();

        var body = "gRPC 重投 " + DateTime.Now.Ticks;
        using (var warm = CreateTopic())
        {
            var sent = await warm.PublishAsync(body);
            Assert.Equal(SendStatus.SendOK, sent.Status);
        }

        using var consumer = new Consumer
        {
            Topic = _topic,
            Group = "grpc-redeliver-" + Guid.NewGuid().ToString("N")[..8],
            NameServerAddress = fixture.NameServerAddress,
            GrpcProxyAddress = ProxyAddress,
        };
        consumer.Start();

        var route = await consumer.QueryRouteViaGrpcAsync(_topic);
        Assert.Equal(GrpcCode.OK, route.Status?.Code);

        // 接收后不确认，等待不可见时间（Broker 最小 10 秒）到期后由 Broker 重投
        var invisible = TimeSpan.FromSeconds(10);
        var first = await ReceiveUntilAsync(consumer, route.MessageQueues, m => m.Body?.ToStr() == body, rounds: 5, invisible);
        Assert.NotNull(first);
        var firstAttempt = first!.SystemProperties?.DeliveryAttempt ?? 0;

        // 重投可能换队列，需继续轮询全部队列
        var second = await ReceiveUntilAsync(consumer, route.MessageQueues, m => m.Body?.ToStr() == body, rounds: 20, invisible);
        Assert.NotNull(second);

        var secondAttempt = second!.SystemProperties?.DeliveryAttempt ?? 0;
        Assert.True(secondAttempt > firstAttempt, $"投递次数未递增：{firstAttempt} → {secondAttempt}");
    }

    [SkippableFact]
    [DisplayName("gRPC_达到最大投递次数_转发死信队列")]
    public async Task ForwardToDeadLetterQueue_ReturnsOk()
    {
        Skip.IfNot(ProxyAvailable(), "未配置或无法连接 gRPC Proxy（ROCKETMQ_GRPC_PROXY）");
        fixture.EnsureAvailable();

        var body = "gRPC 死信 " + DateTime.Now.Ticks;
        using (var warm = CreateTopic())
        {
            var sent = await warm.PublishAsync(body);
            Assert.Equal(SendStatus.SendOK, sent.Status);
        }

        var group = "grpc-dlq-" + Guid.NewGuid().ToString("N")[..8];
        using var consumer = new Consumer
        {
            Topic = _topic,
            Group = group,
            NameServerAddress = fixture.NameServerAddress,
            GrpcProxyAddress = ProxyAddress,
        };
        consumer.Start();

        var route = await consumer.QueryRouteViaGrpcAsync(_topic);
        var message = await ReceiveUntilAsync(consumer, route.MessageQueues, m => m.Body?.ToStr() == body, rounds: 5, TimeSpan.FromSeconds(10));
        Assert.NotNull(message);

        var attempt = message!.SystemProperties?.DeliveryAttempt ?? 1;
        var response = await consumer.ForwardToDeadLetterQueueViaGrpcAsync(
            _topic,
            message.SystemProperties!.ReceiptHandle,
            message.SystemProperties.MessageId,
            attempt,
            attempt);

        Assert.Equal(GrpcCode.OK, response.Status?.Code);
    }

    [SkippableFact]
    [DisplayName("gRPC_推送消费失败_自动重投")]
    public async Task PushConsumer_RetryOnFailure()
    {
        Skip.IfNot(ProxyAvailable(), "未配置或无法连接 gRPC Proxy（ROCKETMQ_GRPC_PROXY）");
        fixture.EnsureAvailable();

        var body = "gRPC 推送重投 " + DateTime.Now.Ticks;
        using (var warm = CreateTopic())
        {
            var sent = await warm.PublishAsync(body);
            Assert.Equal(SendStatus.SendOK, sent.Status);
        }

        var attempts = new System.Collections.Generic.List<Int32>();
        var completed = new TaskCompletionSource<Boolean>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var consumer = new GrpcPushConsumer
        {
            Topic = _topic,
            Group = "grpc-push-retry-" + Guid.NewGuid().ToString("N")[..8],
            Endpoints = ProxyAddress,
            InvisibleDuration = TimeSpan.FromSeconds(10),
            RetryInvisibleDuration = TimeSpan.FromSeconds(10),
            MaxDeliveryAttempts = 0,
            OnMessage = msg =>
            {
                if (msg.Body?.ToStr() == body)
                {
                    lock (attempts)
                    {
                        attempts.Add(msg.SystemProperties?.DeliveryAttempt ?? 0);
                        if (attempts.Count >= 2) completed.TrySetResult(true);
                    }
                }

                // 一律失败，触发延迟重投
                return Task.FromResult(false);
            },
        };
        await consumer.StartAsync();

        var done = await Task.WhenAny(completed.Task, Task.Delay(TimeSpan.FromSeconds(90)));
        await consumer.StopAsync();

        Assert.True(done == completed.Task, "90 秒内未收到重投消息");
        Assert.True(attempts[1] > attempts[0], $"投递次数未递增：{attempts[0]} → {attempts[1]}");
    }

    [SkippableFact]
    [DisplayName("gRPC_并发消费失败_批量转入死信队列")]
    public async Task PushConsumer_BulkFailure_ForwardsToDlq()
    {
        Skip.IfNot(ProxyAvailable(), "未配置或无法连接 gRPC Proxy（ROCKETMQ_GRPC_PROXY）");
        fixture.EnsureAvailable();

        const Int32 total = 30;
        var tag = "bulk-dlq-" + DateTime.Now.Ticks;
        var group = "grpc-bulk-dlq-" + Guid.NewGuid().ToString("N")[..8];

        // 使用独立主题，避免失败消息残留影响其他用例
        var bulkTopic = "grpc-bulk-dlq-topic";

        // 并发投递，模拟高并发失败场景
        using (var warm = new Producer { Topic = bulkTopic, NameServerAddress = fixture.NameServerAddress })
        {
            warm.Start();
            warm.CreateTopic(bulkTopic, 4);

            var tasks = new System.Collections.Generic.List<Task>();
            for (var i = 0; i < total; i++)
            {
                var body = $"{tag}-{i}";
                tasks.Add(Task.Run(async () =>
                {
                    var sent = await warm.PublishAsync(body);
                    Assert.Equal(SendStatus.SendOK, sent.Status);
                }));
            }

            await Task.WhenAll(tasks);
        }

        // 推送消费一律失败：投递次数达到上限即转发死信
        var handled = new System.Collections.Generic.List<String>();
        using (var push = new GrpcPushConsumer
        {
            Topic = bulkTopic,
            Group = group,
            Endpoints = ProxyAddress,
            BatchSize = 32,
            InvisibleDuration = TimeSpan.FromSeconds(10),
            MaxDeliveryAttempts = 1,
            OnMessage = msg =>
            {
                var body = msg.Body?.ToStr() ?? "";
                if (body.StartsWith(tag, StringComparison.Ordinal))
                {
                    lock (handled) handled.Add(body);
                }

                return Task.FromResult(false);
            },
        })
        {
            await push.StartAsync();

            var watch = System.Diagnostics.Stopwatch.StartNew();
            while (watch.Elapsed < TimeSpan.FromSeconds(60) && handled.Count < total) await Task.Delay(500);

            await push.StopAsync();
        }

        Assert.True(handled.Count >= total, $"仅消费 {handled.Count}/{total} 条");

        // 死信队列：同一消费组可读取全部失败消息
        using var dlqConsumer = new Consumer
        {
            Topic = $"%DLQ%{group}",
            Group = group,
            NameServerAddress = fixture.NameServerAddress,
            GrpcProxyAddress = ProxyAddress,
        };
        dlqConsumer.Start();
        await Task.Delay(3000);

        var dlqRoute = await dlqConsumer.QueryRouteViaGrpcAsync($"%DLQ%{group}");
        Assert.Equal(GrpcCode.OK, dlqRoute.Status?.Code);

        var found = new System.Collections.Generic.List<String>();
        var ackEntries = new System.Collections.Generic.List<AckMessageEntry>();
        for (var round = 0; round < 8 && found.Count < total; round++)
        {
            foreach (var queue in dlqRoute.MessageQueues)
            {
                var messages = await dlqConsumer.ReceiveMessageViaGrpcAsync(queue, batchSize: 32,
                    invisibleDuration: TimeSpan.FromSeconds(30), longPollingTimeout: TimeSpan.FromSeconds(2));
                foreach (var m in messages)
                {
                    var body = m.Body?.ToStr() ?? "";
                    if (!body.StartsWith(tag, StringComparison.Ordinal) || found.Contains(body)) continue;

                    found.Add(body);
                    ackEntries.Add(new AckMessageEntry
                    {
                        ReceiptHandle = m.SystemProperties?.ReceiptHandle,
                        MessageId = m.SystemProperties?.MessageId,
                    });
                }
            }

            if (found.Count < total) await Task.Delay(1500);
        }

        Assert.Equal(total, found.Count);

        // 确认死信消息，避免残留影响后续运行
        if (ackEntries.Count > 0)
        {
            var ack = await dlqConsumer.AckMessageViaGrpcAsync($"%DLQ%{group}", ackEntries);
            Assert.Equal(GrpcCode.OK, ack.Status?.Code);
        }
    }

    /// <summary>建主题并等待路由就绪（新建主题的 NameServer 路由注册有延迟）</summary>
    private async Task EnsureTopicAsync(String topic, Int32 queueNum, TopicMessageType messageType)
    {
        using (var warm = new Producer { Topic = topic, NameServerAddress = fixture.NameServerAddress })
        {
            warm.Start();
            warm.CreateTopic(topic, queueNum, 0, messageType);
        }

        for (var i = 0; i < 45; i++)
        {
            try
            {
                using var probe = new Producer { Topic = topic, NameServerAddress = fixture.NameServerAddress };
                probe.Start();
                var sent = await probe.PublishAsync(new Message { Topic = topic, Body = "warm".GetBytes() }, null);
                if (sent.Status == SendStatus.SendOK) return;
            }
            catch
            {
                // 路由尚未就绪，重试
            }

            await Task.Delay(1000);
        }

        throw new Exception($"主题 {topic} 路由未在 45 秒内就绪");
    }

    [SkippableFact]
    [DisplayName("gRPC_事务消息_提交可见回滚不可见")]
    public async Task Transaction_CommitVisibleRollbackInvisible()
    {
        Skip.IfNot(ProxyAvailable(), "未配置或无法连接 gRPC Proxy（ROCKETMQ_GRPC_PROXY）");
        fixture.EnsureAvailable();

        var topic = "grpc-tx-integration-topic";
        var commitBody = "tx-commit-" + DateTime.Now.Ticks;
        var rollbackBody = "tx-rollback-" + DateTime.Now.Ticks;

        // 事务消息要求主题类型为 TRANSACTION
        await EnsureTopicAsync(topic, 4, TopicMessageType.Transaction);

        using var producer = new Producer
        {
            Topic = topic,
            Group = "grpc-tx-producer",
            NameServerAddress = fixture.NameServerAddress,
            GrpcProxyAddress = ProxyAddress,
        };
        producer.Start();

        var commit = await producer.PublishTransactionViaGrpcAsync(commitBody);
        var commitResult = await producer.EndTransactionViaGrpcAsync(commit.MsgId, commit.TransactionId, true);
        Assert.Equal(GrpcCode.OK, commitResult.Status?.Code);

        var rollback = await producer.PublishTransactionViaGrpcAsync(rollbackBody);
        var rollbackResult = await producer.EndTransactionViaGrpcAsync(rollback.MsgId, rollback.TransactionId, false);
        Assert.Equal(GrpcCode.OK, rollbackResult.Status?.Code);

        using var consumer = new Consumer
        {
            Topic = topic,
            Group = "grpc-tx-" + Guid.NewGuid().ToString("N")[..8],
            NameServerAddress = fixture.NameServerAddress,
            GrpcProxyAddress = ProxyAddress,
        };
        consumer.Start();
        await Task.Delay(3000);

        var route = await consumer.QueryRouteViaGrpcAsync(topic);
        var found = new System.Collections.Generic.List<String>();
        for (var i = 0; i < 8 && !found.Contains(commitBody); i++)
        {
            foreach (var queue in route.MessageQueues)
            {
                var messages = await consumer.ReceiveMessageViaGrpcAsync(queue, batchSize: 16,
                    invisibleDuration: TimeSpan.FromSeconds(10), longPollingTimeout: TimeSpan.FromSeconds(2));
                foreach (var m in messages)
                {
                    var body = m.Body?.ToStr() ?? "";
                    if (body.StartsWith("tx-", StringComparison.Ordinal) && !found.Contains(body)) found.Add(body);
                }
            }

            if (!found.Contains(commitBody)) await Task.Delay(1500);
        }

        Assert.Contains(commitBody, found);
        Assert.DoesNotContain(rollbackBody, found);
    }

    [SkippableFact]
    [DisplayName("gRPC_FIFO主题_顺序消费保持发送顺序")]
    public async Task Fifo_OrderlyConsume_PreservesOrder()
    {
        Skip.IfNot(ProxyAvailable(), "未配置或无法连接 gRPC Proxy（ROCKETMQ_GRPC_PROXY）");
        fixture.EnsureAvailable();

        const Int32 total = 20;
        var topic = "grpc-fifo-integration-topic";
        var messageGroup = "fifo-" + Guid.NewGuid().ToString("N")[..8];
        // 主题固定且单队列，消费组每次新建会重放历史消息，故用本次唯一标记区分
        var mark = "seq-" + Guid.NewGuid().ToString("N")[..6] + "-";

        // FIFO 主题用单队列保证全局顺序
        await EnsureTopicAsync(topic, 1, TopicMessageType.Fifo);

        using (var producer = new Producer
        {
            Topic = topic,
            Group = "grpc-fifo-producer",
            NameServerAddress = fixture.NameServerAddress,
            GrpcProxyAddress = ProxyAddress,
        })
        {
            producer.Start();
            for (var i = 0; i < total; i++)
            {
                var sent = await producer.PublishAsync(new Message
                {
                    Topic = topic,
                    Body = $"{mark}{i:d3}".GetBytes(),
                    MessageGroup = messageGroup,
                }, null);
                Assert.Equal(SendStatus.SendOK, sent.Status);
            }
        }

        var received = new System.Collections.Generic.List<Int32>();
        var completed = new TaskCompletionSource<Boolean>(TaskCreationOptions.RunContinuationsAsynchronously);
        using (var push = new GrpcPushConsumer
        {
            Topic = topic,
            Group = "grpc-fifo-consumer-" + Guid.NewGuid().ToString("N")[..8],
            Endpoints = ProxyAddress,
            Orderly = true,
            InvisibleDuration = TimeSpan.FromSeconds(30),
            OnMessage = msg =>
            {
                var body = msg.Body?.ToStr() ?? "";
                if (body.StartsWith(mark, StringComparison.Ordinal))
                {
                    lock (received)
                    {
                        received.Add(Int32.Parse(body[mark.Length..]));
                        if (received.Count >= total) completed.TrySetResult(true);
                    }
                }

                return Task.FromResult(true);
            },
        })
        {
            await push.StartAsync();

            var done = await Task.WhenAny(completed.Task, Task.Delay(TimeSpan.FromSeconds(60)));
            await push.StopAsync();

            Assert.True(done == completed.Task, $"60 秒内仅收到 {received.Count}/{total} 条");
        }

        // 顺序消费下应按发送顺序逐条到达
        for (var i = 0; i < total; i++)
        {
            Assert.Equal(i, received[i]);
        }
    }
}
