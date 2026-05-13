using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Threading;
using System.Threading.Tasks;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Protocol;
using Xunit;

namespace XUnitTest.Integration;

/// <summary>Producer 集成测试</summary>
/// <remarks>
/// 需要本机启动 RocketMQ 并设置环境变量 ROCKETMQ_NAMESERVER=127.0.0.1:9876。
/// 启动命令：dotnet run --file scripts/RocketMqSetup.cs
/// </remarks>
/// <remarks>初始化</remarks>
/// <param name="fixture">RocketMQ Fixture</param>
[Collection("RocketMQ")]
public class ProducerIntegrationTests(RocketMqFixture fixture) : IClassFixture<RocketMqFixture>
{
    [SkippableFact]
    [DisplayName("发送普通消息_返回SendOK")]
    public async Task PublishMessage_ReturnsSuccess()
    {
        fixture.SkipIfUnavailable();

        using var producer = new Producer
        {
            Topic             = "integration-test-topic",
            NameServerAddress = fixture.NameServerAddress,
        };
        producer.Start();

        var result = await producer.PublishAsync("Hello from integration test");
        Assert.Equal(SendStatus.SendOK, result.Status);
        Assert.NotEmpty(result.MsgId);
    }

    [SkippableFact]
    [DisplayName("发送带属性的消息_属性正常保存")]
    public async Task PublishMessageWithProperties_PropertiesPreserved()
    {
        fixture.SkipIfUnavailable();

        using var producer = new Producer
        {
            Topic             = "integration-test-topic",
            NameServerAddress = fixture.NameServerAddress,
        };
        producer.Start();

        var msg = new Message
        {
            Topic = "integration-test-topic",
            Body  = "test body"u8.ToArray(),
            Keys  = "TestKey001",
            Tags  = "TagA",
        };

        var result = await producer.PublishAsync(msg, null);
        Assert.Equal(SendStatus.SendOK, result.Status);
    }

    [SkippableFact]
    [DisplayName("并发发送多条消息_全部成功")]
    public async Task PublishMessagesParallel_AllSucceed()
    {
        fixture.SkipIfUnavailable();

        using var producer = new Producer
        {
            Topic             = "integration-test-topic",
            NameServerAddress = fixture.NameServerAddress,
        };
        producer.Start();

        var tasks = new List<Task<SendResult>>();
        for (var i = 0; i < 10; i++)
        {
            tasks.Add(producer.PublishAsync($"Parallel message {i}"));
        }

        var results = await Task.WhenAll(tasks);
        foreach (var r in results)
        {
            Assert.Equal(SendStatus.SendOK, r.Status);
        }
    }
}

/// <summary>Consumer 集成测试</summary>
/// <remarks>初始化</remarks>
/// <param name="fixture">RocketMQ Fixture</param>
[Collection("RocketMQ")]
public class ConsumerIntegrationTests(RocketMqFixture fixture) : IClassFixture<RocketMqFixture>
{
    [SkippableFact]
    [DisplayName("先发再消费_能收到消息")]
    public async Task ProduceThenConsume_MessageReceived()
    {
        fixture.SkipIfUnavailable();

        const String topic   = "integration-consume-topic";
        const String content = "Hello Consumer";

        using var producer = new Producer
        {
            Topic             = topic,
            NameServerAddress = fixture.NameServerAddress,
        };
        producer.Start();
        await producer.PublishAsync(content);

        var tcs = new TaskCompletionSource<MessageExt>();
        using var consumer = new Consumer
        {
            Topic             = topic,
            Group             = "integration-consumer-group",
            NameServerAddress = fixture.NameServerAddress,
        };

        consumer.OnConsume = (queue, msgs) =>
        {
            foreach (var m in msgs)
            {
                if (!tcs.Task.IsCompleted)
                    tcs.TrySetResult(m);
            }
            return true;
        };
        consumer.Start();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        cts.Token.Register(() => tcs.TrySetCanceled());

        var received = await tcs.Task;
        Assert.NotNull(received);
        Assert.Equal(content, received.BodyString);
    }
}
