using System;
using System.ComponentModel;
using System.Linq;
using System.Net.Sockets;
using System.Threading.Tasks;
using NewLife;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Grpc;
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
        var messages = new System.Collections.Generic.List<GrpcMessage>();
        for (var i = 0; i < 5 && messages.Count == 0; i++)
        {
            foreach (var queue in route.MessageQueues)
            {
                var list = await consumer.ReceiveMessageViaGrpcAsync(queue, batchSize: 8, longPollingTimeout: TimeSpan.FromSeconds(2));
                if (list.Count > 0) { messages.AddRange(list); break; }
            }

            if (messages.Count == 0) await Task.Delay(1000);
        }

        Assert.Contains(messages, m => m.Body?.ToStr() == body);

        var ack = await consumer.AckMessageViaGrpcAsync(_topic,
        [
            .. messages.Select(m => new AckMessageEntry
            {
                ReceiptHandle = m.SystemProperties?.ReceiptHandle,
                MessageId = m.SystemProperties?.MessageId,
            }),
        ]);

        Assert.Equal(GrpcCode.OK, ack.Status?.Code);
    }
}
