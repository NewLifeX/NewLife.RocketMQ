using System;
using System.ComponentModel;
using System.Threading.Tasks;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Grpc;
using Xunit;

namespace XUnitTest.Protocol;

/// <summary>gRPC 客户端基础能力测试：超时头、访问点解析、资源上报设置、长连接会话</summary>
public class GrpcClientTests
{
    [Fact]
    [DisplayName("FormatTimeout_毫秒值直接输出")]
    public void FormatTimeout_Milliseconds()
    {
        Assert.Equal("30000m", GrpcClient.FormatTimeout(TimeSpan.FromSeconds(30)));
        Assert.Equal("1m", GrpcClient.FormatTimeout(TimeSpan.Zero));
        Assert.Equal("1m", GrpcClient.FormatTimeout(TimeSpan.FromMilliseconds(-5)));
    }

    [Fact]
    [DisplayName("FormatTimeout_超过八位自动升单位")]
    public void FormatTimeout_UpgradeUnit()
    {
        // 2 天 = 1.728 亿毫秒，超过八位数字，升为秒
        Assert.Equal("172800S", GrpcClient.FormatTimeout(TimeSpan.FromDays(2)));

        // 1200 天 = 1.0368 亿秒，超过八位数字，升为分钟
        Assert.Equal("1728000M", GrpcClient.FormatTimeout(TimeSpan.FromDays(1200)));
    }

    [Fact]
    [DisplayName("NormalizeAddress_缺少协议时补http")]
    public void NormalizeAddress_AddScheme()
    {
        Assert.Equal("http://127.0.0.1:8081", GrpcClient.NormalizeAddress("127.0.0.1:8081"));
        Assert.Equal("https://mq.example.com:8081", GrpcClient.NormalizeAddress("https://mq.example.com:8081"));

        Assert.Throws<ArgumentNullException>(() => GrpcClient.NormalizeAddress(null));
    }

    [Fact]
    [DisplayName("ParseEndpoints_带端口与协议")]
    public void ParseEndpoints_WithScheme()
    {
        var endpoint = GrpcClient.ParseEndpoints("http://127.0.0.1:8081");

        Assert.Equal(AddressScheme.IPv4, endpoint.Scheme);
        Assert.Single(endpoint.Addresses);
        Assert.Equal("127.0.0.1", endpoint.Addresses[0].Host);
        Assert.Equal(8081, endpoint.Addresses[0].Port);
    }

    [Fact]
    [DisplayName("ParseEndpoints_缺省端口用8081")]
    public void ParseEndpoints_DefaultPort()
    {
        var endpoint = GrpcClient.ParseEndpoints("127.0.0.1");

        Assert.Equal(AddressScheme.IPv4, endpoint.Scheme);
        Assert.Equal(8081, endpoint.Addresses[0].Port);
    }

    [Fact]
    [DisplayName("ParseEndpoints_域名与IPv6识别正确")]
    public void ParseEndpoints_DomainAndIPv6()
    {
        Assert.Equal(AddressScheme.DOMAIN_NAME, GrpcClient.ParseEndpoints("http://mq.example.com:8081").Scheme);
        Assert.Equal(AddressScheme.IPv6, GrpcClient.ParseEndpoints("http://[::1]:8081").Scheme);
    }

    [Fact]
    [DisplayName("ParseEndpoints_空地址返回空")]
    public void ParseEndpoints_Empty()
    {
        Assert.Null(GrpcClient.ParseEndpoints(null));
        Assert.Null(GrpcClient.ParseEndpoints(""));
    }

    [Fact]
    [DisplayName("BuildGrpcSettings_消费者上报订阅关系")]
    public void BuildGrpcSettings_Consumer()
    {
        var consumer = new ExposedConsumer
        {
            Topic = "test-topic",
            Group = "test-group",
            Subscription = "tag-a",
            GrpcProxyAddress = "http://127.0.0.1:8081",
        };

        var settings = consumer.GetGrpcSettings();

        Assert.Equal(GrpcClientType.SIMPLE_CONSUMER, settings.ClientType);
        Assert.NotNull(settings.Subscription);
        Assert.Equal("test-group", settings.Subscription.Group.Name);
        Assert.Null(settings.Publishing);

        var entry = Assert.Single(settings.Subscription.Subscriptions);
        Assert.Equal("test-topic", entry.Topic.Name);
        Assert.Equal(GrpcFilterType.TAG, entry.Expression.Type);
        Assert.Equal("tag-a", entry.Expression.Expression);
        Assert.Equal(8081, settings.AccessPoint.Addresses[0].Port);
    }

    [Fact]
    [DisplayName("BuildGrpcSettings_SQL92与多主题")]
    public void BuildGrpcSettings_SQL92AndTopics()
    {
        var consumer = new ExposedConsumer
        {
            Topic = "default-topic",
            Topics = ["topic-a", "topic-b"],
            Group = "group-x",
            Subscription = "a > 1",
            ExpressionType = "SQL92",
            GrpcProxyAddress = "http://127.0.0.1:8081",
        };

        var settings = consumer.GetGrpcSettings();

        Assert.Equal(2, settings.Subscription.Subscriptions.Count);
        Assert.Equal("topic-a", settings.Subscription.Subscriptions[0].Topic.Name);
        Assert.Equal("topic-b", settings.Subscription.Subscriptions[1].Topic.Name);
        Assert.Equal(GrpcFilterType.SQL, settings.Subscription.Subscriptions[0].Expression.Type);
    }

    [Fact]
    [DisplayName("BuildGrpcSettings_生产者上报发布主题")]
    public void BuildGrpcSettings_Producer()
    {
        var producer = new ExposedProducer
        {
            Topic = "pub-topic",
            GrpcProxyAddress = "127.0.0.1:8081",
        };

        var settings = producer.GetGrpcSettings();

        Assert.Equal(GrpcClientType.PRODUCER, settings.ClientType);
        Assert.Null(settings.Subscription);
        Assert.Equal("pub-topic", Assert.Single(settings.Publishing.Topics).Name);
        // 无协议头地址也能解析出访问点
        Assert.Equal(AddressScheme.IPv4, settings.AccessPoint.Scheme);
    }

    [Fact]
    [DisplayName("TelemetrySession_启动停止且可重复停止")]
    public async Task TelemetrySession_StartStop()
    {
        // 指向无服务的端口，会话应自行重试而不是抛出异常
        using var service = new GrpcMessagingService("http://127.0.0.1:9");
        using var session = new GrpcTelemetrySession(service)
        {
            Settings = new GrpcSettings { ClientType = GrpcClientType.PRODUCER },
            ConnectTimeout = TimeSpan.FromMilliseconds(300),
            RefreshInterval = TimeSpan.FromMilliseconds(200),
            ReconnectInterval = TimeSpan.FromMilliseconds(100),
        };

        session.Start();
        await Task.Delay(800);

        // 连接失败时不应标记为已连接
        Assert.False(session.Active);

        session.Stop();
        session.Stop();

        Assert.False(session.Active);
    }

    [Fact]
    [DisplayName("TelemetrySession_未启动直接停止不抛异常")]
    public void TelemetrySession_StopWithoutStart()
    {
        using var service = new GrpcMessagingService("http://127.0.0.1:9");
        using var session = new GrpcTelemetrySession(service);

        session.Stop();

        Assert.False(session.Active);
    }

    [Fact]
    [DisplayName("TelemetrySession_未连接时等待就绪返回false")]
    public async Task TelemetrySession_WaitReady_NotConnected()
    {
        using var service = new GrpcMessagingService("http://127.0.0.1:9");
        using var session = new GrpcTelemetrySession(service) { ConnectTimeout = TimeSpan.FromMilliseconds(200) };

        Assert.False(await session.WaitReadyAsync(TimeSpan.FromMilliseconds(300)));

        // 已停止的会话不应无限等待
        session.Start();
        session.Stop();
        Assert.False(await session.WaitReadyAsync(TimeSpan.FromMilliseconds(300)));
    }

    /// <summary>暴露受保护成员的消费者</summary>
    private sealed class ExposedConsumer : Consumer
    {
        public GrpcSettings GetGrpcSettings() => BuildGrpcSettings();
    }

    /// <summary>暴露受保护成员的生产者</summary>
    private sealed class ExposedProducer : Producer
    {
        public GrpcSettings GetGrpcSettings() => BuildGrpcSettings();
    }
}
