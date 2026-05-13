using System;
using System.Net.Sockets;
using System.Threading.Tasks;
using Xunit;

#nullable enable

namespace XUnitTestRocketMQ;

/// <summary>RocketMQ 集成测试 Fixture</summary>
/// <remarks>
/// 使用方式：设置环境变量 ROCKETMQ_NAMESERVER=127.0.0.1:9876，再运行测试。
/// 可通过 scripts/RocketMqSetup.cs 在本机一键安装启动 RocketMQ：
///   dotnet run --file scripts/RocketMqSetup.cs
/// 未设置环境变量时，所有集成测试自动跳过（Skip）。
/// </remarks>
public sealed class RocketMqFixture : IAsyncLifetime
{
    /// <summary>外部 NameServer 地址（环境变量 ROCKETMQ_NAMESERVER），如 127.0.0.1:9876</summary>
    public static String? ExternalNameServer =>
        Environment.GetEnvironmentVariable("ROCKETMQ_NAMESERVER");

    /// <summary>是否处于可运行模式</summary>
    public static Boolean IsEnabled => !String.IsNullOrEmpty(ExternalNameServer);

    /// <summary>NameServer 连接地址</summary>
    public String NameServerAddress { get; private set; } = String.Empty;

    private Exception? _initException;

    /// <summary>若未设置 ROCKETMQ_NAMESERVER 或连接失败则跳过当前测试</summary>
    public void SkipIfUnavailable()
    {
        if (!IsEnabled)
            Skip.If(true,
                "集成测试未启用。\n" +
                "设置环境变量 ROCKETMQ_NAMESERVER=127.0.0.1:9876 后重试。\n" +
                "启动本机 RocketMQ：dotnet run --file scripts/RocketMqSetup.cs");

        if (_initException != null)
            Skip.If(true, $"无法连接 RocketMQ：{_initException.Message}");
    }

    /// <inheritdoc/>
    public Task InitializeAsync()
    {
        if (!IsEnabled) return Task.CompletedTask;

        NameServerAddress = ExternalNameServer!;
        _initException    = VerifyConnection(NameServerAddress);
        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public Task DisposeAsync() => Task.CompletedTask;

    // ── 辅助 ──────────────────────────────────────────────────────────────────

    /// <summary>TCP 连通性检测</summary>
    /// <param name="address">格式 host:port</param>
    /// <returns>连接失败时返回异常，否则返回 null</returns>
    private static Exception? VerifyConnection(String address)
    {
        try
        {
            var parts = address.Split(':');
            var host  = parts[0];
            var port  = parts.Length > 1 ? Int32.Parse(parts[1]) : 9876;
            using var tcp = new TcpClient();
            tcp.Connect(host, port);
            return null;
        }
        catch (Exception ex)
        {
            return new InvalidOperationException(
                $"无法连接到 NameServer [{address}]，原因：{ex.Message}", ex);
        }
    }
}
