using System;
using System.Net.Sockets;
using System.Threading.Tasks;
using Xunit;

#nullable enable

namespace XUnitTest.Integration;

/// <summary>RocketMQ 集成测试 Fixture</summary>
/// <remarks>
/// 使用方式：在 Config/RocketMQ.xml 配置 NameServer 地址即可直接运行集成测试，无需设置环境变量。
/// 可通过 scripts/RocketMqSetup.cs 在本机一键安装启动 RocketMQ：
///   dotnet run --file scripts/RocketMqSetup.cs
/// 也支持环境变量覆盖：ROCKETMQ_NAMESERVER=127.0.0.1:9876（优先级高于配置文件）。
/// NameServer 不可达时，所有集成测试自动跳过（Skip）。
/// </remarks>
public sealed class RocketMqFixture : IAsyncLifetime
{
    /// <summary>环境变量覆盖地址（ROCKETMQ_NAMESERVER），如 127.0.0.1:9876；优先级高于配置文件</summary>
    public static String? ExternalNameServer =>
        Environment.GetEnvironmentVariable("ROCKETMQ_NAMESERVER");

    /// <summary>NameServer 连接地址</summary>
    public String NameServerAddress { get; private set; } = String.Empty;

    private Exception? _initException;

    /// <summary>若 NameServer 不可达则跳过当前测试</summary>
    public void SkipIfUnavailable()
    {
        if (_initException != null)
            Skip.If(true,
                $"无法连接 RocketMQ：{_initException.Message}\n" +
                "请检查 Config/RocketMQ.xml 中的 NameServer 配置，并确认 RocketMQ 服务已启动。\n" +
                "启动本机 RocketMQ：dotnet run --file scripts/RocketMqSetup.cs");
    }

    /// <inheritdoc/>
    public Task InitializeAsync()
    {
        // 环境变量优先覆盖，其次从配置文件（MqSetting）读取
        var addr = ExternalNameServer;
        if (String.IsNullOrEmpty(addr)) addr = BasicTest.GetConfig().NameServer;

        NameServerAddress = addr ?? String.Empty;
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
        if (String.IsNullOrEmpty(address))
            return new InvalidOperationException("未配置 NameServer 地址");
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
