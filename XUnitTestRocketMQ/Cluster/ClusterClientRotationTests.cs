using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using NewLife;
using NewLife.Net;
using NewLife.RocketMQ;
using Xunit;

namespace XUnitTest.Cluster;

/// <summary>ClusterClient 多地址轮询与切换测试。验证 raft 多节点下重连/切换不会始终钉在第一个地址（issue #118）</summary>
[System.ComponentModel.DisplayName("ClusterClient 多地址轮询测试")]
public class ClusterClientRotationTests : IDisposable
{
    /// <summary>仅统计接入连接的伪服务器，验证客户端连到了哪个地址</summary>
    private sealed class FakeServer : IDisposable
    {
        private readonly TcpListener _listener;
        private readonly CancellationTokenSource _cts = new();

        public Int32 Port { get; }
        public Int32 Count { get; private set; }

        public FakeServer()
        {
            _listener = new TcpListener(IPAddress.Loopback, 0);
            _listener.Start();
            Port = ((IPEndPoint)_listener.LocalEndpoint).Port;

            _ = AcceptLoopAsync();
        }

        private async Task AcceptLoopAsync()
        {
            while (!_cts.IsCancellationRequested)
            {
                try
                {
                    using var tcp = await _listener.AcceptTcpClientAsync(_cts.Token).ConfigureAwait(false);
                    Count++;
                }
                catch { break; }
            }
        }

        public void Dispose()
        {
            _cts.Cancel();
            _listener.Stop();
        }
    }

    /// <summary>测试子类，通过受保护成员 EnsureCreate 触发建连，避免启动心跳定时器</summary>
    private sealed class TestBrokerClient : BrokerClient
    {
        public TestBrokerClient(String[] servers) : base(servers) { }

        public void ConnectNow() => EnsureCreate();
    }

    private readonly List<FakeServer> _servers = [];
    private readonly List<BrokerClient> _clients = [];

    private FakeServer Listen()
    {
        var server = new FakeServer();
        _servers.Add(server);

        return server;
    }

    private TestBrokerClient CreateClient(params FakeServer[] servers)
    {
        var client = new TestBrokerClient(servers.Select(e => $"127.0.0.1:{e.Port}").ToArray())
        {
            Config = new Producer(),
            Timeout = 1_000,
            Servers = servers.Select(e => new NetUri($"127.0.0.1:{e.Port}")).ToArray(),
        };
        _clients.Add(client);

        return client;
    }

    public void Dispose()
    {
        foreach (var client in _clients) client.TryDispose();
        foreach (var server in _servers) server.Dispose();
        _servers.Clear();
        _clients.Clear();
    }

    /// <summary>等待伪服务器接入数达到阈值（accept 在后台异步完成）</summary>
    private static void WaitCount(FakeServer server, Int32 min)
    {
        var end = DateTime.UtcNow.AddSeconds(3);
        while (server.Count < min && DateTime.UtcNow < end) Thread.Sleep(10);
    }

    [Fact]
    [System.ComponentModel.DisplayName("连接_第一个地址可用_使用第一个")]
    public void Connect_FirstUp_UsesFirst()
    {
        var s1 = Listen();
        var s2 = Listen();
        var client = CreateClient(s1, s2);

        client.ConnectNow();

        WaitCount(s1, 1);
        Assert.True(s1.Count >= 1);
        Assert.Equal(0, s2.Count);
    }

    [Fact]
    [System.ComponentModel.DisplayName("连接_第一个不可用_自动使用第二个")]
    public void Connect_FirstDown_UsesSecond()
    {
        var down = Listen();
        down.Dispose();
        _servers.Remove(down);

        var up = Listen();
        var client = CreateClient(down, up);

        client.ConnectNow();

        WaitCount(up, 1);
        Assert.True(up.Count >= 1);
    }

    [Fact]
    [System.ComponentModel.DisplayName("切换_在两个地址间轮换")]
    public void SwitchServer_MovesToNext()
    {
        var s1 = Listen();
        var s2 = Listen();
        var client = CreateClient(s1, s2);

        client.ConnectNow();
        WaitCount(s1, 1);
        Assert.True(s1.Count >= 1);
        Assert.Equal(0, s2.Count);

        Assert.True(client.SwitchServer());
        WaitCount(s2, 1);
        Assert.True(s2.Count >= 1);

        Assert.True(client.SwitchServer());
        WaitCount(s1, 2);
        Assert.True(s1.Count >= 2);
    }

    [Fact]
    [System.ComponentModel.DisplayName("切换_单地址时保持同一地址")]
    public void SwitchServer_SingleServer_KeepsSame()
    {
        var s1 = Listen();
        var client = CreateClient(s1);

        client.ConnectNow();
        WaitCount(s1, 1);
        Assert.True(s1.Count >= 1);

        Assert.True(client.SwitchServer());
        WaitCount(s1, 2);
        Assert.True(s1.Count >= 2);
    }
}
