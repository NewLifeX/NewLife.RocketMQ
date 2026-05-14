using System;
using System.Net.Sockets;
using NewLife.Log;
using NewLife.RocketMQ;
using Xunit;

// 所有测试用例放入一个汇编级集合，除非单独指定Collection特性
[assembly: CollectionBehavior(CollectionBehavior.CollectionPerAssembly)]

namespace XUnitTest.Integration;

[Collection("Basic")]
public class BasicTest
{
    private static MqSetting _config;

    /// <summary>获取 RocketMQ 配置</summary>
    public static MqSetting GetConfig()
    {
        if (_config != null) return _config;
        lock (typeof(BasicTest))
        {
            if (_config != null) return _config;

            var set = MqSetting.Current;
            if (set.IsNew)
            {
                //set.NameServer = "rocketmq.newlifex.com:9876";
                set.NameServer = "localhost:9876";
                set.Save();
            }

            XTrace.WriteLine("RocketMQ配置：{0}", set.NameServer);

            return _config = set;
        }
    }

    /// <summary>若 NameServer 不可达则跳过当前测试。适合不使用 RocketMqFixture 的集成测试方法。</summary>
    public static void SkipIfUnavailable()
    {
        var addr = Environment.GetEnvironmentVariable("ROCKETMQ_NAMESERVER");
        if (String.IsNullOrEmpty(addr)) addr = GetConfig().NameServer;

        Skip.If(!IsReachable(addr),
            $"无法连接 RocketMQ NameServer [{addr}]，跳过集成测试。\n" +
            "请检查 Config/RocketMQ.xml 配置或通过 ROCKETMQ_NAMESERVER 环境变量指定地址。\n" +
            "启动本机 RocketMQ：dotnet run --file scripts/RocketMqSetup.cs");
    }

    /// <summary>TCP 连通性检测</summary>
    /// <param name="address">格式 host:port</param>
    /// <returns>可达返回 true</returns>
    public static Boolean IsReachable(String? address)
    {
        if (String.IsNullOrEmpty(address)) return false;
        try
        {
            var parts = address.Split(':');
            var host  = parts[0];
            var port  = parts.Length > 1 ? Int32.Parse(parts[1]) : 9876;
            using var tcp = new TcpClient();
            tcp.Connect(host, port);
            return true;
        }
        catch
        {
            return false;
        }
    }
}
