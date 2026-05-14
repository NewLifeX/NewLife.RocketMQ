using System;
using System.ComponentModel;
using System.Threading;
using NewLife.Log;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Protocol;
using Xunit;
using XUnitTest.Integration;

namespace XUnitTest.Cluster;

/// <summary>VIP Channel 集成测试</summary>
/// <remarks>
/// VIP Channel 是 RocketMQ 的一种特殊通道：Broker 在标准端口 (10911) 基础上减 2，
/// 即 VIP 端口 = 10909。开启后消息走 VIP 端口通信。
/// Apache RocketMQ 4.x 默认开启 VIP 通道，5.x 已移除该机制。
/// </remarks>
public class VipChannelIntegrationTests
{
    [Fact]
    [DisplayName("VipChannel_开启VIP通道_发送消息成功")]
    public void VipChannel_Enabled_SendOK()
    {
        var set = BasicTest.GetConfig();
        const String topic = "nx_vip_channel_test";

        using var producer = new Producer
        {
            Topic = topic,
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
            VipChannelEnabled = true,
        };
        producer.Start();
        Thread.Sleep(2000);

        var sr = producer.Publish($"vip-enabled-{DateTime.UtcNow.Ticks}");
        XTrace.WriteLine("VipChannel=true 发送结果: {0}", sr.Status);

        // VIP 端口可能不可用（尤其是 5.x），只要不抛异常或发送成功即可
        Assert.True(sr.Status == SendStatus.SendOK || sr.Status == SendStatus.FlushDiskTimeout,
            $"发送失败: {sr.Status}");
    }

    [Fact]
    [DisplayName("VipChannel_关闭VIP通道_发送消息成功")]
    public void VipChannel_Disabled_SendOK()
    {
        var set = BasicTest.GetConfig();
        const String topic = "nx_vip_channel_test";

        using var producer = new Producer
        {
            Topic = topic,
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
            VipChannelEnabled = false,
        };
        producer.Start();
        Thread.Sleep(2000);

        var sr = producer.Publish($"vip-disabled-{DateTime.UtcNow.Ticks}");
        XTrace.WriteLine("VipChannel=false 发送结果: {0}", sr.Status);

        Assert.Equal(SendStatus.SendOK, sr.Status);
    }
}
