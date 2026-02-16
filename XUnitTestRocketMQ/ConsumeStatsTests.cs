using System;
using System.ComponentModel;
using NewLife.Log;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Protocol;
using Xunit;

namespace XUnitTestRocketMQ;

/// <summary>消费统计和过滤服务器测试</summary>
public class ConsumeStatsTests
{
    [Fact(Skip = "需要RocketMQ服务器")]
    [DisplayName("GetConsumeStats_获取消费统计")]
    public void GetConsumeStats_Test()
    {
        var set = BasicTest.GetConfig();
        using var mq = new Producer
        {
            Topic = "nx_test",
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
        };
        mq.Start();

        var stats = mq.GetConsumeStats("CID_nx_test");
        // 不做严格断言，仅验证不抛出异常
    }

    [Fact(Skip = "需要RocketMQ服务器")]
    [DisplayName("GetTopicStatsInfo_获取Topic统计")]
    public void GetTopicStatsInfo_Test()
    {
        var set = BasicTest.GetConfig();
        using var mq = new Producer
        {
            Topic = "nx_test",
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
        };
        mq.Start();

        var stats = mq.GetTopicStatsInfo("nx_test");
        // 不做严格断言，仅验证不抛出异常
    }

    [Fact(Skip = "需要RocketMQ服务器")]
    [DisplayName("RegisterFilterServer_注册过滤服务器")]
    public void RegisterFilterServer_Test()
    {
        var set = BasicTest.GetConfig();
        using var mq = new Producer
        {
            Topic = "nx_test",
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
        };
        mq.Start();

        var count = mq.RegisterFilterServer("127.0.0.1:9999");
        Assert.True(count >= 0);
    }

    [Fact]
    [DisplayName("RegisterFilterServer_空地址抛出异常")]
    public void RegisterFilterServer_EmptyAddress_ThrowsException()
    {
        using var mq = new Producer();
        Assert.Throws<ArgumentNullException>(() => mq.RegisterFilterServer(null));
        Assert.Throws<ArgumentNullException>(() => mq.RegisterFilterServer(""));
    }

    [Fact]
    [DisplayName("RequestCode包含过滤服务器和统计相关码")]
    public void RequestCode_ContainsFilterAndStatsCodes()
    {
        Assert.Equal(301, (Int32)RequestCode.REGISTER_FILTER_SERVER);
        Assert.Equal(208, (Int32)RequestCode.GET_CONSUME_STATS);
        Assert.Equal(202, (Int32)RequestCode.GET_TOPIC_STATS_INFO);
    }
}
