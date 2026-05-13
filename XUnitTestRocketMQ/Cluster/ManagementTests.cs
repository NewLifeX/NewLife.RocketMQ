using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Threading.Tasks;
using NewLife.Log;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Protocol;
using Xunit;

namespace XUnitTestRocketMQ;

/// <summary>管理功能测试</summary>
public class ManagementTests
{
    [Fact(Skip = "需要RocketMQ服务器")]
    [DisplayName("DeleteTopic_删除主题")]
    public void DeleteTopic_Test()
    {
        var set = BasicTest.GetConfig();
        using var mq = new Producer
        {
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
        };
        mq.Start();

        // 先创建再删除
        mq.CreateTopic("nx_delete_test", 2);
        var count = mq.DeleteTopic("nx_delete_test");
        Assert.True(count >= 0);
    }

    [Fact(Skip = "需要RocketMQ服务器")]
    [DisplayName("CreateSubscriptionGroup_创建消费组")]
    public void CreateSubscriptionGroup_Test()
    {
        var set = BasicTest.GetConfig();
        using var mq = new Producer
        {
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
        };
        mq.Start();

        var count = mq.CreateSubscriptionGroup("nx_test_group", true, 16, 1);
        Assert.True(count >= 0);
    }

    [Fact(Skip = "需要RocketMQ服务器")]
    [DisplayName("DeleteSubscriptionGroup_删除消费组")]
    public void DeleteSubscriptionGroup_Test()
    {
        var set = BasicTest.GetConfig();
        using var mq = new Producer
        {
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
        };
        mq.Start();

        mq.CreateSubscriptionGroup("nx_delete_group");
        var count = mq.DeleteSubscriptionGroup("nx_delete_group");
        Assert.True(count >= 0);
    }

    [Fact(Skip = "需要RocketMQ服务器")]
    [DisplayName("ViewMessage_按ID查看消息")]
    public void ViewMessage_Test()
    {
        var set = BasicTest.GetConfig();
        using var mq = new Producer
        {
            Topic = "nx_test",
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
        };
        mq.Start();

        // 先发一条消息
        var sr = mq.Publish("ViewMessage_Test", "TagA", null);
        Assert.NotNull(sr?.MsgId);

        // 尝试查看（可能因Broker索引延迟返回null）
        var msg = mq.ViewMessage(sr.MsgId);
        // 不做严格断言，仅验证不抛出异常
    }

    [Fact(Skip = "需要RocketMQ服务器")]
    [DisplayName("GetClusterInfo_获取集群信息")]
    public void GetClusterInfo_Test()
    {
        var set = BasicTest.GetConfig();
        using var mq = new Producer
        {
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
        };
        mq.Start();

        var info = mq.GetClusterInfo();
        // 不做严格断言，仅验证不抛出异常
    }

    [Fact(Skip = "需要RocketMQ服务器")]
    [DisplayName("GetConsumerConnectionList_获取消费者连接列表")]
    public async Task GetConsumerConnectionList_Test()
    {
        var set = BasicTest.GetConfig();
        using var consumer = new Consumer
        {
            Topic = "nx_test",
            Group = "test",
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
        };
        consumer.Start();

        var list = await consumer.GetConsumerConnectionList("test");
        // 不做严格断言，仅验证不抛出异常
    }

    [Fact(Skip = "需要RocketMQ服务器")]
    [DisplayName("ResetConsumerOffset_重置消费偏移")]
    public async Task ResetConsumerOffset_Test()
    {
        var set = BasicTest.GetConfig();
        using var consumer = new Consumer
        {
            Topic = "nx_test",
            Group = "test",
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
        };
        consumer.Start();

        // 重置到一小时前
        var timestamp = DateTimeOffset.UtcNow.AddHours(-1).ToUnixTimeMilliseconds();
        var result = await consumer.ResetConsumerOffset(timestamp);
        // 不做严格断言，仅验证不抛出异常
    }
}
