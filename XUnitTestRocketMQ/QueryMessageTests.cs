using System;
using System.ComponentModel;
using NewLife.RocketMQ;
using Xunit;

namespace XUnitTestRocketMQ;

/// <summary>按Key查询消息测试</summary>
public class QueryMessageTests
{
    [Fact]
    [DisplayName("QueryMessageByKey_Null的Key抛出异常")]
    public void QueryMessageByKey_NullKey_ThrowsException()
    {
        using var producer = new Producer();
        Assert.Throws<ArgumentNullException>(() => producer.QueryMessageByKey("test", null));
    }

    [Fact]
    [DisplayName("QueryMessageByKey_空Key抛出异常")]
    public void QueryMessageByKey_EmptyKey_ThrowsException()
    {
        using var producer = new Producer();
        Assert.Throws<ArgumentNullException>(() => producer.QueryMessageByKey("test", ""));
    }

    [Fact(Skip = "需要RocketMQ服务器")]
    [DisplayName("QueryMessageByKey_按Key查询消息")]
    public void QueryMessageByKey_Integration()
    {
        var set = BasicTest.GetConfig();
        using var producer = new Producer
        {
            Topic = "nx_test",
            NameServerAddress = set.NameServer,
        };
        producer.Start();

        var msgs = producer.QueryMessageByKey("nx_test", "test_key_123");
        // 不做严格断言，仅验证不抛出异常
    }
}
