using System;
using System.ComponentModel;
using System.Linq;
using NewLife.RocketMQ.Protocol;
using Xunit;

namespace XUnitTestRocketMQ;

/// <summary>请求头GetProperties方法测试</summary>
public class RequestHeaderTests
{
    #region SendMessageRequestHeader
    [Fact]
    [DisplayName("SendMessageRequestHeader_GetProperties返回所有属性")]
    public void SendMessageRequestHeader_GetProperties_ReturnsAll()
    {
        var header = new SendMessageRequestHeader
        {
            ProducerGroup = "PG_TEST",
            Topic = "test_topic",
            DefaultTopic = "TBW102",
            DefaultTopicQueueNums = 4,
            QueueId = 1,
            SysFlag = 0,
            BornTimestamp = 1000L,
            Flag = 0,
            Properties = "TAGS\u0001test",
            ReconsumeTimes = 0,
            UnitMode = false,
            ConsumeRetryTimes = 0,
            Batch = false,
            BrokerName = "broker-0",
        };

        var dic = header.GetProperties();

        Assert.NotNull(dic);
        Assert.True(dic.Count > 0);

        // XmlElement(ElementName="a") 映射
        Assert.True(dic.ContainsKey("a"), "ProducerGroup应映射为a");
        Assert.Equal("PG_TEST", dic["a"]);

        Assert.True(dic.ContainsKey("b"), "Topic应映射为b");
        Assert.Equal("test_topic", dic["b"]);

        Assert.True(dic.ContainsKey("c"), "DefaultTopic应映射为c");
        Assert.Equal("TBW102", dic["c"]);

        Assert.True(dic.ContainsKey("e"), "QueueId应映射为e");
        Assert.True(dic.ContainsKey("i"), "Properties应映射为i");
    }

    [Fact]
    [DisplayName("SendMessageRequestHeader_默认值GetProperties")]
    public void SendMessageRequestHeader_DefaultValues_GetProperties()
    {
        var header = new SendMessageRequestHeader();

        var dic = header.GetProperties();

        Assert.NotNull(dic);
        // 即使默认值，属性也应该被包含
        Assert.True(dic.Count >= 10);
    }
    #endregion

    #region PullMessageRequestHeader
    [Fact]
    [DisplayName("PullMessageRequestHeader_GetProperties返回camelCase键")]
    public void PullMessageRequestHeader_GetProperties_CamelCaseKeys()
    {
        var header = new PullMessageRequestHeader
        {
            ConsumerGroup = "CG_TEST",
            Topic = "test_topic",
            Subscription = "*",
            QueueId = 2,
            QueueOffset = 100,
            MaxMsgNums = 32,
            SuspendTimeoutMillis = 20000,
        };

        var dic = header.GetProperties();

        Assert.NotNull(dic);
        Assert.True(dic.ContainsKey("consumerGroup"), "应转为camelCase");
        Assert.True(dic.ContainsKey("topic"));
        Assert.True(dic.ContainsKey("subscription"));
        Assert.True(dic.ContainsKey("queueId"));
        Assert.True(dic.ContainsKey("queueOffset"));
        Assert.True(dic.ContainsKey("maxMsgNums"));
        Assert.True(dic.ContainsKey("suspendTimeoutMillis"));
    }

    [Fact]
    [DisplayName("PullMessageRequestHeader_默认ExpressionType为TAG")]
    public void PullMessageRequestHeader_DefaultExpressionType()
    {
        var header = new PullMessageRequestHeader();

        Assert.Equal("TAG", header.ExpressionType);
        Assert.Equal("*", header.Subscription);
        Assert.Equal(20000, header.SuspendTimeoutMillis);
    }

    [Fact]
    [DisplayName("PullMessageRequestHeader_GetProperties值为字符串")]
    public void PullMessageRequestHeader_GetProperties_ValuesAreStrings()
    {
        var header = new PullMessageRequestHeader
        {
            QueueId = 5,
            MaxMsgNums = 32,
        };

        var dic = header.GetProperties();

        // PullMessageRequestHeader 的 GetProperties 会 + "" 转为字符串
        Assert.IsType<String>(dic["queueId"]);
        Assert.Equal("5", dic["queueId"]);
    }
    #endregion

    #region EndTransactionRequestHeader
    [Fact]
    [DisplayName("EndTransactionRequestHeader_GetProperties返回camelCase键")]
    public void EndTransactionRequestHeader_GetProperties_CamelCaseKeys()
    {
        var header = new EndTransactionRequestHeader
        {
            ProducerGroup = "PG_TX",
            TranStateTableOffset = 100,
            CommitLogOffset = 200,
            CommitOrRollback = 1,
            FromTransactionCheck = true,
            MsgId = "MSG001",
            TransactionId = "TX001",
        };

        var dic = header.GetProperties();

        Assert.NotNull(dic);
        Assert.True(dic.ContainsKey("producerGroup"));
        Assert.True(dic.ContainsKey("tranStateTableOffset"));
        Assert.True(dic.ContainsKey("commitLogOffset"));
        Assert.True(dic.ContainsKey("commitOrRollback"));
        Assert.True(dic.ContainsKey("fromTransactionCheck"));
        Assert.True(dic.ContainsKey("msgId"));
        Assert.True(dic.ContainsKey("transactionId"));

        Assert.Equal("PG_TX", dic["producerGroup"]);
        Assert.Equal("TX001", dic["transactionId"]);
    }

    [Fact]
    [DisplayName("EndTransactionRequestHeader_布尔值序列化")]
    public void EndTransactionRequestHeader_BooleanSerialization()
    {
        var header = new EndTransactionRequestHeader
        {
            FromTransactionCheck = true,
        };

        var dic = header.GetProperties();

        // 值是原始 Boolean 对象（非字符串），且为 true
        Assert.True(dic.ContainsKey("fromTransactionCheck"));
        var fromTransactionCheck = Assert.IsType<Boolean>(dic["fromTransactionCheck"]);
        Assert.True(fromTransactionCheck);
    }
    #endregion
}
