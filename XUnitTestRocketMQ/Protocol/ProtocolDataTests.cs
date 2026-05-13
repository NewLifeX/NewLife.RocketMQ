using System;
using System.ComponentModel;
using NewLife.RocketMQ.Protocol;
using Xunit;

namespace XUnitTestRocketMQ;

/// <summary>协议数据结构测试</summary>
public class ProtocolDataTests
{
    #region HeartbeatData
    [Fact]
    [DisplayName("HeartbeatData_默认属性为null")]
    public void HeartbeatData_Defaults()
    {
        var hb = new HeartbeatData();

        Assert.Null(hb.ClientID);
        Assert.Null(hb.ConsumerDataSet);
        Assert.Null(hb.ProducerDataSet);
    }

    [Fact]
    [DisplayName("HeartbeatData_设置所有属性")]
    public void HeartbeatData_SetAllProperties()
    {
        var hb = new HeartbeatData
        {
            ClientID = "client-001",
            ConsumerDataSet = [new ConsumerData { GroupName = "CG1" }],
            ProducerDataSet = [new ProducerData { GroupName = "PG1" }],
        };

        Assert.Equal("client-001", hb.ClientID);
        Assert.Single(hb.ConsumerDataSet);
        Assert.Equal("CG1", hb.ConsumerDataSet[0].GroupName);
        Assert.Single(hb.ProducerDataSet);
        Assert.Equal("PG1", hb.ProducerDataSet[0].GroupName);
    }
    #endregion

    #region ProducerData
    [Fact]
    [DisplayName("ProducerData_默认GroupName")]
    public void ProducerData_DefaultGroupName()
    {
        var pd = new ProducerData();

        Assert.Equal("CLIENT_INNER_PRODUCER", pd.GroupName);
    }

    [Fact]
    [DisplayName("ProducerData_可自定义GroupName")]
    public void ProducerData_CustomGroupName()
    {
        var pd = new ProducerData { GroupName = "MY_PRODUCER" };

        Assert.Equal("MY_PRODUCER", pd.GroupName);
    }
    #endregion

    #region ConsumerData
    [Fact]
    [DisplayName("ConsumerData_默认值")]
    public void ConsumerData_Defaults()
    {
        var cd = new ConsumerData();

        Assert.Equal("CONSUME_FROM_LAST_OFFSET", cd.ConsumeFromWhere);
        Assert.Equal("CONSUME_ACTIVELY", cd.ConsumeType);
        Assert.Null(cd.GroupName);
        Assert.Equal("CLUSTERING", cd.MessageModel);
        Assert.Null(cd.SubscriptionDataSet);
        Assert.False(cd.UnitMode);
    }

    [Fact]
    [DisplayName("ConsumerData_设置所有属性")]
    public void ConsumerData_SetAllProperties()
    {
        var cd = new ConsumerData
        {
            ConsumeFromWhere = "CONSUME_FROM_FIRST_OFFSET",
            ConsumeType = "CONSUME_PASSIVELY",
            GroupName = "CG_TEST",
            MessageModel = "BROADCASTING",
            UnitMode = true,
            SubscriptionDataSet = [new SubscriptionData { Topic = "test" }],
        };

        Assert.Equal("CONSUME_FROM_FIRST_OFFSET", cd.ConsumeFromWhere);
        Assert.Equal("CONSUME_PASSIVELY", cd.ConsumeType);
        Assert.Equal("CG_TEST", cd.GroupName);
        Assert.Equal("BROADCASTING", cd.MessageModel);
        Assert.True(cd.UnitMode);
        Assert.Single(cd.SubscriptionDataSet);
    }
    #endregion

    #region SubscriptionData
    [Fact]
    [DisplayName("SubscriptionData_默认值")]
    public void SubscriptionData_Defaults()
    {
        var sd = new SubscriptionData();

        Assert.Null(sd.Topic);
        Assert.Equal("TAG", sd.ExpressionType);
        Assert.Equal("*", sd.SubString);
        Assert.Null(sd.TagsSet);
        Assert.Null(sd.CodeSet);
        Assert.False(sd.ClassFilterMode);
        Assert.Null(sd.FilterClassSource);
        Assert.True(sd.SubVersion > 0);
    }

    [Fact]
    [DisplayName("SubscriptionData_设置SQL92过滤")]
    public void SubscriptionData_SQL92Filter()
    {
        var sd = new SubscriptionData
        {
            Topic = "order_topic",
            ExpressionType = "SQL92",
            SubString = "price > 100",
        };

        Assert.Equal("order_topic", sd.Topic);
        Assert.Equal("SQL92", sd.ExpressionType);
        Assert.Equal("price > 100", sd.SubString);
    }

    [Fact]
    [DisplayName("SubscriptionData_设置标签集合")]
    public void SubscriptionData_TagsSet()
    {
        var sd = new SubscriptionData
        {
            Topic = "test",
            TagsSet = ["TagA", "TagB", "TagC"],
            CodeSet = ["1", "2"],
        };

        Assert.Equal(3, sd.TagsSet.Length);
        Assert.Equal("TagA", sd.TagsSet[0]);
        Assert.Equal(2, sd.CodeSet.Length);
    }
    #endregion

    #region QueryResult
    [Fact]
    [DisplayName("QueryResult_默认值")]
    public void QueryResult_Defaults()
    {
        var qr = new QueryResult();

        Assert.Equal(0, qr.IndexLastUpdateTimestamp);
        Assert.Null(qr.MessageList);
    }

    [Fact]
    [DisplayName("QueryResult_设置属性")]
    public void QueryResult_SetProperties()
    {
        var qr = new QueryResult
        {
            IndexLastUpdateTimestamp = 12345,
            MessageList = [new MessageExt { Topic = "t1" }],
        };

        Assert.Equal(12345, qr.IndexLastUpdateTimestamp);
        Assert.Single(qr.MessageList);
        Assert.Equal("t1", qr.MessageList[0].Topic);
    }
    #endregion
}
