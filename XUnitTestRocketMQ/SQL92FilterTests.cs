using System;
using System.ComponentModel;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Protocol;
using Xunit;

namespace XUnitTestRocketMQ;

/// <summary>SQL92过滤功能测试</summary>
public class SQL92FilterTests
{
    [Fact]
    [DisplayName("ExpressionType_默认值为TAG")]
    public void ExpressionType_DefaultIsTAG()
    {
        using var consumer = new Consumer();
        Assert.Equal("TAG", consumer.ExpressionType);
    }

    [Fact]
    [DisplayName("ExpressionType_可设置为SQL92")]
    public void ExpressionType_CanSetToSQL92()
    {
        using var consumer = new Consumer
        {
            ExpressionType = "SQL92",
            Subscription = "age > 18 AND region = 'hangzhou'",
        };

        Assert.Equal("SQL92", consumer.ExpressionType);
        Assert.Contains("age > 18", consumer.Subscription);
    }

    [Fact]
    [DisplayName("PullMessageRequestHeader_ExpressionType默认TAG")]
    public void PullHeader_ExpressionType_Default()
    {
        var header = new PullMessageRequestHeader();
        Assert.Equal("TAG", header.ExpressionType);
    }

    [Fact]
    [DisplayName("PullMessageRequestHeader_ExpressionType可设为SQL92")]
    public void PullHeader_ExpressionType_SQL92()
    {
        var header = new PullMessageRequestHeader
        {
            ExpressionType = "SQL92",
            Subscription = "price BETWEEN 10 AND 100",
        };

        Assert.Equal("SQL92", header.ExpressionType);
        Assert.Equal("price BETWEEN 10 AND 100", header.Subscription);
    }
}
