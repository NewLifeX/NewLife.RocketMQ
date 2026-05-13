using System;
using System.ComponentModel;
using NewLife.RocketMQ;
using Xunit;

namespace XUnitTestRocketMQ;

/// <summary>消费限流测试</summary>
public class ConcurrentConsumeTests
{
    [Fact]
    [DisplayName("MaxConcurrentConsume_默认为0")]
    public void MaxConcurrentConsume_DefaultZero()
    {
        using var consumer = new Consumer();
        Assert.Equal(0, consumer.MaxConcurrentConsume);
    }

    [Fact]
    [DisplayName("MaxConcurrentConsume_可设置正整数")]
    public void MaxConcurrentConsume_CanSetPositive()
    {
        using var consumer = new Consumer { MaxConcurrentConsume = 10 };
        Assert.Equal(10, consumer.MaxConcurrentConsume);
    }

    [Fact]
    [DisplayName("MaxConcurrentConsume_设为1时串行消费")]
    public void MaxConcurrentConsume_Serial()
    {
        using var consumer = new Consumer { MaxConcurrentConsume = 1 };
        Assert.Equal(1, consumer.MaxConcurrentConsume);
    }
}
