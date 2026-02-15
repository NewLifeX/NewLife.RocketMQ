using System;
using System.ComponentModel;
using System.Threading.Tasks;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Protocol;
using Xunit;

namespace XUnitTestRocketMQ;

/// <summary>消费重试功能测试</summary>
public class RetryTests
{
    [Fact]
    [DisplayName("MaxReconsumeTimes_默认值为16")]
    public void MaxReconsumeTimes_DefaultValue()
    {
        using var consumer = new Consumer();
        Assert.Equal(16, consumer.MaxReconsumeTimes);
    }

    [Fact]
    [DisplayName("EnableRetry_默认启用")]
    public void EnableRetry_DefaultTrue()
    {
        using var consumer = new Consumer();
        Assert.True(consumer.EnableRetry);
    }

    [Fact]
    [DisplayName("RetryDelayLevel_默认为0")]
    public void RetryDelayLevel_DefaultZero()
    {
        using var consumer = new Consumer();
        Assert.Equal(0, consumer.RetryDelayLevel);
    }

    [Fact]
    [DisplayName("MaxReconsumeTimes_可自定义")]
    public void MaxReconsumeTimes_CanBeCustomized()
    {
        using var consumer = new Consumer
        {
            MaxReconsumeTimes = 5,
        };
        Assert.Equal(5, consumer.MaxReconsumeTimes);
    }

    [Fact]
    [DisplayName("EnableRetry_可禁用")]
    public void EnableRetry_CanBeDisabled()
    {
        using var consumer = new Consumer
        {
            EnableRetry = false,
        };
        Assert.False(consumer.EnableRetry);
    }

    [Fact]
    [DisplayName("SendMessageBack_Null消息抛出异常")]
    public async Task SendMessageBack_NullMessage_ThrowsException()
    {
        using var consumer = new Consumer();
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            consumer.SendMessageBackAsync(null));
    }
}
