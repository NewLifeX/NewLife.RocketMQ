using System;
using System.ComponentModel;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Protocol;
using Xunit;

namespace XUnitTestRocketMQ;

/// <summary>消息压缩功能测试</summary>
public class CompressionTests
{
    [Fact]
    [DisplayName("压缩阈值_默认值为4096")]
    public void CompressOverBytes_DefaultValue()
    {
        using var producer = new Producer();
        Assert.Equal(4096, producer.CompressOverBytes);
    }

    [Fact]
    [DisplayName("压缩阈值_可自定义设置")]
    public void CompressOverBytes_CanBeSet()
    {
        using var producer = new Producer
        {
            CompressOverBytes = 1024,
        };
        Assert.Equal(1024, producer.CompressOverBytes);
    }

    [Fact]
    [DisplayName("压缩阈值_设为0禁用压缩")]
    public void CompressOverBytes_ZeroDisablesCompression()
    {
        using var producer = new Producer
        {
            CompressOverBytes = 0,
        };
        Assert.Equal(0, producer.CompressOverBytes);
    }
}
