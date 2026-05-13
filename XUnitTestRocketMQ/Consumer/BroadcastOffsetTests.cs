using System;
using System.ComponentModel;
using System.IO;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Models;
using Xunit;

namespace XUnitTestRocketMQ;

/// <summary>广播模式本地偏移持久化测试</summary>
public class BroadcastOffsetTests
{
    [Fact]
    [DisplayName("MessageModel_默认Clustering")]
    public void MessageModel_DefaultClustering()
    {
        using var consumer = new Consumer();
        Assert.Equal(MessageModels.Clustering, consumer.MessageModel);
    }

    [Fact]
    [DisplayName("MessageModel_可设置为Broadcasting")]
    public void MessageModel_CanSetBroadcasting()
    {
        using var consumer = new Consumer { MessageModel = MessageModels.Broadcasting };
        Assert.Equal(MessageModels.Broadcasting, consumer.MessageModel);
    }

    [Fact]
    [DisplayName("OffsetStorePath_默认为null")]
    public void OffsetStorePath_DefaultNull()
    {
        using var consumer = new Consumer();
        Assert.Null(consumer.OffsetStorePath);
    }

    [Fact]
    [DisplayName("OffsetStorePath_可自定义")]
    public void OffsetStorePath_CanBeCustomized()
    {
        var path = Path.Combine(Path.GetTempPath(), "test_offsets.json");
        using var consumer = new Consumer { OffsetStorePath = path };
        Assert.Equal(path, consumer.OffsetStorePath);
    }
}
