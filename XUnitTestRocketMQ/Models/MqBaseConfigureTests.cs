using System;
using System.ComponentModel;
using NewLife.RocketMQ;
using Xunit;

namespace XUnitTest.Models;

/// <summary>MqBase.Configure 配置映射测试</summary>
public class MqBaseConfigureTests
{
    [Fact]
    [DisplayName("Configure_应用NameServer与Topic与Group")]
    public void Configure_AppliesBasicSettings()
    {
        using var mq = new Producer();
        var setting = new MqSetting
        {
            NameServer = "127.0.0.1:9876",
            Topic = "topic1",
            Group = "group1",
        };

        mq.Configure(setting);

        Assert.Equal("127.0.0.1:9876", mq.NameServerAddress);
        Assert.Equal("topic1", mq.Topic);
        Assert.Equal("group1", mq.Group);
    }

    [Fact]
    [DisplayName("Configure_空值不覆盖已有配置")]
    public void Configure_EmptyValues_KeepExisting()
    {
        using var mq = new Producer
        {
            NameServerAddress = "10.0.0.1:9876",
            Topic = "keep-topic",
            Group = "keep-group",
        };

        mq.Configure(new MqSetting());

        Assert.Equal("10.0.0.1:9876", mq.NameServerAddress);
        Assert.Equal("keep-topic", mq.Topic);
        Assert.Equal("keep-group", mq.Group);
    }

    [Fact]
    [DisplayName("Configure_旧版阿里云配置写入Aliyun选项")]
    public void Configure_LegacyAliyun_WritesOptions()
    {
        using var mq = new Producer();

        mq.Configure(new MqSetting
        {
            Server = "http://onsaddr.example.com",
            AccessKey = "ak",
            SecretKey = "sk",
        });

#pragma warning disable CS0618
        var options = mq.Aliyun;
        Assert.NotNull(options);
        Assert.Equal("http://onsaddr.example.com", options.Server);
        Assert.Equal("ak", options.AccessKey);
        Assert.Equal("sk", options.SecretKey);
#pragma warning restore CS0618
    }

    [Fact]
    [DisplayName("Configure_不覆盖已存在的CloudProvider")]
    public void Configure_KeepsExistingCloudProvider()
    {
        var provider = new AclProvider { AccessKey = "ak", SecretKey = "sk" };
        using var mq = new Producer { CloudProvider = provider };

        mq.Configure(new MqSetting { AccessKey = "other", SecretKey = "other" });

        Assert.Same(provider, mq.CloudProvider);
    }
}
