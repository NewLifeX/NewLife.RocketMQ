using System;
using System.ComponentModel;
using NewLife.RocketMQ;
using Xunit;

namespace XUnitTestRocketMQ;

/// <summary>MqSetting配置测试</summary>
public class MqSettingTests
{
    [Fact]
    [DisplayName("MqSetting_属性可设置")]
    public void MqSetting_SetProperties()
    {
        var setting = new MqSetting
        {
            NameServer = "127.0.0.1:9876",
            Topic = "test_topic",
            Group = "test_group",
            Server = "http://ons.aliyun.com",
            AccessKey = "ak",
            SecretKey = "sk",
        };

        Assert.Equal("127.0.0.1:9876", setting.NameServer);
        Assert.Equal("test_topic", setting.Topic);
        Assert.Equal("test_group", setting.Group);
        Assert.Equal("http://ons.aliyun.com", setting.Server);
        Assert.Equal("ak", setting.AccessKey);
        Assert.Equal("sk", setting.SecretKey);
    }
}
