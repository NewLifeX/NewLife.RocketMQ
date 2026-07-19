using System;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Protocol;
using Xunit;

namespace XUnitTest.Cloud;

/// <summary>云厂商适配器单元测试。不依赖 RocketMQ 服务器，测试 Provider 自身的逻辑</summary>
[System.ComponentModel.DisplayName("云厂商适配器单元测试")]
public class SupportCloudProviderTests
{
    [Fact]
    [System.ComponentModel.DisplayName("AclProvider_ACL2.0属性_默认Disabled")]
    public void AclProvider_DefaultAclEnabled_False()
    {
        var provider = new AclProvider
        {
            AccessKey = "testKey",
            SecretKey = "testSecret",
        };

        Assert.False(provider.AclEnabled);
        Assert.Equal(0, provider.ResourceType);
        Assert.Null(provider.ResourceName);
    }

    [Fact]
    [System.ComponentModel.DisplayName("AclProvider_ACL2.0属性_设置后生效")]
    public void AclProvider_AclEnabled_PropertiesSet()
    {
        var provider = new AclProvider
        {
            AccessKey = "testKey",
            SecretKey = "testSecret",
            AclEnabled = true,
            ResourceType = 1,
            ResourceName = "testTopic",
        };

        Assert.True(provider.AclEnabled);
        Assert.Equal(1, provider.ResourceType);
        Assert.Equal("testTopic", provider.ResourceName);
    }

    [Fact]
    [System.ComponentModel.DisplayName("AclProvider_Name_非空")]
    public void AclProvider_Name_NotEmpty()
    {
        var provider = new AclProvider
        {
            AccessKey = "testKey",
            SecretKey = "testSecret",
        };

        Assert.False(String.IsNullOrEmpty(provider.Name));
    }

    [Fact]
    [System.ComponentModel.DisplayName("AclProvider_TransformTopic_不转换")]
    public void AclProvider_TransformTopic_NoChange()
    {
        var provider = new AclProvider
        {
            AccessKey = "testKey",
            SecretKey = "testSecret",
        };

        var topic = "test_topic";
        var result = provider.TransformTopic(topic);

        Assert.Equal(topic, result);
    }

    [Fact]
    [System.ComponentModel.DisplayName("AclProvider_TransformGroup_不转换")]
    public void AclProvider_TransformGroup_NoChange()
    {
        var provider = new AclProvider
        {
            AccessKey = "testKey",
            SecretKey = "testSecret",
        };

        var group = "test_group";
        var result = provider.TransformGroup(group);

        Assert.Equal(group, result);
    }

    [Fact]
    [System.ComponentModel.DisplayName("AclProvider_GetNameServerAddress_返回null")]
    public void AclProvider_GetNameServerAddress_ReturnsNull()
    {
        var provider = new AclProvider
        {
            AccessKey = "testKey",
            SecretKey = "testSecret",
        };

        Assert.Null(provider.GetNameServerAddress());
    }

    [Fact]
    [System.ComponentModel.DisplayName("AliyunProvider_实例ID前缀_转换Topic")]
    public void AliyunProvider_InstanceId_TransformsTopic()
    {
        var provider = new AliyunProvider
        {
            AccessKey = "testKey",
            SecretKey = "testSecret",
            InstanceId = "MQ_INST_1234",
        };

        var topic = "test_topic";
        var result = provider.TransformTopic(topic);

        Assert.Equal("MQ_INST_1234%test_topic", result);
    }

    [Fact]
    [System.ComponentModel.DisplayName("AliyunProvider_实例ID前缀_转换Group")]
    public void AliyunProvider_InstanceId_TransformsGroup()
    {
        var provider = new AliyunProvider
        {
            AccessKey = "testKey",
            SecretKey = "testSecret",
            InstanceId = "MQ_INST_1234",
        };

        var group = "GID_test";
        var result = provider.TransformGroup(group);

        Assert.Equal("MQ_INST_1234%GID_test", result);
    }

    [Fact]
    [System.ComponentModel.DisplayName("AliyunProvider_无实例ID_不转换Topic")]
    public void AliyunProvider_NoInstanceId_NoTransform()
    {
        var provider = new AliyunProvider
        {
            AccessKey = "testKey",
            SecretKey = "testSecret",
        };

        var topic = "test_topic";
        var result = provider.TransformTopic(topic);

        Assert.Equal(topic, result);
    }

    [Fact]
    [System.ComponentModel.DisplayName("HuaweiProvider_实例ID_不转换Topic")]
    public void HuaweiProvider_InstanceId_NoTransform()
    {
        var provider = new HuaweiProvider
        {
            AccessKey = "testKey",
            SecretKey = "testSecret",
            InstanceId = "HW_INST_5678",
        };

        var topic = "test_topic";
        var result = provider.TransformTopic(topic);

        // 华为云文档说明不转换 Topic/Group
        Assert.Equal(topic, result);
    }

    [Fact]
    [System.ComponentModel.DisplayName("TencentProvider_Namespace前缀_转换Topic")]
    public void TencentProvider_Namespace_TransformsTopic()
    {
        var provider = new TencentProvider
        {
            AccessKey = "testSecretId",
            SecretKey = "testSecretKey",
            Namespace = "ns_test",
        };

        var topic = "test_topic";
        var result = provider.TransformTopic(topic);

        Assert.Equal("ns_test%test_topic", result);
    }

    [Fact]
    [System.ComponentModel.DisplayName("TencentProvider_Namespace前缀_转换Group")]
    public void TencentProvider_Namespace_TransformsGroup()
    {
        var provider = new TencentProvider
        {
            AccessKey = "testSecretId",
            SecretKey = "testSecretKey",
            Namespace = "ns_test",
        };

        var group = "GID_test";
        var result = provider.TransformGroup(group);

        Assert.Equal("ns_test%GID_test", result);
    }

    [Fact]
    [System.ComponentModel.DisplayName("TencentProvider_无Namespace_不转换Topic")]
    public void TencentProvider_NoNamespace_NoTransform()
    {
        var provider = new TencentProvider
        {
            AccessKey = "testSecretId",
            SecretKey = "testSecretKey",
        };

        var topic = "test_topic";
        var result = provider.TransformTopic(topic);

        Assert.Equal(topic, result);
    }

    [Fact]
    [System.ComponentModel.DisplayName("HuaweiProvider_EnableSsl_默认False")]
    public void HuaweiProvider_EnableSsl_DefaultFalse()
    {
        var provider = new HuaweiProvider
        {
            AccessKey = "testKey",
            SecretKey = "testSecret",
        };

        // 华为云 EnableSsl 默认 false，由用户按需开启
        Assert.False(provider.EnableSsl);
    }

    [Fact]
    [System.ComponentModel.DisplayName("ICloudProvider_接口一致性_所有Provider实现")]
    public void AllProviders_ImplementInterface()
    {
        var providers = new ICloudProvider[]
        {
            new AliyunProvider { AccessKey = "ak", SecretKey = "sk" },
            new HuaweiProvider { AccessKey = "ak", SecretKey = "sk" },
            new TencentProvider { AccessKey = "ak", SecretKey = "sk" },
            new AclProvider { AccessKey = "ak", SecretKey = "sk" },
        };

        foreach (var provider in providers)
        {
            Assert.NotNull(provider.Name);
            Assert.Equal("ak", provider.AccessKey);
            Assert.Equal("sk", provider.SecretKey);
        }
    }

    [Fact]
    [System.ComponentModel.DisplayName("AliyunOptions_AliyunProvider_桥接一致性")]
    public void AliyunOptions_Bridge_Consistency()
    {
        var options = new AliyunOptions
        {
            AccessKey = "bridgeAk",
            SecretKey = "bridgeSk",
            InstanceId = "MQ_INST_BRIDGE",
            OnsChannel = "ALIYUN",
        };

        var provider = new AliyunProvider
        {
            AccessKey = options.AccessKey,
            SecretKey = options.SecretKey,
            InstanceId = options.InstanceId,
        };

        Assert.Equal(options.AccessKey, provider.AccessKey);
        Assert.Equal(options.SecretKey, provider.SecretKey);
        Assert.Equal(options.InstanceId, provider.InstanceId);
    }

    [Fact]
    [System.ComponentModel.DisplayName("AclOptions_AclProvider_桥接一致性")]
    public void AclOptions_Bridge_Consistency()
    {
        var options = new AclOptions
        {
            AccessKey = "bridgeAk",
            SecretKey = "bridgeSk",
            OnsChannel = "LOCAL",
        };

        var provider = new AclProvider
        {
            AccessKey = options.AccessKey,
            SecretKey = options.SecretKey,
        };

        Assert.Equal(options.AccessKey, provider.AccessKey);
        Assert.Equal(options.SecretKey, provider.SecretKey);
    }
}
