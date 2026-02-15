using System;
using System.ComponentModel;
using NewLife.RocketMQ;
using Xunit;

namespace XUnitTestRocketMQ;

/// <summary>云厂商适配器接口测试</summary>
public class CloudProviderTests
{
    #region AliyunProvider
    [Fact]
    [DisplayName("AliyunProvider_默认通道为ALIYUN")]
    public void AliyunProvider_DefaultOnsChannel()
    {
        var provider = new AliyunProvider();
        Assert.Equal("Aliyun", provider.Name);
        Assert.Equal("ALIYUN", provider.OnsChannel);
    }

    [Fact]
    [DisplayName("AliyunProvider_有InstanceId时转换Topic")]
    public void AliyunProvider_TransformTopic_WithInstanceId()
    {
        var provider = new AliyunProvider { InstanceId = "MQ_INST_123" };

        var result = provider.TransformTopic("test_topic");
        Assert.Equal("MQ_INST_123%test_topic", result);
    }

    [Fact]
    [DisplayName("AliyunProvider_无InstanceId时不转换Topic")]
    public void AliyunProvider_TransformTopic_WithoutInstanceId()
    {
        var provider = new AliyunProvider();

        var result = provider.TransformTopic("test_topic");
        Assert.Equal("test_topic", result);
    }

    [Fact]
    [DisplayName("AliyunProvider_已有前缀时不重复添加")]
    public void AliyunProvider_TransformTopic_AlreadyPrefixed()
    {
        var provider = new AliyunProvider { InstanceId = "MQ_INST_123" };

        var result = provider.TransformTopic("MQ_INST_123%test_topic");
        Assert.Equal("MQ_INST_123%test_topic", result);
    }

    [Fact]
    [DisplayName("AliyunProvider_有InstanceId时转换Group")]
    public void AliyunProvider_TransformGroup_WithInstanceId()
    {
        var provider = new AliyunProvider { InstanceId = "MQ_INST_123" };

        var result = provider.TransformGroup("test_group");
        Assert.Equal("MQ_INST_123%test_group", result);
    }

    [Fact]
    [DisplayName("AliyunProvider_无Server时GetNameServerAddress返回null")]
    public void AliyunProvider_GetNameServerAddress_NoServer()
    {
        var provider = new AliyunProvider();

        var result = provider.GetNameServerAddress();
        Assert.Null(result);
    }

    [Fact]
    [DisplayName("AliyunProvider_非HTTP地址时GetNameServerAddress返回null")]
    public void AliyunProvider_GetNameServerAddress_NonHttp()
    {
        var provider = new AliyunProvider { Server = "tcp://example.com" };

        var result = provider.GetNameServerAddress();
        Assert.Null(result);
    }

    [Fact]
    [DisplayName("AliyunProvider_FromOptions转换旧版选项")]
    public void AliyunProvider_FromOptions()
    {
        var options = new AliyunOptions
        {
            AccessKey = "ak",
            SecretKey = "sk",
            InstanceId = "inst1",
            OnsChannel = "ALIYUN",
            Server = "http://test.com",
        };

        var provider = AliyunProvider.FromOptions(options);

        Assert.NotNull(provider);
        Assert.Equal("ak", provider.AccessKey);
        Assert.Equal("sk", provider.SecretKey);
        Assert.Equal("inst1", provider.InstanceId);
        Assert.Equal("ALIYUN", provider.OnsChannel);
        Assert.Equal("http://test.com", provider.Server);
    }

    [Fact]
    [DisplayName("AliyunProvider_FromOptions_Null返回Null")]
    public void AliyunProvider_FromOptions_Null()
    {
        var result = AliyunProvider.FromOptions(null);
        Assert.Null(result);
    }
    #endregion

    #region AclProvider
    [Fact]
    [DisplayName("AclProvider_默认属性")]
    public void AclProvider_Defaults()
    {
        var provider = new AclProvider();
        Assert.Equal("ACL", provider.Name);
        Assert.Equal("", provider.OnsChannel);
    }

    [Fact]
    [DisplayName("AclProvider_不转换Topic和Group")]
    public void AclProvider_NoTransform()
    {
        var provider = new AclProvider();

        Assert.Equal("test_topic", provider.TransformTopic("test_topic"));
        Assert.Equal("test_group", provider.TransformGroup("test_group"));
    }

    [Fact]
    [DisplayName("AclProvider_GetNameServerAddress返回null")]
    public void AclProvider_GetNameServerAddress_Null()
    {
        var provider = new AclProvider();
        Assert.Null(provider.GetNameServerAddress());
    }

    [Fact]
    [DisplayName("AclProvider_FromOptions转换旧版选项")]
    public void AclProvider_FromOptions()
    {
        var options = new AclOptions
        {
            AccessKey = "acl_ak",
            SecretKey = "acl_sk",
            OnsChannel = "LOCAL",
        };

        var provider = AclProvider.FromOptions(options);

        Assert.NotNull(provider);
        Assert.Equal("acl_ak", provider.AccessKey);
        Assert.Equal("acl_sk", provider.SecretKey);
        Assert.Equal("LOCAL", provider.OnsChannel);
    }

    [Fact]
    [DisplayName("AclProvider_FromOptions_Null返回Null")]
    public void AclProvider_FromOptions_Null()
    {
        var result = AclProvider.FromOptions(null);
        Assert.Null(result);
    }
    #endregion

    #region HuaweiProvider
    [Fact]
    [DisplayName("HuaweiProvider_默认通道为HUAWEI")]
    public void HuaweiProvider_Defaults()
    {
        var provider = new HuaweiProvider();
        Assert.Equal("Huawei", provider.Name);
        Assert.Equal("HUAWEI", provider.OnsChannel);
        Assert.False(provider.EnableSsl);
    }

    [Fact]
    [DisplayName("HuaweiProvider_不转换Topic和Group")]
    public void HuaweiProvider_NoTransform()
    {
        var provider = new HuaweiProvider();

        Assert.Equal("test_topic", provider.TransformTopic("test_topic"));
        Assert.Equal("test_group", provider.TransformGroup("test_group"));
    }

    [Fact]
    [DisplayName("HuaweiProvider_GetNameServerAddress返回null")]
    public void HuaweiProvider_GetNameServerAddress_Null()
    {
        var provider = new HuaweiProvider();
        Assert.Null(provider.GetNameServerAddress());
    }
    #endregion

    #region TencentProvider
    [Fact]
    [DisplayName("TencentProvider_默认通道为TENCENT")]
    public void TencentProvider_Defaults()
    {
        var provider = new TencentProvider();
        Assert.Equal("Tencent", provider.Name);
        Assert.Equal("TENCENT", provider.OnsChannel);
    }

    [Fact]
    [DisplayName("TencentProvider_有Namespace时转换Topic")]
    public void TencentProvider_TransformTopic_WithNamespace()
    {
        var provider = new TencentProvider { Namespace = "ns1" };

        var result = provider.TransformTopic("test_topic");
        Assert.Equal("ns1%test_topic", result);
    }

    [Fact]
    [DisplayName("TencentProvider_无Namespace时不转换Topic")]
    public void TencentProvider_TransformTopic_WithoutNamespace()
    {
        var provider = new TencentProvider();

        var result = provider.TransformTopic("test_topic");
        Assert.Equal("test_topic", result);
    }

    [Fact]
    [DisplayName("TencentProvider_有Namespace时转换Group")]
    public void TencentProvider_TransformGroup_WithNamespace()
    {
        var provider = new TencentProvider { Namespace = "ns1" };

        var result = provider.TransformGroup("test_group");
        Assert.Equal("ns1%test_group", result);
    }

    [Fact]
    [DisplayName("TencentProvider_已有前缀时不重复添加")]
    public void TencentProvider_TransformTopic_AlreadyPrefixed()
    {
        var provider = new TencentProvider { Namespace = "ns1" };

        var result = provider.TransformTopic("ns1%test_topic");
        Assert.Equal("ns1%test_topic", result);
    }

    [Fact]
    [DisplayName("TencentProvider_GetNameServerAddress返回null")]
    public void TencentProvider_GetNameServerAddress_Null()
    {
        var provider = new TencentProvider();
        Assert.Null(provider.GetNameServerAddress());
    }
    #endregion

    #region MqBase集成
    [Fact]
    [DisplayName("MqBase_CloudProvider默认为null")]
    public void MqBase_CloudProvider_DefaultNull()
    {
        using var producer = new Producer();
        Assert.Null(producer.CloudProvider);
    }

    [Fact]
    [DisplayName("MqBase_设置CloudProvider")]
    public void MqBase_CloudProvider_CanBeSet()
    {
        using var producer = new Producer
        {
            CloudProvider = new AliyunProvider
            {
                AccessKey = "ak",
                SecretKey = "sk",
                InstanceId = "MQ_INST_123",
            }
        };

        Assert.NotNull(producer.CloudProvider);
        Assert.IsType<AliyunProvider>(producer.CloudProvider);
        Assert.Equal("ak", producer.CloudProvider.AccessKey);
    }

    [Fact]
    [DisplayName("MqBase_设置旧版Aliyun自动同步到CloudProvider")]
    public void MqBase_LegacyAliyun_SyncsToCloudProvider()
    {
#pragma warning disable CS0618
        using var producer = new Producer
        {
            Aliyun = new AliyunOptions
            {
                AccessKey = "ak",
                SecretKey = "sk",
            }
        };
#pragma warning restore CS0618

        Assert.NotNull(producer.CloudProvider);
        Assert.IsType<AliyunProvider>(producer.CloudProvider);
        Assert.Equal("ak", producer.CloudProvider.AccessKey);
    }

    [Fact]
    [DisplayName("MqBase_设置旧版AclOptions自动同步到CloudProvider")]
    public void MqBase_LegacyAclOptions_SyncsToCloudProvider()
    {
#pragma warning disable CS0618
        using var producer = new Producer
        {
            AclOptions = new AclOptions
            {
                AccessKey = "acl_ak",
                SecretKey = "acl_sk",
            }
        };
#pragma warning restore CS0618

        Assert.NotNull(producer.CloudProvider);
        Assert.IsType<AclProvider>(producer.CloudProvider);
        Assert.Equal("acl_ak", producer.CloudProvider.AccessKey);
    }

    [Fact]
    [DisplayName("MqBase_显式设置CloudProvider不被旧版属性覆盖")]
    public void MqBase_ExplicitCloudProvider_NotOverridden()
    {
        var acl = new AclProvider { AccessKey = "explicit_ak", SecretKey = "sk" };
        using var producer = new Producer
        {
            CloudProvider = acl,
        };

#pragma warning disable CS0618
        // 设置旧版属性时，因为 CloudProvider 已有值，不会覆盖
        producer.Aliyun = new AliyunOptions { AccessKey = "old_ak", SecretKey = "sk" };
#pragma warning restore CS0618

        Assert.Same(acl, producer.CloudProvider);
        Assert.Equal("explicit_ak", producer.CloudProvider.AccessKey);
    }

    [Fact]
    [DisplayName("MqBase_腾讯云Provider集成测试")]
    public void MqBase_TencentProvider_Integration()
    {
        using var producer = new Producer
        {
            CloudProvider = new TencentProvider
            {
                AccessKey = "tencent_id",
                SecretKey = "tencent_key",
                Namespace = "ns_test",
            }
        };

        Assert.NotNull(producer.CloudProvider);
        Assert.IsType<TencentProvider>(producer.CloudProvider);
        Assert.Equal("tencent_id", producer.CloudProvider.AccessKey);

        var tp = (TencentProvider)producer.CloudProvider;
        Assert.Equal("ns_test%my_topic", tp.TransformTopic("my_topic"));
        Assert.Equal("ns_test%my_group", tp.TransformGroup("my_group"));
    }
    #endregion
}
