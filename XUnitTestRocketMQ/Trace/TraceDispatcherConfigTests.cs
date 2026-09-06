using System;
using System.ComponentModel;
using System.Reflection;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Client;
using Xunit;

namespace XUnitTest.Trace;

/// <summary>
/// 验证 AsyncTraceDispatcher 内部轨迹Producer继承宿主的云厂商/ACL认证配置（issue #117）。
/// EnableMessageTrace=true 时，轨迹消息由内部独立Producer发送到 RMQ_SYS_TRACE_TOPIC，
/// 若未继承 CloudProvider 则无 AccessKey/Signature 签名，阿里云Broker会拒绝（__accessKey is blank）。
/// AsyncTraceDispatcher 为 internal 类型，公开路径需要真实Broker，此处按仓库既有先例（如CommandTests）
/// 用反射直接构造并读取私有字段，实现完全离线的单元验证。
/// </summary>
public class TraceDispatcherConfigTests
{
    private static IDisposable CreateDispatcher(MqBase host)
    {
        var type = typeof(Producer).Assembly.GetType("NewLife.RocketMQ.MessageTrace.AsyncTraceDispatcher");
        Assert.NotNull(type);

        var ctor = type.GetConstructor(BindingFlags.Instance | BindingFlags.NonPublic, null, new[] { typeof(MqBase) }, null);
        Assert.NotNull(ctor);

        return (IDisposable)ctor.Invoke(new Object[] { host });
    }

    private static Producer GetTraceProducer(IDisposable dispatcher)
    {
        var type = dispatcher.GetType();
        var field = type.GetField("_traceProducer", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(field);

        return (Producer)field.GetValue(dispatcher);
    }

    [Fact]
    [DisplayName("轨迹分发器_继承宿主CloudProvider")]
    public void InheritCloudProvider()
    {
        var provider = new AliyunProvider
        {
            AccessKey = "ak",
            SecretKey = "sk",
            InstanceId = "MQ_INST_123",
            OnsChannel = "ALIYUN",
        };
        using var host = new Producer { CloudProvider = provider };

        var dispatcher = CreateDispatcher(host);
        try
        {
            var traceProducer = GetTraceProducer(dispatcher);

            Assert.NotNull(traceProducer.CloudProvider);
            Assert.Same(provider, traceProducer.CloudProvider);
        }
        finally
        {
            dispatcher.Dispose();
        }
    }

    [Fact]
    [DisplayName("轨迹分发器_继承旧版Aliyun配置")]
    public void InheritLegacyAliyun()
    {
        using var host = new Producer();
#pragma warning disable CS0618
        var aliyun = new AliyunOptions();
        // 空密钥赋值时不触发CloudProvider自动同步，模拟 MqSetting.Configure 事后填充密钥的路径
        host.Aliyun = aliyun;
        aliyun.AccessKey = "ak";
        aliyun.SecretKey = "sk";
        aliyun.InstanceId = "MQ_INST_123";
#pragma warning restore CS0618

        // 关键前提：宿主CloudProvider为空，应走旧版配置兼容分支
        Assert.Null(host.CloudProvider);

        var dispatcher = CreateDispatcher(host);
        try
        {
            var traceProducer = GetTraceProducer(dispatcher);

            Assert.NotNull(traceProducer.CloudProvider);
            var provider = Assert.IsType<AliyunProvider>(traceProducer.CloudProvider);
            Assert.Equal("ak", provider.AccessKey);
            Assert.Equal("sk", provider.SecretKey);
            Assert.Equal("MQ_INST_123", provider.InstanceId);
        }
        finally
        {
            dispatcher.Dispose();
        }
    }

    [Fact]
    [DisplayName("轨迹分发器_继承旧版AclOptions配置")]
    public void InheritLegacyAcl()
    {
        using var host = new Producer();
#pragma warning disable CS0618
        host.AclOptions = new AclOptions { AccessKey = "ak", SecretKey = "sk", OnsChannel = "LOCAL" };
#pragma warning restore CS0618

        var dispatcher = CreateDispatcher(host);
        try
        {
            var traceProducer = GetTraceProducer(dispatcher);

            Assert.NotNull(traceProducer.CloudProvider);
            var provider = Assert.IsType<AclProvider>(traceProducer.CloudProvider);
            Assert.Equal("ak", provider.AccessKey);
            Assert.Equal("sk", provider.SecretKey);
        }
        finally
        {
            dispatcher.Dispose();
        }
    }

    [Fact]
    [DisplayName("轨迹分发器_无认证配置时保持为空")]
    public void NoProvider()
    {
        using var host = new Producer();

        var dispatcher = CreateDispatcher(host);
        try
        {
            var traceProducer = GetTraceProducer(dispatcher);

            Assert.Null(traceProducer.CloudProvider);
        }
        finally
        {
            dispatcher.Dispose();
        }
    }
}
