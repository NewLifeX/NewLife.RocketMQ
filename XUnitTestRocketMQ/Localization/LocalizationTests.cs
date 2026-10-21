using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using NewLife;
using NewLife.Configuration;
using NewLife.Log;
using NewLife.RocketMQ;
using Xunit;

namespace XUnitTest.I18n;

/// <summary>运行时多语言本地化测试</summary>
/// <remarks>用例会临时改写 <see cref="Localization.Culture"/>，执行完毕即恢复</remarks>
[Collection("Localization")]
public class LocalizationTests
{
    [Fact(DisplayName = "运行时词表：中文译文生效且支持格式化")]
    public void TranslateBuiltInKey()
    {
        var saved = Localization.Culture;
        try
        {
            Localization.Culture = CultureInfo.GetCultureInfo("zh-CN");

            Assert.Equal("Topic 不能为空", Localization.Translate("Topic cannot be empty."));
            Assert.Equal("初始化offset[broker-a@1] Offset=0", Localization.Translate("Initialized offset [{0}@{1}] Offset={2:n0}", "broker-a", 1, 0));
        }
        finally
        {
            Localization.Culture = saved;
        }
    }

    [Fact(DisplayName = "运行时词表：未支持语言回退英文原文")]
    public void FallbackToEnglish()
    {
        var saved = Localization.Culture;
        try
        {
            Localization.Culture = CultureInfo.GetCultureInfo("en-US");

            Assert.Equal("Topic cannot be empty.", Localization.Translate("Topic cannot be empty."));

            Localization.Culture = CultureInfo.GetCultureInfo("sw-KE");

            Assert.Equal("Topic cannot be empty.", Localization.Translate("Topic cannot be empty."));
        }
        finally
        {
            Localization.Culture = saved;
        }
    }

    [Fact(DisplayName = "带前缀日志：前缀不参与词表查找，正文照常翻译")]
    public void PrefixedLogKeepsTranslation()
    {
        var saved = Localization.Culture;
        var logger = new CaptureLogger();
        try
        {
            Localization.Culture = CultureInfo.GetCultureInfo("zh-CN");

            var producer = new Producer { Log = logger };
            producer.WriteLog("Topic [{0}] is not created on the Broker yet, skipping the initial offset commit", "tp");

            Assert.Single(logger.Messages);
            Assert.Contains("尚未创建", logger.Messages[0]);
            Assert.Contains("tp", logger.Messages[0]);
        }
        finally
        {
            Localization.Culture = saved;
        }
    }

    [Fact(DisplayName = "带前缀日志：英文环境下保持英文原文")]
    public void PrefixedLogStaysEnglish()
    {
        var saved = Localization.Culture;
        var logger = new CaptureLogger();
        try
        {
            Localization.Culture = CultureInfo.GetCultureInfo("en-US");

            var consumer = new Consumer { Log = logger };
            consumer.WriteLog("Failed to reset consume offset: {0}", "timeout");

            Assert.Single(logger.Messages);
            Assert.Contains("Failed to reset consume offset: timeout", logger.Messages[0]);
        }
        finally
        {
            Localization.Culture = saved;
        }
    }

    [Fact(DisplayName = "公开 API 异常：按当前语言输出中文")]
    public void PublicApiExceptionIsLocalized()
    {
        var saved = Localization.Culture;
        try
        {
            Localization.Culture = CultureInfo.GetCultureInfo("zh-CN");

            var producer = new Producer();
            var ex = Assert.Throws<ArgumentException>(() => producer.PublishBatch(new List<NewLife.RocketMQ.Protocol.Message>()));
            Assert.StartsWith("消息集合不能为空。", ex.Message);
        }
        finally
        {
            Localization.Culture = saved;
        }
    }

    [Fact(DisplayName = "公开 API 异常：协议层与 gRPC 提示同样可翻译")]
    public void ProtocolAndGrpcMessagesAreLocalized()
    {
        var saved = Localization.Culture;
        try
        {
            Localization.Culture = CultureInfo.GetCultureInfo("zh-CN");

            var ex = Assert.Throws<NotSupportedException>(() => NewLife.RocketMQ.Protocol.MessageCompressorRegistry.GetOrThrow(5));
            Assert.Contains("未注册", ex.Message);

            var grpc = Localization.Translate("gRPC service not initialized. Set GrpcProxyAddress first.");
            Assert.Contains("GrpcProxyAddress", grpc);
            Assert.NotEqual("gRPC service not initialized. Set GrpcProxyAddress first.", grpc);
        }
        finally
        {
            Localization.Culture = saved;
        }
    }

    [Fact(DisplayName = "配置注释：英文源文可被词表翻译")]
    public void ConfigDescriptionIsTranslatable()
    {
        var saved = Localization.Culture;
        try
        {
            var description = typeof(MqSetting).GetProperty(nameof(MqSetting.NameServer))!
                .GetCustomAttributes(typeof(System.ComponentModel.DescriptionAttribute), false)
                .Cast<System.ComponentModel.DescriptionAttribute>()
                .FirstOrDefault();
            Assert.NotNull(description);
            Assert.Equal("NameServer address. Brokers are discovered from it.", description!.Description);

            // 英文源文 + 词表译文；NewLife.Core 写 ini/xml 注释时按此翻译
            Localization.Culture = CultureInfo.GetCultureInfo("zh-CN");
            Assert.Contains("名称服务器", Localization.Translate(description.Description));
        }
        finally
        {
            Localization.Culture = saved;
        }
    }

    [Fact(DisplayName = "配置注释：写配置时按日志语言翻译")]
    public void ConfigCommentIsLocalized()
    {
        var savedCulture = Localization.Culture;
        var savedLogCulture = Localization.LogCulture;
        try
        {
            Localization.Culture = null;
            Localization.LogCulture = CultureInfo.GetCultureInfo("zh-CN");

            const String source = "NameServer address. Brokers are discovered from it.";
            var comment = ConfigHelper.LocalizeComment(source);

            Assert.NotEqual(source, comment);
            Assert.Contains("名称服务器", comment);
        }
        finally
        {
            Localization.Culture = savedCulture;
            Localization.LogCulture = savedLogCulture;
        }
    }

    /// <summary>收集日志正文的日志器</summary>
    private sealed class CaptureLogger : Logger
    {
        public List<String> Messages { get; } = [];

        protected override void OnWrite(LogLevel level, String format, params Object[] args)
        {
            Messages.Add(args == null || args.Length == 0 ? format : String.Format(format, args));
        }
    }
}