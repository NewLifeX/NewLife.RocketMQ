using Xunit;

namespace XUnitTest.I18n;

/// <summary>本地化测试集合</summary>
/// <remarks>用例会改写进程级的 <see cref="NewLife.Localization.Culture"/>，必须与其它测试集合串行，避免并行用例读到翻译后的消息。</remarks>
[CollectionDefinition("Localization", DisableParallelization = true)]
public class LocalizationCollection { }
