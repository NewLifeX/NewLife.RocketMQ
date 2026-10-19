using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using NewLife;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Grpc;
using Xunit;

namespace XUnitTest.I18n;

/// <summary>跨文化鲁棒性测试</summary>
/// <remarks>
/// 客户端可能运行在任意 <see cref="CultureInfo.CurrentCulture"/> 之下，协议字段与词表都必须区域无关：
/// 数字格式化不能带千位分隔符，大小写转换不能跟随土耳其语等特殊规则，日历不能影响协议时间戳。
/// </remarks>
[Collection("Localization")]
public class CultureRobustnessTests
{
    #region 协议格式化
    [Theory(DisplayName = "外来文化下协议头格式化保持区域无关")]
    [InlineData("de-DE")]
    [InlineData("tr-TR")]
    [InlineData("th-TH")]
    [InlineData("ar-SA")]
    public void ProtocolFormattingIsCultureInvariant(String culture)
    {
        var saved = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo(culture);

            // grpc-timeout 头只接受 ASCII 数字加单位，带千位分隔符会被服务端拒绝
            Assert.Equal("1500m", GrpcClient.FormatTimeout(TimeSpan.FromMilliseconds(1500)));
            Assert.Equal("5000m", GrpcClient.FormatTimeout(TimeSpan.FromSeconds(5)));

            // 协议时间戳固定十进制，不受佛教历、回历等日历影响
            var stamp = new DateTime(2026, 10, 19, 12, 34, 56, DateTimeKind.Utc).ToString("yyyyMMddTHHmmssZ", CultureInfo.InvariantCulture);
            Assert.Equal("20261019T123456Z", stamp);
        }
        finally
        {
            CultureInfo.CurrentCulture = saved;
        }
    }
    #endregion

    #region 词表完整性
    [Fact(DisplayName = "运行时词表：每种语言的每条词条都能命中译文")]
    public void EveryLocaleEntryIsTranslated()
    {
        // 触发库模块初始化，完成 31 种语言词表注册
        _ = new MqSetting();

        var dir = Path.Combine(SourceRoot(), "NewLife.RocketMQ", "Locales");
        Assert.True(Directory.Exists(dir), $"未找到词表目录：{dir}");

        var files = Directory.GetFiles(dir, "*.yml").OrderBy(e => e).ToArray();
        Assert.True(files.Length >= 31, $"词表语言数不足：{files.Length}");

        var saved = Localization.Culture;
        var total = 0;
        var fallback = new List<String>();
        try
        {
            foreach (var file in files)
            {
                var culture = Path.GetFileNameWithoutExtension(file);
                var table = ParseLocaleFile(file);
                Assert.True(table.Count >= 100, $"{culture} 词条过少：{table.Count}");

                Localization.Culture = CultureInfo.GetCultureInfo(culture);

                foreach (var item in table)
                {
                    total++;
                    var text = Localization.Translate(item.Key);

                    // 与译文一致即命中；少数词条（如 Topic）本身与英文相同，视为正常
                    if (text == item.Value) continue;
                    if (item.Value == item.Key) continue;

                    // 与 NewLife.Core 内置词条重名时，类库注册不覆盖（overwrite=false），译文以 Core 为准
                    if (text != item.Key) continue;

                    fallback.Add($"[{culture}] {item.Key}");
                }
            }
        }
        finally
        {
            Localization.Culture = saved;
        }

        Assert.True(total >= 3000, $"校验词条数异常：{total}");
        Assert.True(fallback.Count == 0, "以下词条回退英文：" + Environment.NewLine + fallback.Join(Environment.NewLine));
    }
    #endregion

    #region 源码约定
    [Fact(DisplayName = "库源码不使用区域敏感的大小写转换")]
    public void NoCultureSensitiveCasing()
    {
        var dir = Path.Combine(SourceRoot(), "NewLife.RocketMQ");
        Assert.True(Directory.Exists(dir), $"未找到库源码目录：{dir}");

        var hits = new List<String>();
        foreach (var file in Directory.GetFiles(dir, "*.cs", SearchOption.AllDirectories))
        {
            if (file.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}")) continue;
            if (file.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}")) continue;

            var lines = File.ReadAllLines(file);
            for (var i = 0; i < lines.Length; i++)
            {
                var line = lines[i].Trim();
                if (line.StartsWith("//")) continue;
                if (line.Contains(".ToUpper()") || line.Contains(".ToLower()")) hits.Add($"{Path.GetFileName(file)}:{i + 1} {line}");
            }
        }

        Assert.True(hits.Count == 0, "大小写转换必须用 ToUpperInvariant/ToLowerInvariant（土耳其语会把 i/I 转成 ı/İ）：" + Environment.NewLine + hits.Join(Environment.NewLine));
    }
    #endregion

    #region 辅助
    /// <summary>测试工程所在仓库根目录</summary>
    /// <param name="path">调用方文件路径</param>
    /// <returns></returns>
    private static String SourceRoot([CallerFilePath] String path = null) => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(path)!, "..", ".."));

    /// <summary>解析词表文件。每行形如 "英文源文": "译文"</summary>
    /// <param name="path">词表路径</param>
    /// <returns></returns>
    private static Dictionary<String, String> ParseLocaleFile(String path)
    {
        var dic = new Dictionary<String, String>(StringComparer.Ordinal);
        foreach (var raw in File.ReadAllLines(path))
        {
            var line = raw.Trim();
            if (line.Length == 0 || line.StartsWith("#") || !line.StartsWith("\"")) continue;

            var index = 0;
            var key = ReadQuoted(line, ref index);
            if (key == null) continue;

            var value = ReadQuoted(line, ref index);
            if (value == null) continue;

            dic[key] = value;
        }
        return dic;
    }

    /// <summary>读取引号字符串，处理 \" 与 \\ 转义</summary>
    /// <param name="line">当前行</param>
    /// <param name="index">起始位置，返回后指向结束引号之后</param>
    /// <returns></returns>
    private static String ReadQuoted(String line, ref Int32 index)
    {
        while (index < line.Length && line[index] != '"') index++;
        if (index >= line.Length) return null;
        index++;

        var sb = new StringBuilder();
        while (index < line.Length)
        {
            var ch = line[index];
            if (ch == '\\' && index + 1 < line.Length)
            {
                sb.Append(line[index + 1]);
                index += 2;
                continue;
            }
            if (ch == '"')
            {
                index++;
                return sb.ToString();
            }
            sb.Append(ch);
            index++;
        }

        return null;
    }
    #endregion
}
