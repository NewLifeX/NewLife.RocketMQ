using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using NewLife.RocketMQ;
using Xunit;

namespace XUnitTest.Protocol;

/// <summary>NativeAOT 元数据清单一致性测试</summary>
/// <remarks>
/// <c>MqJsonContext</c> 的 <c>[JsonSerializable]</c> 触发源生成器产出成员访问器，<c>MqJsonAot</c> 的 <c>[DynamicDependency]</c>
/// 让裁剪器保留属性本身；两份清单任缺其一，AOT 下对象体都会被静默序列化为 <c>{}</c>。
/// 本用例在源码清单层面强制两者同步维护，新增模型时漏改一处即失败。
/// </remarks>
[DisplayName("NativeAOT元数据清单")]
public class AotMetadataTests
{
    [Fact(DisplayName = "JSON上下文与裁剪保留清单一致")]
    public void JsonContextMatchesPreserveList()
    {
        var dir = Path.Combine(SourceRoot(), "NewLife.RocketMQ");
        var context = ReadTypes(Path.Combine(dir, "MqJsonContext.cs"), @"\[JsonSerializable\(typeof\((\w+)\)\)\]");
        var preserve = ReadTypes(Path.Combine(dir, "MqJsonAot.cs"), @"\[DynamicDependency\(DynamicallyAccessedMemberTypes\.PublicProperties,\s*typeof\((\w+)\)\)\]");

        Assert.NotEmpty(context);
        Assert.NotEmpty(preserve);

        var missingPreserve = context.Except(preserve).ToArray();
        Assert.True(missingPreserve.Length == 0, "以下类型在 MqJsonContext 登记却未在 MqJsonAot 保留，AOT 下会序列化为 {}：" + String.Join(", ", missingPreserve));

        var missingContext = preserve.Except(context).ToArray();
        Assert.True(missingContext.Length == 0, "以下类型在 MqJsonAot 保留却未在 MqJsonContext 登记，AOT 下没有成员访问器：" + String.Join(", ", missingContext));
    }

    /// <summary>收集源码中匹配的类型名</summary>
    /// <param name="path">源码路径</param>
    /// <param name="pattern">匹配 typeof(T) 的正则</param>
    /// <returns></returns>
    private static HashSet<String> ReadTypes(String path, String pattern)
    {
        Assert.True(File.Exists(path), $"未找到源码文件：{path}");

        var set = new HashSet<String>(StringComparer.Ordinal);
        foreach (Match match in Regex.Matches(File.ReadAllText(path), pattern))
        {
            set.Add(match.Groups[1].Value);
        }
        return set;
    }

    /// <summary>测试工程所在仓库根目录</summary>
    /// <param name="path">调用方文件路径</param>
    /// <returns></returns>
    private static String SourceRoot([CallerFilePath] String path = null) => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(path), "..", ".."));
}
