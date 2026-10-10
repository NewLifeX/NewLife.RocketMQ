// ============================================================
// RocketMQ NativeAOT 发布基线采集（体积 / 启动开销 / 吞吐）
//
//   运行：dotnet run --file scripts/AotBaseline.cs -- [--publish <发布目录>]
//   参数：--publish DIR       AOT 发布目录，默认 AotSmoke/bin/Release/net10.0/<rid>/publish
//         --runs N            启动耗时采样次数，默认 5（取中位数）
//         --max-exe-mb N      可执行文件大小上限（MB），超出则退出码 1；默认 0 不判定
//         --max-startup-ms N  启动耗时上限（毫秒），超出则退出码 1；默认 0 不判定
//         --json FILE         以 JSON 输出到文件，便于 CI 归档
//
//   输出 Markdown 表格（可直接写入 CI 的 GITHUB_STEP_SUMMARY），退出码 0=通过、1=超出阈值、2=环境错误。
// ============================================================

using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Text;

var publishDir = GetArg("--publish", null);
var runs = Int32.Parse(GetArg("--runs", "5"), CultureInfo.InvariantCulture);
var maxExeMb = Double.Parse(GetArg("--max-exe-mb", "0"), CultureInfo.InvariantCulture);
var maxStartupMs = Double.Parse(GetArg("--max-startup-ms", "0"), CultureInfo.InvariantCulture);
var jsonFile = GetArg("--json", null);

if (String.IsNullOrEmpty(publishDir))
{
    var rid = RuntimeInformation.RuntimeIdentifier;
    publishDir = Path.Combine("AotSmoke", "bin", "Release", "net10.0", rid, "publish");
}

publishDir = Path.GetFullPath(publishDir);
if (!Directory.Exists(publishDir))
{
    Console.Error.WriteLine($"发布目录不存在：{publishDir}，请先执行 dotnet publish AotSmoke/AotSmoke.csproj -c Release -r {RuntimeInformation.RuntimeIdentifier}");
    return 2;
}

var files = new DirectoryInfo(publishDir).GetFiles();
var exe = files.FirstOrDefault(f => !f.Name.EndsWith(".pdb", StringComparison.OrdinalIgnoreCase)
    && (f.Extension.Equals(".exe", StringComparison.OrdinalIgnoreCase) || f.Name == "AotSmoke"));
if (exe == null)
{
    Console.Error.WriteLine($"发布目录中未找到可执行文件：{publishDir}");
    return 2;
}

String Mb(Int64 bytes) => (bytes / 1024d / 1024d).ToString("n2", CultureInfo.InvariantCulture) + " MB";

var symbolBytes = files.Where(f => f.Name.EndsWith(".pdb", StringComparison.OrdinalIgnoreCase)).Sum(f => f.Length);
var totalBytes = files.Sum(f => f.Length);

// 启动开销：多次运行 --probe，取中位数
var startupSamples = new List<Double>();
for (var i = 0; i < runs; i++)
{
    var watch = Stopwatch.StartNew();
    using var process = Process.Start(new ProcessStartInfo(exe.FullName, "--probe")
    {
        RedirectStandardOutput = true,
        UseShellExecute = false,
    })!;
    process.StandardOutput.ReadToEnd();
    process.WaitForExit();
    watch.Stop();
    startupSamples.Add(watch.Elapsed.TotalMilliseconds);
}
startupSamples.Sort();
var startupMedian = startupSamples.Count == 0 ? 0 : startupSamples[startupSamples.Count / 2];

// 吞吐：解析 --bench 输出的 BENCH|名称|ops/s|毫秒
var benches = new List<(String Name, Double Ops, Double Ms)>();
using (var bench = Process.Start(new ProcessStartInfo(exe.FullName, "--bench")
{
    RedirectStandardOutput = true,
    UseShellExecute = false,
})!)
{
    String? line;
    while ((line = bench.StandardOutput.ReadLine()) != null)
    {
        if (!line.StartsWith("BENCH|", StringComparison.Ordinal)) continue;
        var parts = line.Split('|');
        if (parts.Length < 4) continue;
        benches.Add((parts[1], Double.Parse(parts[2], NumberStyles.Any, CultureInfo.InvariantCulture), Double.Parse(parts[3], CultureInfo.InvariantCulture)));
    }
    bench.WaitForExit();
}

var sb = new StringBuilder();
sb.AppendLine("## NativeAOT 发布基线");
sb.AppendLine();
sb.AppendLine($"- 运行时：{RuntimeInformation.RuntimeIdentifier}（{Environment.Version}）");
sb.AppendLine($"- 发布目录：`{publishDir}`");
sb.AppendLine();
sb.AppendLine("| 指标 | 值 |");
sb.AppendLine("|------|----|");
sb.AppendLine($"| 可执行文件 | {Mb(exe.Length)} |");
sb.AppendLine($"| 发布目录（不含符号） | {Mb(totalBytes - symbolBytes)} |");
sb.AppendLine($"| 发布目录（含符号） | {Mb(totalBytes)} |");
sb.AppendLine($"| 启动开销（{startupSamples.Count} 次中位数） | {startupMedian.ToString("n1", CultureInfo.InvariantCulture)} ms |");
foreach (var (name, ops, ms) in benches)
{
    sb.AppendLine($"| {name} | {ops:n0} ops/s（{ms:n0} ms） |");
}

var report = sb.ToString();
Console.WriteLine(report);

if (!String.IsNullOrEmpty(jsonFile))
{
    var json = new StringBuilder();
    json.AppendLine("{");
    json.AppendLine($"  \"rid\": \"{RuntimeInformation.RuntimeIdentifier}\",");
    json.AppendLine($"  \"exeBytes\": {exe.Length},");
    json.AppendLine($"  \"publishBytes\": {totalBytes - symbolBytes},");
    json.AppendLine($"  \"startupMedianMs\": {startupMedian.ToString("n1", CultureInfo.InvariantCulture)},");
    json.AppendLine("  \"benches\": {");
    json.AppendLine(String.Join(",\n", benches.Select(b => $"    \"{b.Name}\": {b.Ops.ToString("n0", CultureInfo.InvariantCulture).Replace(",", "")}")));
    json.AppendLine("  }");
    json.AppendLine("}");
    File.WriteAllText(jsonFile, json.ToString());
    Console.WriteLine($"JSON 已写入 {Path.GetFullPath(jsonFile)}");
}

var failed = false;
if (maxExeMb > 0 && exe.Length / 1024d / 1024d > maxExeMb)
{
    Console.Error.WriteLine($"可执行文件 {Mb(exe.Length)} 超出上限 {maxExeMb} MB");
    failed = true;
}
if (maxStartupMs > 0 && startupMedian > maxStartupMs)
{
    Console.Error.WriteLine($"启动开销 {startupMedian:n1} ms 超出上限 {maxStartupMs} ms");
    failed = true;
}

return failed ? 1 : 0;

String GetArg(String name, String? fallback)
{
    for (var i = 0; i < args.Length - 1; i++)
    {
        if (args[i] == name) return args[i + 1];
    }

    return fallback!;
}
