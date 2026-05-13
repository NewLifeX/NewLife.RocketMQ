// ============================================================
// RocketMQ Windows 一键安装 & 启动脚本（需要 .NET 10 SDK）
//
//   安装 + 启动：dotnet run --file scripts/RocketMqSetup.cs
//   停止：       dotnet run --file scripts/RocketMqSetup.cs -- --stop
//   状态查看：   dotnet run --file scripts/RocketMqSetup.cs -- --status
// ============================================================

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

await RocketMqSetup.RunAsync(args);

static class RocketMqSetup
{
    #region 配置

    const String VERSION      = "5.3.1";
    const String INSTALL_ROOT = @"C:\RocketMQ";

    static readonly String[] RMQ_MIRRORS =
    [
        $"https://mirrors.tuna.tsinghua.edu.cn/apache/rocketmq/{VERSION}/rocketmq-all-{VERSION}-bin-release.zip",
        $"https://mirrors.aliyun.com/apache/rocketmq/{VERSION}/rocketmq-all-{VERSION}-bin-release.zip",
        $"https://archive.apache.org/dist/rocketmq/{VERSION}/rocketmq-all-{VERSION}-bin-release.zip",
    ];

    static readonly String[] JDK_MIRRORS =
    [
        "https://aka.ms/download-jdk/microsoft-jdk-17.0.11-windows-x64.zip",
        "https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.11%2B9/OpenJDK17U-jdk_x64_windows_hotspot_17.0.11_9.zip",
    ];

    #endregion

    #region 路径

    static String RmqHome    => Path.Combine(INSTALL_ROOT, $"rocketmq-{VERSION}");
    static String JdkHome    => Path.Combine(INSTALL_ROOT, "jdk17");
    static String JavaExe    => Path.Combine(JdkHome, "bin", "java.exe");
    static String StoreDir   => Path.Combine(INSTALL_ROOT, "store");
    static String LogDir     => Path.Combine(INSTALL_ROOT, "logs");
    static String NamesrvLog => Path.Combine(LogDir, "namesrv.log");
    static String BrokerLog  => Path.Combine(LogDir, "broker.log");
    static String BrokerConf => Path.Combine(INSTALL_ROOT, "broker.conf");
    static String PidFile    => Path.Combine(INSTALL_ROOT, "rocketmq.pids");

    #endregion

    static String? _javaExe;

    #region 入口

    public static async Task RunAsync(String[] args)
    {
        if (args.Contains("--stop"))   { StopAll();   return; }
        if (args.Contains("--status")) { ShowStatus(); return; }

        try
        {
            PrintBanner();
            Directory.CreateDirectory(INSTALL_ROOT);
            Directory.CreateDirectory(LogDir);

            await EnsureJavaAsync();
            await EnsureRocketMqAsync();
            WriteBrokerConf();
            await StartServicesAsync();
            PrintSuccess();
        }
        catch (Exception ex)
        {
            PrintError("启动失败：" + ex.Message);
            Environment.Exit(1);
        }
    }

    #endregion

    #region 步骤 1：Java

    static async Task EnsureJavaAsync()
    {
        PrintStep("[1/4] 检查 Java...");

        var sysJava = FindSystemJava();
        if (sysJava != null)
        {
            var ver = GetJavaVersion(sysJava);
            if (ver >= 11)
            {
                _javaExe = sysJava;
                PrintOk($"系统 Java {ver}：{sysJava}");
                return;
            }
            PrintWarn($"系统 Java {ver} 版本过低（需 ≥11），将下载 JDK 17");
        }

        if (File.Exists(JavaExe))
        {
            _javaExe = JavaExe;
            PrintOk($"本地 JDK {GetJavaVersion(JavaExe)}：{JavaExe}");
            return;
        }

        PrintStep("  下载 Microsoft Build of OpenJDK 17...");
        var zipPath = Path.Combine(INSTALL_ROOT, "jdk17.zip");
        await DownloadAsync("JDK 17", JDK_MIRRORS, zipPath);

        PrintStep("  解压 JDK...");
        var tmpDir = Path.Combine(INSTALL_ROOT, "_jdk_tmp");
        SafeDeleteDir(tmpDir);
        ZipFile.ExtractToDirectory(zipPath, tmpDir);
        var inner = Directory.GetDirectories(tmpDir).FirstOrDefault() ?? tmpDir;
        SafeDeleteDir(JdkHome);
        Directory.Move(inner, JdkHome);
        SafeDeleteDir(tmpDir);
        File.Delete(zipPath);

        _javaExe = JavaExe;
        PrintOk($"JDK {GetJavaVersion(JavaExe)} 已安装 → {JdkHome}");
    }

    static String? FindSystemJava()
    {
        var javaHome = Environment.GetEnvironmentVariable("JAVA_HOME");
        if (!String.IsNullOrEmpty(javaHome))
        {
            var exe = Path.Combine(javaHome, "bin", "java.exe");
            if (File.Exists(exe)) return exe;
        }

        foreach (var dir in (Environment.GetEnvironmentVariable("PATH") ?? "").Split(';'))
        {
            if (String.IsNullOrWhiteSpace(dir)) continue;
            var exe = Path.Combine(dir.Trim(), "java.exe");
            if (File.Exists(exe)) return exe;
        }

        return null;
    }

    static Int32 GetJavaVersion(String java)
    {
        try
        {
            var psi = new ProcessStartInfo(java, "-version")
            {
                RedirectStandardError = true,
                UseShellExecute       = false,
                CreateNoWindow        = true,
            };
            using var p = Process.Start(psi)!;
            var output = p.StandardError.ReadToEnd();
            p.WaitForExit();
            var m = Regex.Match(output, @"""(\d+)[._]");
            if (m.Success && Int32.TryParse(m.Groups[1].Value, out var major))
                return major == 1 ? 8 : major;
        }
        catch { }
        return 0;
    }

    #endregion

    #region 步骤 2：RocketMQ

    static async Task EnsureRocketMqAsync()
    {
        PrintStep("[2/4] 检查 RocketMQ...");

        var libDir = Path.Combine(RmqHome, "lib");
        if (Directory.Exists(libDir) && Directory.GetFiles(libDir, "*.jar").Length > 0)
        {
            PrintOk($"已存在：{RmqHome}");
            return;
        }

        var zipPath = Path.Combine(INSTALL_ROOT, $"rocketmq-all-{VERSION}-bin-release.zip");
        await DownloadAsync($"RocketMQ {VERSION}", RMQ_MIRRORS, zipPath);

        PrintStep("  解压 RocketMQ...");
        var tmpDir = Path.Combine(INSTALL_ROOT, "_rmq_tmp");
        SafeDeleteDir(tmpDir);
        ZipFile.ExtractToDirectory(zipPath, tmpDir);
        var inner = Directory.GetDirectories(tmpDir).FirstOrDefault() ?? tmpDir;
        SafeDeleteDir(RmqHome);
        Directory.Move(inner, RmqHome);
        SafeDeleteDir(tmpDir);
        File.Delete(zipPath);

        PrintOk($"RocketMQ {VERSION} 已安装 → {RmqHome}");
    }

    #endregion

    #region 步骤 3：broker.conf

    static void WriteBrokerConf()
    {
        PrintStep("[3/4] 写入 broker.conf...");
        Directory.CreateDirectory(StoreDir);

        var conf = $"""
            brokerClusterName=DefaultCluster
            brokerName=broker-a
            brokerId=0
            deleteWhen=04
            fileReservedTime=48
            brokerRole=ASYNC_MASTER
            flushDiskType=ASYNC_FLUSH
            autoCreateTopicEnable=true
            listenPort=10911
            storePathRootDir={StoreDir.Replace('\\', '/')}
            brokerIP1=127.0.0.1
            """;

        File.WriteAllText(BrokerConf, conf, new UTF8Encoding(false));
        PrintOk($"broker.conf → {BrokerConf}");
    }

    #endregion

    #region 步骤 4：启动

    static async Task StartServicesAsync()
    {
        PrintStep("[4/4] 启动服务...");

        StopAll(silent: true);
        await Task.Delay(1500);

        SafeDeleteDir(StoreDir);
        Directory.CreateDirectory(StoreDir);
        SafeDeleteFile(NamesrvLog);
        SafeDeleteFile(BrokerLog);

        var java    = _javaExe!;
        var confDir = Path.Combine(RmqHome, "conf");
        var jars    = Directory.GetFiles(Path.Combine(RmqHome, "lib"), "*.jar");
        var cp      = confDir + ";" + String.Join(";", jars);
        var jvmOpts = "-server -Xms256m -Xmx512m -XX:+UseG1GC -Dfile.encoding=UTF-8";

        var env = new Dictionary<String, String> { ["ROCKETMQ_HOME"] = RmqHome };

        PrintStep("  启动 NameServer (127.0.0.1:9876)...");
        var nsProc = StartJava(java, $"{jvmOpts} -cp \"{cp}\" org.apache.rocketmq.namesrv.NamesrvStartup", env, NamesrvLog);
        if (!await WaitForKeyword(NamesrvLog, "boot success", 30))
        {
            TryKill(nsProc);
            throw new Exception($"NameServer 启动超时，查看日志：{NamesrvLog}");
        }
        PrintOk($"NameServer 已启动 (PID={nsProc.Id})");

        PrintStep("  启动 Broker (127.0.0.1:10911)...");
        var bkProc = StartJava(java, $"{jvmOpts} -cp \"{cp}\" org.apache.rocketmq.broker.BrokerStartup -n 127.0.0.1:9876 -c \"{BrokerConf}\"", env, BrokerLog);
        if (!await WaitForKeyword(BrokerLog, "boot success", 40))
        {
            TryKill(bkProc);
            TryKill(nsProc);
            throw new Exception($"Broker 启动超时，查看日志：{BrokerLog}");
        }
        PrintOk($"Broker 已启动 (PID={bkProc.Id})");

        File.WriteAllLines(PidFile, [nsProc.Id.ToString(), bkProc.Id.ToString()]);

        PrintStep("  等待 Topic 路由注册...");
        await Task.Delay(5000);
    }

    #endregion

    #region 停止 & 状态

    static void StopAll(Boolean silent = false)
    {
        if (!silent) PrintStep("停止 RocketMQ...");

        if (!File.Exists(PidFile))
        {
            if (!silent) PrintWarn("未找到进程记录文件");
            return;
        }

        var killed = 0;
        foreach (var line in File.ReadAllLines(PidFile))
        {
            if (!Int32.TryParse(line.Trim(), out var pid)) continue;
            try
            {
                var p = Process.GetProcessById(pid);
                p.Kill(entireProcessTree: true);
                if (!silent) PrintOk($"已停止 PID={pid} ({p.ProcessName})");
                killed++;
            }
            catch { }
        }

        File.Delete(PidFile);
        if (!silent && killed == 0) PrintWarn("记录的进程均已不存在");
    }

    static void ShowStatus()
    {
        PrintStep("RocketMQ 状态：");

        if (!File.Exists(PidFile))
        {
            PrintWarn("未找到进程记录（服务可能未启动）");
            return;
        }

        foreach (var line in File.ReadAllLines(PidFile))
        {
            if (!Int32.TryParse(line.Trim(), out var pid)) continue;
            try
            {
                var p = Process.GetProcessById(pid);
                PrintOk($"运行中：PID={pid}  {p.ProcessName}");
            }
            catch
            {
                PrintWarn($"PID={pid} 已停止");
            }
        }
    }

    #endregion

    #region 工具

    static Process StartJava(String java, String arguments, Dictionary<String, String> envVars, String logFile)
    {
        var psi = new ProcessStartInfo(java, arguments)
        {
            UseShellExecute        = false,
            CreateNoWindow         = true,
            RedirectStandardOutput = true,
            RedirectStandardError  = true,
        };
        foreach (var kv in envVars)
        {
            psi.Environment[kv.Key] = kv.Value;
        }
        var proc = Process.Start(psi)!;
        proc.OutputDataReceived += (_, e) => { if (e.Data != null) SafeAppendLog(logFile, e.Data); };
        proc.ErrorDataReceived  += (_, e) => { if (e.Data != null) SafeAppendLog(logFile, e.Data); };
        proc.BeginOutputReadLine();
        proc.BeginErrorReadLine();
        return proc;
    }

    static async Task<Boolean> WaitForKeyword(String logFile, String keyword, Int32 timeoutSec)
    {
        var deadline = DateTime.UtcNow.AddSeconds(timeoutSec);
        while (DateTime.UtcNow < deadline)
        {
            if (File.Exists(logFile))
            {
                try
                {
                    if (File.ReadAllText(logFile).Contains(keyword, StringComparison.OrdinalIgnoreCase))
                    {
                        Console.WriteLine();
                        return true;
                    }
                }
                catch { }
            }
            Console.Write(".");
            await Task.Delay(1000);
        }
        Console.WriteLine();
        return false;
    }

    static async Task DownloadAsync(String name, String[] urls, String dest)
    {
        if (File.Exists(dest))
        {
            Console.WriteLine($"  {name} 缓存已存在（{new FileInfo(dest).Length / 1024}KB），跳过");
            return;
        }

        using var http = new HttpClient { Timeout = TimeSpan.FromMinutes(15) };
        http.DefaultRequestHeaders.Add("User-Agent", "Mozilla/5.0 (compatible; RocketMQ-Setup/1.0)");

        Exception? lastEx = null;
        foreach (var url in urls)
        {
            Console.WriteLine($"  [{name}] {url}");
            try
            {
                var tmp = dest + ".tmp";
                using var resp = await http.GetAsync(url, HttpCompletionOption.ResponseHeadersRead);
                resp.EnsureSuccessStatusCode();

                var total   = resp.Content.Headers.ContentLength ?? -1L;
                var read    = 0L;
                var lastPct = -1;
                using var src = await resp.Content.ReadAsStreamAsync();
                using var dst = File.Create(tmp);
                var buf = new Byte[65536];
                Int32 n;
                while ((n = await src.ReadAsync(buf, 0, buf.Length)) > 0)
                {
                    await dst.WriteAsync(buf, 0, n);
                    read += n;
                    if (total > 0)
                    {
                        var pct = (Int32)(read * 10 / total) * 10;
                        if (pct != lastPct)
                        {
                            Console.Write($"\r  进度：{pct,3}%  {read / 1048576,4}MB / {total / 1048576}MB  ");
                            lastPct = pct;
                        }
                    }
                }
                Console.WriteLine($"\r  下载完成：{read / 1048576}MB{new String(' ', 30)}");
                dst.Close();
                File.Move(tmp, dest, overwrite: true);
                lastEx = null;
                break;
            }
            catch (Exception ex)
            {
                lastEx = ex;
                Console.WriteLine($"\n  失败：{ex.Message}，尝试下一镜像...");
                var tmp = dest + ".tmp";
                if (File.Exists(tmp)) File.Delete(tmp);
            }
        }

        if (lastEx != null) throw new Exception($"所有镜像下载失败：{name}", lastEx);
    }

    static void SafeDeleteDir(String dir) { if (Directory.Exists(dir)) Directory.Delete(dir, true); }
    static void SafeDeleteFile(String f)  { if (File.Exists(f)) File.Delete(f); }
    static void SafeAppendLog(String f, String line) { try { File.AppendAllText(f, line + Environment.NewLine); } catch { } }
    static void TryKill(Process? p) { if (p == null) return; try { p.Kill(entireProcessTree: true); } catch { } }

    #endregion

    #region 输出

    static void PrintBanner()
    {
        Console.ForegroundColor = ConsoleColor.Cyan;
        Console.WriteLine("============================================================");
        Console.WriteLine($"  RocketMQ {VERSION} Windows 安装工具");
        Console.WriteLine($"  安装目录：{INSTALL_ROOT}");
        Console.WriteLine("============================================================");
        Console.ResetColor();
    }

    static void PrintStep(String msg) => Console.WriteLine(msg);

    static void PrintOk(String msg)
    {
        Console.ForegroundColor = ConsoleColor.Green;
        Console.WriteLine("  + " + msg);
        Console.ResetColor();
    }

    static void PrintWarn(String msg)
    {
        Console.ForegroundColor = ConsoleColor.Yellow;
        Console.WriteLine("  ! " + msg);
        Console.ResetColor();
    }

    static void PrintError(String msg)
    {
        Console.ForegroundColor = ConsoleColor.Red;
        Console.WriteLine("  X " + msg);
        Console.ResetColor();
    }

    static void PrintSuccess()
    {
        Console.ForegroundColor = ConsoleColor.Green;
        Console.WriteLine();
        Console.WriteLine("============================================================");
        Console.WriteLine($"  RocketMQ {VERSION} 启动成功！");
        Console.WriteLine();
        Console.WriteLine("  NameServer : 127.0.0.1:9876");
        Console.WriteLine("  Broker     : 127.0.0.1:10911");
        Console.WriteLine();
        Console.WriteLine("  运行集成测试：");
        Console.WriteLine("  $env:ROCKETMQ_NAMESERVER=\"127.0.0.1:9876\"");
        Console.WriteLine("  dotnet test XUnitTestRocketMQ\\XUnitTestRocketMQ.csproj");
        Console.WriteLine();
        Console.WriteLine("  停止：dotnet run --file scripts/RocketMqSetup.cs -- --stop");
        Console.WriteLine("============================================================");
        Console.ResetColor();
    }

    #endregion
}
