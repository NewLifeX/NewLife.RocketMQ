using System;
using System.Runtime.CompilerServices;
using NewLife.Log;

namespace XUnitTest;

/// <summary>测试环境初始化</summary>
internal static class TestEnvironment
{
    /// <summary>默认关闭框架日志，避免集成测试输出数 MB 日志拖慢 dotnet test / VS 测试窗口；排查时设置 ROCKETMQ_TEST_VERBOSE=1 恢复</summary>
    [ModuleInitializer]
    internal static void Init()
    {
        if (Environment.GetEnvironmentVariable("ROCKETMQ_TEST_VERBOSE") == "1") return;

        XTrace.Log = Logger.Null;
    }
}
