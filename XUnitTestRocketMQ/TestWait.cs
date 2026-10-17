using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace XUnitTest;

/// <summary>测试等待助手：用条件轮询替代固定 Sleep，缩短等待时间并降低慢环境下的偶发失败</summary>
internal static class TestWait
{
    /// <summary>在超时时间内等待条件成立</summary>
    /// <param name="condition">条件</param>
    /// <param name="timeoutMs">超时毫秒数</param>
    /// <param name="intervalMs">轮询间隔毫秒数</param>
    /// <returns>条件成立返回 true，超时返回 false</returns>
    public static Boolean Until(Func<Boolean> condition, Int32 timeoutMs = 10_000, Int32 intervalMs = 100)
    {
        var sw = Stopwatch.StartNew();
        while (sw.ElapsedMilliseconds < timeoutMs)
        {
            if (condition()) return true;
            System.Threading.Thread.Sleep(intervalMs);
        }

        return condition();
    }

    /// <summary>在超时时间内等待异步条件成立</summary>
    /// <param name="condition">异步条件</param>
    /// <param name="timeoutMs">超时毫秒数</param>
    /// <param name="intervalMs">轮询间隔毫秒数</param>
    /// <returns>条件成立返回 true，超时返回 false</returns>
    public static async Task<Boolean> UntilAsync(Func<Task<Boolean>> condition, Int32 timeoutMs = 10_000, Int32 intervalMs = 100)
    {
        var sw = Stopwatch.StartNew();
        while (sw.ElapsedMilliseconds < timeoutMs)
        {
            if (await condition().ConfigureAwait(false)) return true;
            await Task.Delay(intervalMs).ConfigureAwait(false);
        }

        return await condition().ConfigureAwait(false);
    }
}
