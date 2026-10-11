#if NETSTANDARD2_1_OR_GREATER || NET5_0_OR_GREATER
using NewLife.Log;
using NewLife.Model;

namespace NewLife.RocketMQ.Grpc;

/// <summary>gRPC客户端资源上报会话（Telemetry长连接）</summary>
/// <remarks>
/// RocketMQ 5.x 的 gRPC 协议要求客户端与 Proxy 之间维持一条 Telemetry 长连接：Proxy 在该连接上缓存客户端设置
/// （生产者主题、消费者订阅关系），并保持客户端注册；连接一旦结束，Proxy 会注销客户端并清理设置，
/// 之后的 ReceiveMessage 会因取不到订阅信息而失败。
/// 
/// 由于 .NET 的 HttpClient 在请求体未结束时不会上抛服务端响应，本会话采用只写长连接实现：
/// 建立连接后写入 Settings 并保持请求体打开，由 Proxy 侧缓存；连接断开时按 <see cref="ReconnectInterval"/> 自动重建，
/// 并按 <see cref="RefreshInterval"/> 周期性重发 Settings 防止注册失效。
/// 服务端下发的指令（校验消息、打印线程栈等）在只写模式下无法响应，属于当前实现的已知限制。
/// </remarks>
/// <example>
/// <code>
/// var session = new GrpcTelemetrySession(service)
/// {
///     Settings = new GrpcSettings { ClientType = GrpcClientType.PRODUCER },
///     Log = Logger.Null,
/// };
/// session.Start();
/// // ... 生产或消费 ...
/// session.Stop();
/// </code>
/// </example>
public class GrpcTelemetrySession : DisposeBase
{
    #region 属性
    /// <summary>gRPC消息服务</summary>
    public GrpcMessagingService Service { get; }

    /// <summary>客户端设置</summary>
    public GrpcSettings Settings { get; set; }

    /// <summary>重建间隔。到期后重建长连接并重新上报设置，默认60秒；设置为零时不重建</summary>
    /// <remarks>
    /// 请求体保持打开时，连接断开无法从发送侧感知（HttpClient 不会结束挂起的 SendAsync），
    /// 因此无法通过探活判断 Proxy 是否重启；改为周期性重建连接，由重建时的设置上报恢复注册关系。
    /// </remarks>
    public TimeSpan RefreshInterval { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>断线重连间隔。默认5秒</summary>
    public TimeSpan ReconnectInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>连接确认时长。连接被拒绝或不可达会在此期间暴露，默认1秒</summary>
    public TimeSpan ConnectTimeout { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>注册生效等待。连接建立后等待片刻再视为就绪，默认1.5秒，避免设置帧尚未被 Proxy 处理</summary>
    public TimeSpan RegistrationDelay { get; set; } = TimeSpan.FromMilliseconds(1500);

    /// <summary>是否已建立连接</summary>
    public Boolean Active { get; private set; }

    /// <summary>已建立（或重建）长连接的次数，用于长稳观测重建稳定性</summary>
    public Int32 ConnectCount { get; private set; }

    /// <summary>建立或保持连接时出现异常的次数，用于观测 Proxy 侧抖动</summary>
    public Int32 ErrorCount { get; private set; }

    /// <summary>日志</summary>
    public ILog Log { get; set; } = Logger.Null;

    /// <summary>性能追踪</summary>
    public ITracer Tracer { get; set; }

    private CancellationTokenSource _cts;
    private Task _task;
    #endregion

    #region 构造
    /// <summary>实例化资源上报会话</summary>
    /// <param name="service">gRPC消息服务</param>
    public GrpcTelemetrySession(GrpcMessagingService service) => Service = service ?? throw new ArgumentNullException(nameof(service));
    #endregion

    #region 方法
    /// <summary>启动长连接。立即返回，连接与保活在后台任务中维护</summary>
    public void Start()
    {
        if (_task != null) return;

        _cts = new CancellationTokenSource();
        _task = Task.Run(() => RunAsync(_cts.Token));
    }

    /// <summary>停止长连接</summary>
    public void Stop()
    {
        var cts = _cts;
        if (cts == null) return;

        _cts = null;
        cts.Cancel();

        try
        {
            _task?.Wait(TimeSpan.FromSeconds(5));
        }
        catch (AggregateException) { }
        catch (ObjectDisposedException) { }

        _task = null;
        Active = false;
        cts.Dispose();
    }

    /// <summary>等待连接就绪。Proxy 侧的订阅关系由该连接维护，就绪前的接收请求可能失败</summary>
    /// <param name="timeout">超时时间</param>
    /// <returns>就绪返回 true</returns>
    public async Task<Boolean> WaitReadyAsync(TimeSpan timeout)
    {
        var end = Runtime.TickCount64 + (Int64)timeout.TotalMilliseconds;
        while (Runtime.TickCount64 < end)
        {
            if (Active) return true;
            if (_cts == null) break;

            await Task.Delay(50).ConfigureAwait(false);
        }

        return Active;
    }

    private async Task RunAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            using var span = Tracer?.NewSpan($"mq:telemetry:grpc");
            try
            {
                using var stream = Service.OpenTelemetryStreamAsync(Settings, cancellationToken);

                // 等待连接建立：连接不可达时不会完成，连接被拒绝时 Completion 先行结束
                var connected = await Task.WhenAny(stream.Connected, stream.Completion, Task.Delay(ConnectTimeout, cancellationToken)).ConfigureAwait(false);
                if (connected != stream.Connected) throw new InvalidOperationException($"Telemetry长连接失败：{stream.Error?.Message}");

                // 等待设置帧被 Proxy 处理，避免紧随其后的接收请求取不到订阅关系
                if (RegistrationDelay > TimeSpan.Zero) await Task.Delay(RegistrationDelay, cancellationToken).ConfigureAwait(false);

                Active = true;
                ConnectCount++;
                span?.AppendTag($"Active={Active}");

                // 保持到重建周期：到期后重建连接并重新上报设置，覆盖 Proxy 重启、注册失效与连接断开未被感知等情况
                var keep = RefreshInterval > TimeSpan.Zero ? Task.Delay(RefreshInterval, cancellationToken) : Task.Delay(Timeout.Infinite, cancellationToken);
                await Task.WhenAny(stream.Completion, keep).ConfigureAwait(false);
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                ErrorCount++;
                span?.SetError(ex, null);
                Log.Warn("Telemetry长连接异常[{0}]：{1}", Service.Client?.Address, ex.Message);
            }
            finally
            {
                Active = false;
            }

            if (cancellationToken.IsCancellationRequested) break;

            try
            {
                await Task.Delay(ReconnectInterval, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) { break; }
        }
    }
    #endregion

    #region 释放
    /// <summary>释放资源</summary>
    /// <param name="disposing">是否释放托管资源</param>
    protected override void Dispose(Boolean disposing)
    {
        if (disposing) Stop();

        base.Dispose(disposing);
    }
    #endregion
}
#endif
