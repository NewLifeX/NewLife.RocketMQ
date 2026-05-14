#if NETSTANDARD2_1_OR_GREATER
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NewLife.Log;

namespace NewLife.RocketMQ.Grpc;

/// <summary>gRPC Push 模式消费者（长轮询驱动）</summary>
/// <remarks>
/// 基于 RocketMQ 5.x gRPC 的 Push 消费语义：
/// 内部使用长轮询线程循环拉取消息，触发 <see cref="OnMessage"/> 回调。
/// - 回调返回 true  → 自动 Ack
/// - 回调返回 false → 自动 ChangeInvisibleDuration（延迟重投）
/// - 回调抛出异常  → 同样触发 ChangeInvisibleDuration
/// 
/// 通过 <see cref="MaxConcurrentConsume"/> 信号量控制并发处理数，
/// 适合消费延迟较高的场景，避免无限积压。
/// </remarks>
/// <example>
/// <code>
/// var pushConsumer = new GrpcPushConsumer
/// {
///     Topic     = "your-topic",
///     Group     = "your-group",
///     Endpoints = "127.0.0.1:8081",
///     OnMessage = async (msg) =>
///     {
///         Console.WriteLine(msg.Body.ToStr());
///         return true; // 消费成功
///     },
/// };
/// await pushConsumer.StartAsync();
/// // ...
/// await pushConsumer.StopAsync();
/// </code>
/// </example>
public class GrpcPushConsumer : IDisposable
{
    #region 属性

    /// <summary>主题</summary>
    public String Topic { get; set; }

    /// <summary>消费组</summary>
    public String Group { get; set; }

    /// <summary>gRPC Proxy 地址（host:port）</summary>
    public String Endpoints { get; set; }

    /// <summary>命名空间（可选）</summary>
    public String Namespace { get; set; }

    /// <summary>每次拉取批量（默认32）</summary>
    public Int32 BatchSize { get; set; } = 32;

    /// <summary>消息不可见时间（Ack 超时，默认30秒）</summary>
    public TimeSpan InvisibleDuration { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>长轮询超时（默认20秒）</summary>
    public TimeSpan LongPollingTimeout { get; set; } = TimeSpan.FromSeconds(20);

    /// <summary>消费失败后重新变为可见的延迟（默认5秒）</summary>
    public TimeSpan RetryInvisibleDuration { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>最大并发消费数（默认20）</summary>
    public Int32 MaxConcurrentConsume { get; set; } = 20;

    /// <summary>消息处理回调。返回 true 则 Ack，返回 false 或抛出异常则 ChangeInvisibleDuration</summary>
    public Func<GrpcMessage, Task<Boolean>> OnMessage { get; set; }

    /// <summary>日志</summary>
    public ILog Log { get; set; } = Logger.Null;

    #endregion

    #region 私有字段

    private GrpcMessagingService _service;
    private CancellationTokenSource _cts;
    private Task _consumeTask;
    private SemaphoreSlim _semaphore;
    private Boolean _disposed;

    #endregion

    #region 启动/停止

    /// <summary>启动 Push Consumer</summary>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException">未设置必要属性时抛出</exception>
    public async Task StartAsync()
    {
        if (Topic.IsNullOrEmpty()) throw new InvalidOperationException("Topic 不能为空");
        if (Group.IsNullOrEmpty()) throw new InvalidOperationException("Group 不能为空");
        if (Endpoints.IsNullOrEmpty()) throw new InvalidOperationException("Endpoints 不能为空");
        if (OnMessage == null) throw new InvalidOperationException("OnMessage 回调不能为空");

        var grpcClient = new GrpcClient { Address = Endpoints };
        _service = new GrpcMessagingService
        {
            Client = grpcClient,
            Namespace = Namespace ?? String.Empty,
            Log = Log,
        };

        _semaphore = new SemaphoreSlim(MaxConcurrentConsume, MaxConcurrentConsume);
        _cts = new CancellationTokenSource();

        // 先查询路由，获取消息队列
        var routeResponse = await _service.QueryRouteAsync(Topic, _cts.Token).ConfigureAwait(false);
        if (routeResponse?.MessageQueues == null || routeResponse.MessageQueues.Count == 0)
            throw new InvalidOperationException($"主题 [{Topic}] 未找到可用队列");

        _consumeTask = Task.Run(() => ConsumeLoopAsync(routeResponse.MessageQueues, _cts.Token), _cts.Token);

        Log.Info($"[GrpcPushConsumer] 已启动，Topic={Topic}，Group={Group}，队列数={routeResponse.MessageQueues.Count}");
    }

    /// <summary>停止 Push Consumer</summary>
    /// <returns></returns>
    public async Task StopAsync()
    {
        if (_cts == null) return;

        _cts.Cancel();
        try
        {
            if (_consumeTask != null)
                await _consumeTask.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // 正常取消
        }

        Log.Info("[GrpcPushConsumer] 已停止");
    }

    #endregion

    #region 消费循环

    private async Task ConsumeLoopAsync(IList<GrpcMessageQueue> queues, CancellationToken cancellationToken)
    {
        var queueIndex = 0;

        while (!cancellationToken.IsCancellationRequested)
        {
            // 轮询各队列
            var queue = queues[queueIndex % queues.Count];
            queueIndex++;

            try
            {
                var messages = await _service.ReceiveMessageAsync(
                    Group,
                    queue,
                    batchSize: BatchSize,
                    invisibleDuration: InvisibleDuration,
                    longPollingTimeout: LongPollingTimeout,
                    cancellationToken: cancellationToken).ConfigureAwait(false);

                foreach (var msg in messages)
                {
                    if (cancellationToken.IsCancellationRequested) break;

                    // 等待并发槽位
                    await _semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

                    // 并发处理（不 await，让循环继续拉取）
                    _ = Task.Run(async () =>
                    {
                        try
                        {
                            await HandleMessageAsync(msg, cancellationToken).ConfigureAwait(false);
                        }
                        finally
                        {
                            _semaphore.Release();
                        }
                    }, cancellationToken);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                Log.Error($"[GrpcPushConsumer] 拉取消息异常: {ex.Message}");
                // 出错后短暂等待，避免疯狂重试
                try { await Task.Delay(1000, cancellationToken).ConfigureAwait(false); }
                catch (OperationCanceledException) { break; }
            }
        }
    }

    private async Task HandleMessageAsync(GrpcMessage msg, CancellationToken cancellationToken)
    {
        var receiptHandle = msg.SystemProperties?.ReceiptHandle;
        var messageId = msg.SystemProperties?.MessageId ?? "(unknown)";

        var success = false;
        try
        {
            success = await OnMessage(msg).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Log.Error($"[GrpcPushConsumer] 处理消息 {messageId} 异常: {ex.Message}");
            success = false;
        }

        if (success)
        {
            // 消费成功 → Ack
            if (!receiptHandle.IsNullOrEmpty())
            {
                try
                {
                    await _service.AckMessageAsync(
                        Topic,
                        Group,
                        [new AckMessageEntry { MessageId = messageId, ReceiptHandle = receiptHandle }],
                        cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    Log.Warn($"[GrpcPushConsumer] Ack 消息 {messageId} 失败: {ex.Message}");
                }
            }
        }
        else
        {
            // 消费失败 → 修改可见时间（延迟重投）
            if (!receiptHandle.IsNullOrEmpty())
            {
                try
                {
                    await _service.ChangeInvisibleDurationAsync(
                        Topic,
                        Group,
                        receiptHandle,
                        messageId,
                        RetryInvisibleDuration,
                        cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    Log.Warn($"[GrpcPushConsumer] ChangeInvisibleDuration 消息 {messageId} 失败: {ex.Message}");
                }
            }
        }
    }

    #endregion

    #region 日志

    /// <summary>释放资源</summary>
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        _cts?.Cancel();
        _cts?.Dispose();
        _service?.Dispose();
        _semaphore?.Dispose();
    }

    #endregion
}
#endif
