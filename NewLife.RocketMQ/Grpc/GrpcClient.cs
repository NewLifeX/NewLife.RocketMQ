#if NETSTANDARD2_1_OR_GREATER
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using NewLife.Log;

namespace NewLife.RocketMQ.Grpc;

/// <summary>轻量级gRPC客户端。基于HttpClient HTTP/2实现gRPC协议</summary>
/// <remarks>
/// gRPC协议格式：
/// - 传输层: HTTP/2
/// - 请求: POST /package.Service/Method
/// - 内容类型: application/grpc
/// - 消息帧: [1字节压缩标志][4字节大端序长度][Protobuf消息体]
/// 
/// 不依赖任何gRPC包，完全使用 HttpClient + 手工帧编码实现。
/// </remarks>
public class GrpcClient : IDisposable
{
    #region 属性
    /// <summary>目标地址。格式如 http://host:port</summary>
    public String Address { get; set; }

    /// <summary>超时时间（毫秒）。默认30000</summary>
    public Int32 Timeout { get; set; } = 30_000;

    /// <summary>访问令牌。用于认证</summary>
    public String AccessKey { get; set; }

    /// <summary>访问密钥</summary>
    public String SecretKey { get; set; }

    /// <summary>客户端标识</summary>
    public String ClientId { get; set; }

    /// <summary>命名空间</summary>
    public String Namespace { get; set; }

    /// <summary>是否跳过服务端证书校验。默认true（内网 gRPC 通常不验证证书）；生产环境建议设为 false 启用证书校验</summary>
    public Boolean SkipCertificateValidation { get; set; } = true;

    /// <summary>日志</summary>
    public ILog Log { get; set; } = Logger.Null;

    /// <summary>性能追踪</summary>
    public ITracer Tracer { get; set; }

    private HttpClient _client;
    #endregion

    #region 构造
    /// <summary>实例化gRPC客户端</summary>
    public GrpcClient() { }

    /// <summary>实例化gRPC客户端</summary>
    /// <param name="address">目标地址</param>
    public GrpcClient(String address) => Address = address;

    /// <summary>释放</summary>
    public void Dispose()
    {
        _client?.Dispose();
        _client = null;
    }
    #endregion

    #region 方法
    /// <summary>确保HttpClient已创建</summary>
    private HttpClient EnsureClient()
    {
        if (_client != null) return _client;
        lock (this)
        {
            if (_client != null) return _client;

            var handler = new HttpClientHandler
            {
                // gRPC 通常不需要验证服务端证书（内网），可通过 SkipCertificateValidation 关闭校验
                ServerCertificateCustomValidationCallback = SkipCertificateValidation ? (_, _, _, _) => true : null,
            };

            var client = new HttpClient(handler)
            {
                BaseAddress = new Uri(Address),
                Timeout = TimeSpan.FromMilliseconds(Timeout),
            };

            _client = client;
            return client;
        }
    }

    /// <summary>Unary调用。发送一个请求，接收一个响应</summary>
    /// <param name="service">服务名。如 apache.rocketmq.v2.MessagingService</param>
    /// <param name="method">方法名。如 SendMessage</param>
    /// <param name="request">请求消息</param>
    /// <param name="cancellationToken">取消通知</param>
    /// <returns>响应数据</returns>
    public async Task<Byte[]> UnaryCallAsync(String service, String method, Byte[] request, CancellationToken cancellationToken = default)
    {
        var client = EnsureClient();
        var path = $"/{service}/{method}";

        using var span = Tracer?.NewSpan($"grpc:{method}", path);
        try
        {
            // 构造gRPC帧：[1字节压缩标志=0][4字节大端序长度][数据]
            var frame = FrameEncode(request);

            var httpRequest = new HttpRequestMessage(HttpMethod.Post, path);
            httpRequest.Version = new Version(2, 0);

            // gRPC 必需的 headers
            httpRequest.Content = new ByteArrayContent(frame);
            httpRequest.Content.Headers.ContentType = new MediaTypeHeaderValue("application/grpc");
            httpRequest.Headers.Add("te", "trailers");

            // 认证和元数据
            SetMetadata(httpRequest);

            var response = await client.SendAsync(httpRequest, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);

            // 检查 HTTP 状态（网关/代理可能返回 404/502 HTML 页面）
            if (!response.IsSuccessStatusCode)
            {
                response.Dispose();
                throw new InvalidOperationException($"gRPC {method} 失败：HTTP {(Int32)response.StatusCode}");
            }

            // 读取响应
            var responseData = await response.Content.ReadAsByteArrayAsync().ConfigureAwait(false);

            // 检查 grpc-status
            CheckGrpcStatus(response);

            // 解码gRPC帧
            var result = FrameDecode(responseData);

            response.Dispose();
            return result;
        }
        catch (Exception ex)
        {
            span?.SetError(ex, null);
            throw;
        }
    }

    /// <summary>Server Streaming调用。发送一个请求，流式接收多个响应</summary>
    /// <param name="service">服务名</param>
    /// <param name="method">方法名</param>
    /// <param name="request">请求消息</param>
    /// <param name="cancellationToken">取消通知</param>
    /// <returns>响应数据流</returns>
    public async IAsyncEnumerable<Byte[]> ServerStreamingCallAsync(String service, String method, Byte[] request, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var client = EnsureClient();
        var path = $"/{service}/{method}";

        using var span = Tracer?.NewSpan($"grpc:{method}:stream", path);
        HttpResponseMessage response = null;
        Stream stream = null;
        try
        {
            var frame = FrameEncode(request);

            var httpRequest = new HttpRequestMessage(HttpMethod.Post, path);
            httpRequest.Version = new Version(2, 0);
            httpRequest.Content = new ByteArrayContent(frame);
            httpRequest.Content.Headers.ContentType = new MediaTypeHeaderValue("application/grpc");
            httpRequest.Headers.Add("te", "trailers");

            SetMetadata(httpRequest);

            response = await client.SendAsync(httpRequest, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
            if (!response.IsSuccessStatusCode)
                throw new InvalidOperationException($"gRPC {method} 失败：HTTP {(Int32)response.StatusCode}");

            stream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);

            // 流式读取多个gRPC帧
            var headerBuf = new Byte[5];
            while (!cancellationToken.IsCancellationRequested)
            {
                // 读取5字节帧头
                var read = await ReadFullAsync(stream, headerBuf, 0, 5, cancellationToken).ConfigureAwait(false);
                if (read < 5) break;

                // 解析帧头（4字节大端序无符号长度）
                var compressed = headerBuf[0] != 0;
                var length = (Int32)(((UInt32)headerBuf[1] << 24) | ((UInt32)headerBuf[2] << 16) | ((UInt32)headerBuf[3] << 8) | headerBuf[4]);
                if (length < 0) throw new InvalidDataException($"gRPC 帧长度非法[{length}]");
                if (length == 0) continue;

                // 读取消息体
                var body = new Byte[length];
                read = await ReadFullAsync(stream, body, 0, length, cancellationToken).ConfigureAwait(false);
                if (read < length) break;

                yield return body;
            }

            // 流结束，检查 grpc-status，RPC 失败时让上层抛异常重试而非静默返回空
            CheckGrpcStatus(response);
        }
        finally
        {
            stream?.Dispose();
            response?.Dispose();
        }
    }
    /// <summary>打开双向流式调用（Telemetry 等）。返回可写请求流与可读响应流的会话</summary>
    /// <param name="service">服务名。如 apache.rocketmq.v2.MessagingService</param>
    /// <param name="method">方法名。如 Telemetry</param>
    /// <param name="firstFrame">首帧数据。发送前先写入请求流，避免流式发送阻塞</param>
    /// <param name="cancellationToken">取消通知</param>
    /// <returns>双向流会话</returns>
    public async Task<GrpcStreamSession> BidirectionalStreamingCallAsync(String service, String method, Byte[] firstFrame = null, CancellationToken cancellationToken = default)
    {
        var client = EnsureClient();
        var path = $"/{service}/{method}";

        using var span = Tracer?.NewSpan($"grpc:{method}:bidi", path);
        try
        {
            // 请求流使用管道流：SendAsync 启动后仍可继续写入后续帧
            var requestStream = new PipeStream();
            if (firstFrame != null && firstFrame.Length > 0)
            {
                var frame = FrameEncode(firstFrame);
                requestStream.Write(frame, 0, frame.Length);
            }

            var httpRequest = new HttpRequestMessage(HttpMethod.Post, path);
            httpRequest.Version = new Version(2, 0);
            httpRequest.Content = new StreamContent(requestStream);
            httpRequest.Content.Headers.ContentType = new MediaTypeHeaderValue("application/grpc");
            httpRequest.Headers.Add("te", "trailers");

            SetMetadata(httpRequest);

            var response = await client.SendAsync(httpRequest, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);

            // 检查 HTTP 状态
            if (!response.IsSuccessStatusCode)
            {
                response.Dispose();
                httpRequest.Dispose();
                throw new InvalidOperationException($"gRPC {method} 失败：HTTP {(Int32)response.StatusCode}");
            }

            var responseStream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);

            return new GrpcStreamSession(httpRequest, response, requestStream, responseStream);
        }
        catch (Exception ex)
        {
            span?.SetError(ex, null);
            throw;
        }
    }

    #endregion

    #region 辅助
    /// <summary>设置gRPC元数据（认证等）</summary>
    /// <param name="request">请求</param>
    private void SetMetadata(HttpRequestMessage request)
    {
        if (!String.IsNullOrEmpty(ClientId))
            request.Headers.TryAddWithoutValidation("x-mq-client-id", ClientId);

        if (!String.IsNullOrEmpty(Namespace))
            request.Headers.TryAddWithoutValidation("x-mq-namespace", Namespace);

        // RocketMQ 5.x gRPC 使用 authorization header
        if (!String.IsNullOrEmpty(AccessKey) && !String.IsNullOrEmpty(SecretKey))
        {
            var timestamp = DateTime.UtcNow.ToString("yyyyMMddTHHmmssZ");
            request.Headers.TryAddWithoutValidation("x-mq-date-time", timestamp);
            request.Headers.TryAddWithoutValidation("authorization",
                $"MQv2-HMAC-SHA1 Credential={AccessKey}, SignedHeaders=x-mq-date-time, Signature={ComputeSignature(timestamp)}");
        }

        request.Headers.TryAddWithoutValidation("x-mq-language", "DOTNET");
        request.Headers.TryAddWithoutValidation("x-mq-protocol", "grpc");
    }

    /// <summary>计算签名</summary>
    /// <param name="dateTime">时间戳</param>
    /// <returns></returns>
    private String ComputeSignature(String dateTime)
    {
        using var hmac = new System.Security.Cryptography.HMACSHA1(System.Text.Encoding.UTF8.GetBytes(SecretKey));
        var data = System.Text.Encoding.UTF8.GetBytes(dateTime);
        return Convert.ToBase64String(hmac.ComputeHash(data));
    }

    /// <summary>gRPC帧编码。[1字节压缩标志][4字节大端序长度][数据]</summary>
    /// <param name="data">数据</param>
    /// <returns></returns>
    public static Byte[] FrameEncode(Byte[] data)
    {
        var len = data?.Length ?? 0;
        var frame = new Byte[5 + len];
        frame[0] = 0; // 不压缩
        frame[1] = (Byte)(len >> 24);
        frame[2] = (Byte)(len >> 16);
        frame[3] = (Byte)(len >> 8);
        frame[4] = (Byte)len;
        if (len > 0) Buffer.BlockCopy(data, 0, frame, 5, len);
        return frame;
    }

    /// <summary>gRPC帧解码。跳过5字节帧头返回消息体</summary>
    /// <param name="frame">帧数据</param>
    /// <returns></returns>
    public static Byte[] FrameDecode(Byte[] frame)
    {
        if (frame == null || frame.Length < 5) return [];

        // 4字节大端序无符号长度，避免符号扩展（帧长 >= 2^31 时按非法处理）
        var length = (Int32)(((UInt32)frame[1] << 24) | ((UInt32)frame[2] << 16) | ((UInt32)frame[3] << 8) | frame[4]);
        if (length <= 0 || frame.Length < 5 + length) return [];

        var data = new Byte[length];
        Buffer.BlockCopy(frame, 5, data, 0, length);
        return data;
    }

    /// <summary>检查gRPC响应状态</summary>
    /// <param name="response">HTTP响应</param>
    internal static void CheckGrpcStatus(HttpResponseMessage response)
    {
        // grpc-status 可能在 trailer 或 header 中
        var grpcStatus = -1;
        String grpcMessage = null;

        // TrailingHeaders 在 .NET 5+ 可用，低版本 try-catch 处理
        try
        {
#if NET5_0_OR_GREATER
            if (response.TrailingHeaders.TryGetValues("grpc-status", out var statusValues))
            {
                var statusStr = statusValues.FirstOrDefault();
                if (statusStr != null) Int32.TryParse(statusStr, out grpcStatus);
            }
            if (response.TrailingHeaders.TryGetValues("grpc-message", out var msgValues))
            {
                grpcMessage = msgValues.FirstOrDefault();
            }
#endif
        }
        catch { }

        // 也检查普通 header（有些服务器在 header 中返回）
        if (grpcStatus < 0 && response.Headers.TryGetValues("grpc-status", out var headerStatusValues))
        {
            var statusStr = headerStatusValues.FirstOrDefault();
            if (statusStr != null) Int32.TryParse(statusStr, out grpcStatus);
        }

        // grpc-status: 0 = OK
        if (grpcStatus > 0)
        {
            throw new InvalidOperationException($"gRPC error {grpcStatus}: {grpcMessage ?? "Unknown error"}");
        }
    }

    /// <summary>确保从流中读取指定长度的数据</summary>
    internal static async Task<Int32> ReadFullAsync(Stream stream, Byte[] buffer, Int32 offset, Int32 count, CancellationToken cancellationToken)
    {
        var totalRead = 0;
        while (totalRead < count)
        {
            var read = await stream.ReadAsync(buffer, offset + totalRead, count - totalRead, cancellationToken).ConfigureAwait(false);
            if (read == 0) break;
            totalRead += read;
        }
        return totalRead;
    }
    #endregion
}

/// <summary>gRPC双向流会话。提供帧级别的读写与资源释放</summary>
/// <remarks>
/// 用于 Telemetry 等双向流式 RPC：
/// - WriteFrameAsync 向请求流写入一帧（客户端→服务端）
/// - ReadFrameAsync 从响应流读取一帧（服务端→客户端），流结束返回 null
/// - CompleteRequest 通知请求流结束（不再发送）
/// </remarks>
public class GrpcStreamSession : IDisposable
{
    private readonly HttpRequestMessage _request;
    private readonly HttpResponseMessage _response;
    private readonly PipeStream _requestStream;
    private readonly Stream _responseStream;
    private Boolean _requestCompleted;

    /// <summary>实例化</summary>
    internal GrpcStreamSession(HttpRequestMessage request, HttpResponseMessage response, PipeStream requestStream, Stream responseStream)
    {
        _request = request;
        _response = response;
        _requestStream = requestStream;
        _responseStream = responseStream;
    }

    /// <summary>写入一帧消息（客户端→服务端）</summary>
    /// <param name="data">消息体（不含gRPC帧头）</param>
    /// <param name="cancellationToken">取消通知</param>
    public async Task WriteFrameAsync(Byte[] data, CancellationToken cancellationToken = default)
    {
        var frame = GrpcClient.FrameEncode(data);
        await _requestStream.WriteAsync(frame, 0, frame.Length, cancellationToken).ConfigureAwait(false);
        await _requestStream.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>读取一帧消息（服务端→客户端）。流结束返回 null</summary>
    /// <param name="cancellationToken">取消通知</param>
    /// <returns>消息体，流结束返回 null</returns>
    public async Task<Byte[]> ReadFrameAsync(CancellationToken cancellationToken = default)
    {
        var headerBuf = new Byte[5];
        var read = await GrpcClient.ReadFullAsync(_responseStream, headerBuf, 0, 5, cancellationToken).ConfigureAwait(false);
        if (read < 5)
        {
            // 流结束，检查 grpc-status，RPC 失败时抛异常而非静默返回
            GrpcClient.CheckGrpcStatus(_response);
            return null;
        }

        var length = (Int32)(((UInt32)headerBuf[1] << 24) | ((UInt32)headerBuf[2] << 16) | ((UInt32)headerBuf[3] << 8) | headerBuf[4]);
        if (length < 0) throw new InvalidDataException($"gRPC 帧长度非法[{length}]");
        if (length == 0) return [];

        var body = new Byte[length];
        read = await GrpcClient.ReadFullAsync(_responseStream, body, 0, length, cancellationToken).ConfigureAwait(false);
        if (read < length)
        {
            GrpcClient.CheckGrpcStatus(_response);
            return null;
        }

        return body;
    }

    /// <summary>通知请求流结束（客户端不再发送）</summary>
    public void CompleteRequest()
    {
        if (_requestCompleted) return;
        _requestCompleted = true;

        _requestStream.Complete();
    }

    /// <summary>释放资源</summary>
    public void Dispose()
    {
        CompleteRequest();
        _requestStream.Dispose();
        _responseStream.Dispose();
        _response.Dispose();
        _request.Dispose();
    }
}

/// <summary>内存管道流。用于 gRPC 双向流的请求流，支持边写边读（生产者-消费者）</summary>
internal class PipeStream : Stream
{
    private readonly Queue<Byte[]> _chunks = new();
    private Int32 _chunkPos;
    private readonly SemaphoreSlim _lock = new(1, 1);
    private readonly SemaphoreSlim _signal = new(0, Int32.MaxValue);
    private Boolean _completed;

    /// <summary>是否可读</summary>
    public override Boolean CanRead => true;

    /// <summary>是否可定位</summary>
    public override Boolean CanSeek => false;

    /// <summary>是否可写</summary>
    public override Boolean CanWrite => true;

    /// <summary>长度</summary>
    public override Int64 Length => throw new NotSupportedException();

    /// <summary>位置</summary>
    public override Int64 Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    /// <summary>定位。不支持</summary>
    public override Int64 Seek(Int64 offset, SeekOrigin origin) => throw new NotSupportedException();

    /// <summary>设置长度。不支持</summary>
    public override void SetLength(Int64 value) => throw new NotSupportedException();

    /// <summary>刷新</summary>
    public override void Flush() { }

    /// <summary>读取。无数据时阻塞等待</summary>
    public override Int32 Read(Byte[] buffer, Int32 offset, Int32 count)
    {
        while (true)
        {
            _lock.Wait();
            try
            {
                if (_chunks.Count > 0)
                {
                    var chunk = _chunks.Peek();
                    var n = Math.Min(count, chunk.Length - _chunkPos);
                    Buffer.BlockCopy(chunk, _chunkPos, buffer, offset, n);
                    _chunkPos += n;
                    if (_chunkPos >= chunk.Length)
                    {
                        _chunks.Dequeue();
                        _chunkPos = 0;
                    }
                    return n;
                }
            }
            finally
            {
                _lock.Release();
            }

            if (_completed) return 0;

            _signal.Wait();
        }
    }

    /// <summary>写入数据块</summary>
    public override void Write(Byte[] buffer, Int32 offset, Int32 count)
    {
        var chunk = new Byte[count];
        Buffer.BlockCopy(buffer, offset, chunk, 0, count);

        _lock.Wait();
        try
        {
            _chunks.Enqueue(chunk);
        }
        finally
        {
            _lock.Release();
        }

        _signal.Release();
    }

    /// <summary>通知写入结束</summary>
    public void Complete()
    {
        _completed = true;
        _signal.Release();
    }

    /// <summary>释放资源</summary>
    protected override void Dispose(Boolean disposing)
    {
        if (disposing)
        {
            _completed = true;
            _signal.Release();
            _lock.Dispose();
            _signal.Dispose();
        }
        base.Dispose(disposing);
    }
}
#endif
