using System.Net.Sockets;
using System.Security.Cryptography;
using NewLife.Data;
using NewLife.Log;
using NewLife.Net;
using NewLife.RocketMQ.Client;
using NewLife.RocketMQ.Protocol;
using NewLife.Serialization;

namespace NewLife.RocketMQ;

/// <summary>集群客户端</summary>
/// <remarks>
/// 维护到一个集群的客户端连接，内部采用负载均衡调度算法。
/// </remarks>
public abstract class ClusterClient : DisposeBase
{
    #region 属性
    /// <summary>编号</summary>
    public String Id { get; set; }

    /// <summary>名称</summary>
    public String Name { get; set; }

    /// <summary>超时。默认3000ms</summary>
    public Int32 Timeout { get; set; } = 3_000;

    /// <summary>服务器地址集合</summary>
    public NetUri[] Servers { get; set; }

    /// <summary>配置</summary>
    public MqBase Config { get; set; }

    /// <summary>性能跟踪</summary>
    public ITracer Tracer { get; set; }

    private ISocketClient _Client;
    private SerializeType _serializeType = SerializeType.JSON;
    #endregion

    #region 构造
    /// <summary>实例化</summary>
    public ClusterClient()
    {
        //_Pool = new MyPool { Client = this };
    }

    /// <summary>销毁</summary>
    /// <param name="disposing"></param>
    protected override void Dispose(Boolean disposing)
    {
        base.Dispose(disposing);

        //_Pool.TryDispose();
        _Client.TryDispose();
    }
    #endregion

    #region 方法
    /// <summary>开始</summary>
    public void Start()
    {
        using var span = Tracer?.NewSpan($"mq:{Name}:Start", Servers);
        OnStart();
    }

    /// <summary>开始</summary>
    protected virtual void OnStart()
    {
        WriteLog("集群地址：{0}", Servers.Join(";"));

        if (Config != null)
            _serializeType = Config.SerializeType;

        EnsureCreate();
    }

    /// <summary>确保创建连接</summary>
    protected void EnsureCreate()
    {
        var client = _Client;
        if (client != null && client.Active && !client.Disposed) return;
        lock (this)
        {
            client = _Client;
            if (client != null && client.Active && !client.Disposed) return;
            _Client = null;

            foreach (var uri in Servers)
            {
                WriteLog("正在连接[{0}]", uri);

                if (uri.Type == NetType.Unknown) uri.Type = NetType.Tcp;

                client = uri.CreateRemote();
                client.Timeout = Timeout;
                client.Log = Log;
                if (Log != null && Log.Level <= LogLevel.Debug) client.Tracer = Tracer;
                client.Add(new MqCodec { Timeout = Timeout });

                // 关闭Tcp延迟以合并小包的算法，降低延迟
                if (client is TcpSession tcp)
                {
                    tcp.SslProtocol = Config.SslProtocol;
                    tcp.Certificate = Config.Certificate;
                    tcp.NoDelay = true;
                }

                try
                {
                    if (client.Open())
                    {
                        client.Received += Client_Received;
                        _Client = client;
                        break;
                    }
                }
                catch { }
            }

            if (_Client == null) throw new XException("[{0}]集群所有地址[{1}]连接失败！", Name, Servers.Length);
        }
    }

    private Int32 g_id;
    /// <summary>发送命令</summary>
    /// <param name="cmd"></param>
    /// <param name="waitResult"></param>
    /// <param name="cancellationToken">取消通知</param>
    /// <returns></returns>
    protected virtual async Task<Command> SendAsync(Command cmd, Boolean waitResult, CancellationToken cancellationToken = default)
    {
        if (cmd.Header.Opaque == 0) cmd.Header.Opaque = Interlocked.Increment(ref g_id);

        if (Log != null && Log.Level <= LogLevel.Debug) WriteLog("=> {0}", cmd);

        var code = (RequestCode)cmd.Header.Code;
        using var span = Tracer?.NewSpan($"mq:{Name}:SendAsync:{code}");

        // 签名
        SetSignature(cmd);

        EnsureCreate();
        var client = _Client;
        try
        {
            if (span is DefaultSpan ds && ds.TraceFlag > 0)
            {
                span.AppendTag(cmd);
                span.AppendTag(cmd.Payload?.ToStr());
            }

            if (waitResult)
            {
                var rs = await client.SendMessageAsync(cmd, cancellationToken).ConfigureAwait(false);

                if (Log != null && Log.Level <= LogLevel.Debug) WriteLog("<= {0}", rs as Command);

                var result = rs as Command;
                if (rs != null && span is DefaultSpan ds2 && ds2.TraceFlag > 0)
                {
                    span.AppendTag(Environment.NewLine);
                    span.AppendTag(rs);
                    span.AppendTag(result?.Payload?.ToStr());
                }

                return result;
            }
            else
            {
                var row = client.SendMessage(cmd);
                return new Command
                {
                    Reply = true,
                    Header = new Header() { Code = (Int32)ResponseCode.SUCCESS }
                };
            }
        }
        catch (Exception ex)
        {
            // 拉取消息超时，不记录错误日志
            if (code == RequestCode.PULL_MESSAGE && ex is TaskCanceledException)
                span?.AppendTag(ex.Message);
            else
                span?.SetError(ex, null);

            // 销毁，下次使用另一个地址
            if (ex is SocketException or IOException)
                client.TryDispose();

            throw;
        }
    }

    private void SetSignature(Command cmd)
    {
        // 阿里签名。阿里云ONS需要反射消息具体字段，把值转字符串后拼起来，再加上body后，取HmacSHA1
        // Apache RocketMQ ACL 签名机制跟阿里一致，需要排序然后再加上body后，取HmacSHA1

        String accessKey;
        String secretKey;
        String onsChannel;

        // 根据配置判断是阿里版本还是Apache开源版本
        var aliyun = Config.Aliyun;
        if (aliyun == null || aliyun.AccessKey.IsNullOrEmpty())
        {
            // Apache RocketMQ:如果未配置签名AccessKey信息直接返回，不加密
            var acl = Config.AclOptions;
            if (acl == null || acl.AccessKey.IsNullOrEmpty()) return;

            accessKey = acl.AccessKey;
            secretKey = acl.SecretKey;
            onsChannel = acl.OnsChannel;
        }
        else
        {
            // 阿里版本RocketMQ
            accessKey = aliyun.AccessKey;
            secretKey = aliyun.SecretKey;
            onsChannel = aliyun.OnsChannel;
        }

        var sha = new HMACSHA1(secretKey.GetBytes());
        var ms = new MemoryStream();

        // AccessKey + OnsChannel
        ms.Write(accessKey.GetBytes());
        ms.Write(onsChannel.GetBytes());

        // ExtFields
        var dic = cmd.Header.GetExtFields();
        //var extFieldsDic = dic.OrderBy(e => e.Key).ToDictionary(e => e.Key, e => e.Value);
        foreach (var extFields in dic)
        {
            if (extFields.Value != null) ms.Write(extFields.Value.GetBytes());
        }

        // Body
        cmd.Payload?.CopyTo(ms);

        var sign = sha.ComputeHash(ms.ToArray());
        dic["Signature"] = sign.ToBase64();
        dic["AccessKey"] = accessKey;
        dic["OnsChannel"] = onsChannel;
    }

    /// <summary>发送指定类型的命令</summary>
    /// <param name="request"></param>
    /// <param name="body"></param>
    /// <param name="extFields"></param>
    /// <param name="ignoreError"></param>
    /// <returns></returns>
    public virtual Command Invoke(RequestCode request, Object body, Object extFields = null, Boolean ignoreError = false)
    {
        var cmd = CreateCommand(request, body, extFields);

        // 避免UI死锁
        var rs = SendAsync(cmd, true).ConfigureAwait(false).GetAwaiter().GetResult();

        // 判断异常响应
        if (!ignoreError && rs.Header != null && rs.Header.Code != 0) throw rs.Header.CreateException();

        return rs;
    }

    /// <summary>发送指定类型的命令</summary>
    public virtual async Task<Command> InvokeAsync(RequestCode request, Object body, Object extFields = null,
        Boolean ignoreError = false, CancellationToken cancellationToken = default)
    {
        var cmd = CreateCommand(request, body, extFields);

        var rs = await SendAsync(cmd, true, cancellationToken).ConfigureAwait(false);

        // 判断异常响应
        if (!ignoreError && rs.Header != null && rs.Header.Code != 0)
        {
            throw rs.Header.CreateException();
        }

        return rs;
    }

    /// <summary>发送指定类型的命令</summary>
    /// <param name="request"></param>
    /// <param name="body"></param>
    /// <param name="extFields"></param>
    /// <returns></returns>
    public virtual Command InvokeOneway(RequestCode request, Object body, Object extFields = null)
    {
        var cmd = CreateCommand(request, body, extFields);
        cmd.OneWay = true;

        // 避免UI死锁
        var rs = Task.Run(() => SendAsync(cmd, false)).Result;

        return rs;
    }

    private Command CreateCommand(RequestCode request, Object body, Object extFields)
    {
        var header = new Header
        {
            Code = (Int32)request,
            SerializeTypeCurrentRPC = _serializeType + "",
            Remark = request + "",
        };

        var cmd = new Command
        {
            Header = header,
        };

        // 主体
        if (body is IPacket pk)
            cmd.Payload = pk;
        else if (body is Byte[] buf)
            cmd.Payload = (ArrayPacket)buf;
        else if (body != null)
            cmd.Payload = (ArrayPacket)Config.JsonHost.Write(body, false, false, false).GetBytes();

        if (extFields != null)
        {
            var dic = header.GetExtFields();
            foreach (var item in extFields.ToDictionary())
            {
                dic[item.Key] = item.Value + "";
            }
        }

        OnBuild(header);

        return cmd;
    }

    /// <summary>建立命令时，处理头部</summary>
    /// <param name="header"></param>
    protected virtual void OnBuild(Header header)
    {
        header.Language = "DOTNET";
    }
    #endregion

    #region 接收数据
    private void Client_Received(Object sender, ReceivedEventArgs e)
    {
        if (e.Message is not Command cmd) return;

        if (cmd.Reply)
        {
            //if (cmd.Header != null) _serializeType = cmd.Header.SerializeTypeCurrentRPC.ToEnum(SerializeType.JSON);

            return;
        }

        var rs = OnReceive(cmd);
        if (rs != null)
        {
            var ss = sender as ISocketRemote;
            ss.SendMessage(rs);
        }
    }

    /// <summary>收到命令时</summary>
    public event EventHandler<EventArgs<Command>> Received;

    /// <summary>收到命令</summary>
    /// <param name="cmd"></param>
    protected virtual Command OnReceive(Command cmd)
    {
        var code = !cmd.Reply ? (RequestCode)cmd.Header.Code + "" : (ResponseCode)cmd.Header.Code + "";
        WriteLog("收到：Code={0} {1}", code, cmd.Header.ToJson());

        using var span = Tracer?.NewSpan($"mq:{Name}:Receive:{code}", cmd);
        span?.AppendTag(cmd.Payload?.ToStr());
        try
        {
            if (Received == null) return null;

            var e = new EventArgs<Command>(cmd);
            Received.Invoke(this, e);

            return e.Arg;
        }
        catch (Exception ex)
        {
            span?.SetError(ex, null);

            throw;
        }
    }
    #endregion

    #region 日志
    /// <summary>日志</summary>
    public ILog Log { get; set; }

    /// <summary>写日志</summary>
    /// <param name="format"></param>
    /// <param name="args"></param>
    public void WriteLog(String format, params Object[] args) => Log?.Info($"[{Name}]{format}", args);
    #endregion
}