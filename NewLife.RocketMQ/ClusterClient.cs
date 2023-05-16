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
    public virtual void Start()
    {
        WriteLog("集群地址：{0}", Servers.Join(";"));

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
                client.Add(new MqCodec { Timeout = 59_000 });

                // 关闭Tcp延迟以合并小包的算法，降低延迟
                if (client is TcpSession tcp) tcp.NoDelay = true;

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
    /// <returns></returns>
    protected virtual async Task<Command> SendAsync(Command cmd, Boolean waitResult)
    {
        if (cmd.Header.Opaque == 0) cmd.Header.Opaque = Interlocked.Increment(ref g_id);

        WriteLog("=> {0}", cmd);

        // 签名
        SetSignature(cmd);

        EnsureCreate();
        var client = _Client;
        try
        {
            if (waitResult)
            {
                var rs = await client.SendMessageAsync(cmd);

                WriteLog("<= {0}", rs as Command);

                return rs as Command;
            }
            else
            {
                var row = client.SendMessage(cmd);
                return new Command
                {
                    Header = new Header() { Code = row }
                };
            }
        }
        catch
        {
            // 销毁，下次使用另一个地址
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
    internal virtual Command Invoke(RequestCode request, Object body, Object extFields = null, Boolean ignoreError = false)
    {
        var cmd = CreateCommand(request, body, extFields);

        // 避免UI死锁
        var rs = Task.Run(() => SendAsync(cmd, true)).Result;

        // 判断异常响应
        if (!ignoreError && rs.Header != null && rs.Header.Code != 0) throw rs.Header.CreateException();

        return rs;
    }

    /// <summary>发送指定类型的命令</summary>
    internal virtual async Task<Command> InvokeAsync(RequestCode request, Object body, Object extFields = null,
        Boolean ignoreError = false)
    {
        var cmd = CreateCommand(request, body, extFields);

        var rs = await SendAsync(cmd, true);

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
    internal virtual Command InvokeOneway(RequestCode request, Object body, Object extFields = null)
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
            Remark = request + "",
        };

        var cmd = new Command
        {
            Header = header,
        };

        // 主体
        if (body is Packet pk)
            cmd.Payload = pk;
        else if (body is Byte[] buf)
            cmd.Payload = buf;
        else if (body != null)
            cmd.Payload = JsonWriter.ToJson(body, false, false, false).GetBytes();

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
        if (e.Message is not Command cmd || cmd.Reply) return;

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
        var code = (cmd.Header.Flag & 1) == 0 ? (RequestCode)cmd.Header.Code + "" : (ResponseCode)cmd.Header.Code + "";

        WriteLog("收到：Code={0} {1}", code, cmd.Header.ToJson());

        if (Received == null) return null;

        var e = new EventArgs<Command>(cmd);
        Received.Invoke(this, e);

        return e.Arg;
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