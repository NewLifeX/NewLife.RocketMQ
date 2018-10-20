using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Threading;
using NewLife.Collections;
using NewLife.Log;
using NewLife.Net;
using NewLife.RocketMQ.Client;
using NewLife.RocketMQ.Protocol;
using NewLife.Serialization;

namespace NewLife.RocketMQ
{
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
        #endregion

        #region 构造
        /// <summary>实例化</summary>
        public ClusterClient()
        {
            _Pool = new MyPool { Client = this };
        }

        /// <summary>销毁</summary>
        /// <param name="disposing"></param>
        protected override void OnDispose(Boolean disposing)
        {
            base.OnDispose(disposing);

            _Pool.TryDispose();
            //_Client.TryDispose();
        }
        #endregion

        #region 方法
        /// <summary>开始</summary>
        public virtual void Start()
        {
            WriteLog("[{0}]集群地址：{1}", Name, Servers.Join(";", e => $"{e.Host}:{e.Port}"));

            //ReceiveAsync();
        }

        private Int32 g_id;
        /// <summary>发送命令</summary>
        /// <param name="cmd"></param>
        /// <returns></returns>
        protected virtual Command Send(Command cmd)
        {
            if (cmd.Header.Opaque == 0) cmd.Header.Opaque = g_id++;

            // 签名
            SetSignature(cmd);

            // 轮询调用
            Exception last = null;
            var times = Servers.Length;
            for (var i = 0; i < times; i++)
            {
                var client = _Pool.Get();
                try
                {
                    //var client = GetClient();
                    //lock (client)
                    //{
                    var ns = client.GetStream();

                    // 其它指令
                    while (ns.DataAvailable)
                    {
                        var cmd2 = new Command();
                        cmd2.Read(ns);
                        if (cmd2.Header == null) break;

                        var rs = OnReceive(cmd2);
                        if (rs != null) rs.Write(ns);
                    }
                    // 清空残留
                    while (ns.DataAvailable) ns.ReadBytes(1024);

                    var ms = new BufferedStream(ns);
                    cmd.Write(ms);
                    ms.Flush();

                    while (true)
                    {
                        var rs = new Command();
                        rs.Read(ms);
                        if (rs.Header == null) return rs;

                        // 当前请求的响应
                        if ((rs.Header.Flag & 1) == 1 && rs.Header.Opaque == cmd.Header.Opaque) return rs;

                        // 其它指令
                        rs = OnReceive(rs);
                        if (rs != null) rs.Write(ns);
                    }
                    //}
                }
                catch (Exception ex)
                {
                    //if (ex is SocketException) _Client = null;

                    last = ex;
                    Thread.Sleep(100);
                }
                finally
                {
                    _Pool.Put(client);
                }
            }

            throw last;
        }

        private void SetSignature(Command cmd)
        {
            // 签名。阿里云ONS需要反射消息具体字段，把值转字符串后拼起来，再加上body后，取HmacSHA1
            var cfg = Config;
            if (cfg.AccessKey.IsNullOrEmpty()) return;

            var sha = new HMACSHA1(cfg.SecretKey.GetBytes());

            var ms = new MemoryStream();
            // AccessKey + OnsChannel
            ms.Write(cfg.AccessKey.GetBytes());
            ms.Write(cfg.OnsChannel.GetBytes());
            // ExtFields
            var dic = cmd.Header.GetExtFields();

            foreach (var item in dic)
            {
                if (item.Value != null) ms.Write(item.Value.GetBytes());
            }
            // Body
            if (cmd.Body != null && cmd.Body.Length > 0) ms.Write(cmd.Body);

            var sign = sha.ComputeHash(ms.ToArray());

            dic["Signature"] = sign.ToBase64();
            dic["AccessKey"] = cfg.AccessKey;
            dic["OnsChannel"] = cfg.OnsChannel;
        }

        /// <summary>发送指定类型的命令</summary>
        /// <param name="request"></param>
        /// <param name="body"></param>
        /// <param name="extFields"></param>
        /// <param name="ignoreError"></param>
        /// <returns></returns>
        internal virtual Command Invoke(RequestCode request, Object body, Object extFields = null, Boolean ignoreError = false)
        {
#if DEBUG
            WriteLog("Invoke: {0}", request);
#endif

            var header = new Header
            {
                Code = (Int32)request,
            };

            var cmd = new Command
            {
                Header = header,
            };

            // 主体
            if (body is Byte[] buf)
                cmd.Body = buf;
            else if (body != null)
                cmd.Body = JsonWriter.ToJson(body, false, false, false).GetBytes();

            if (extFields != null)
            {
                var dic = header.GetExtFields();
                foreach (var item in extFields.ToDictionary())
                {
                    dic[item.Key] = item.Value + "";
                }
            }

            OnBuild(header);

            var rs = Send(cmd);

            // 判断异常响应
            if (!ignoreError && rs.Header != null && rs.Header.Code != 0)
            {
                // 优化异常输出
                var err = rs.Header.Remark;
                if (!err.IsNullOrEmpty())
                {
                    var p = err.IndexOf("Exception: ");
                    if (p >= 0) err = err.Substring(p + "Exception: ".Length);
                    p = err.IndexOf(", ");
                    if (p > 0) err = err.Substring(0, p);
                }

                throw new ResponseException(rs.Header.Code, err);
            }

            return rs;
        }

        /// <summary>建立命令时，处理头部</summary>
        /// <param name="header"></param>
        protected virtual void OnBuild(Header header)
        {
            // 阿里云支持 CSharp
            var cfg = Config;
            if (!cfg.AccessKey.IsNullOrEmpty()) header.Language = "CSharp";

            //// 阿里云密钥
            //if (!cfg.AccessKey.IsNullOrEmpty())
            //{
            //    var dic = header.ExtFields;

            //    dic["AccessKey"] = cfg.AccessKey;
            //    dic["SecretKey"] = cfg.SecretKey;

            //    if (!cfg.OnsChannel.IsNullOrEmpty()) dic["OnsChannel"] = cfg.OnsChannel;
            //}
        }
        #endregion

        #region 接收数据
        //private TimerX _timer;

        ///// <summary>开启异步接收</summary>
        //protected void ReceiveAsync()
        //{
        //    if (_timer == null) _timer = new TimerX(CheckReceive, null, 1000, 1000, "ClusterClient");
        //}

        //private void CheckReceive(Object state = null)
        //{
        //    var client = _Client;
        //    if (client == null) return;

        //    lock (client)
        //    {
        //        var ns = client.GetStream();
        //        if (ns == null) return;

        //        // 循环处理收到的命令
        //        while (ns.DataAvailable)
        //        {
        //            var cmd = new Command();
        //            if (cmd.Read(ns) && cmd.Header != null) ThreadPool.QueueUserWorkItem(s => OnReceive(cmd));
        //        }
        //    }
        //}

        /// <summary>收到命令时</summary>
        public event EventHandler<EventArgs<Command>> Received;

        /// <summary>收到命令</summary>
        /// <param name="cmd"></param>
        protected virtual Command OnReceive(Command cmd)
        {
#if DEBUG
            var code = (cmd.Header.Flag & 1) == 0 ? (RequestCode)cmd.Header.Code + "" : (ResponseCode)cmd.Header.Code + "";

            WriteLog("收到：Code={0} {1}", code, cmd.Header.ToJson());
#endif

            if (Received == null) return null;

            var e = new EventArgs<Command>(cmd);
            Received.Invoke(this, e);

            return e.Arg;
        }
        #endregion

        #region 连接池
        private readonly MyPool _Pool;
        //private TcpClient _Client;

        class MyPool : ObjectPool<TcpClient>
        {
            public ClusterClient Client { get; set; }

            public MyPool()
            {
                // 最小两个连接，一个长连接拉数据，另一个传输心跳等命令
                Min = 2;
                IdleTime = 40;
                AllIdleTime = 180;
            }

            protected override TcpClient OnCreate() => Client.OnCreate();

            protected override Boolean OnPut(TcpClient value) => value.Connected;
        }

        ///// <summary>获取主连接</summary>
        ///// <returns></returns>
        //protected virtual TcpClient GetMaster()
        //{
        //    if (_Client != null && _Client.Connected) return _Client;

        //    return _Client = OnCreate();
        //}

        private Int32 _ServerIndex;
        /// <summary>创建网络连接。轮询使用地址</summary>
        /// <returns></returns>
        protected virtual TcpClient OnCreate()
        {
            var idx = Interlocked.Increment(ref _ServerIndex);
            idx = (idx - 1) % Servers.Length;

            var uri = Servers[idx];
            WriteLog("正在连接[{0}:{1}]", uri.Host, uri.Port);

            var client = new TcpClient();

            var timeout = Timeout;

            // 采用异步来解决连接超时设置问题
            var ar = client.BeginConnect(uri.Address, uri.Port, null, null);
            if (!ar.AsyncWaitHandle.WaitOne(timeout, false))
            {
                client.Close();
                throw new TimeoutException($"连接[{uri}][{timeout}ms]超时！");
            }

            return client;
        }
        #endregion

        #region 日志
        /// <summary>日志</summary>
        public ILog Log { get; set; } = Logger.Null;

        /// <summary>写日志</summary>
        /// <param name="format"></param>
        /// <param name="args"></param>
        public void WriteLog(String format, params Object[] args) => Log?.Info($"[{Name}]{format}", args);
        #endregion
    }
}