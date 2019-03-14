using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
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
        protected override void OnDispose(Boolean disposing)
        {
            base.OnDispose(disposing);

            //_Pool.TryDispose();
            _Client.TryDispose();
        }
        #endregion

        #region 方法
        /// <summary>开始</summary>
        public virtual void Start()
        {
            WriteLog("集群地址：{0}", Servers.Join(";", e => $"{e.Host}:{e.Port}"));

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
                    client.Log = Log;
                    client.Timeout = Timeout;
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

                if (_Client == null) throw new XException("[{0}]集群所有地址连接失败！", Name);
            }
        }

        private Int32 g_id;
        /// <summary>发送命令</summary>
        /// <param name="cmd"></param>
        /// <returns></returns>
        protected virtual async Task<Command> SendAsync(Command cmd)
        {
            if (cmd.Header.Opaque == 0) cmd.Header.Opaque = Interlocked.Increment(ref g_id);

            // 签名
            SetSignature(cmd);

#if DEBUG
            WriteLog("=> {0}", cmd);
#endif

            EnsureCreate();
            var client = _Client;
            try
            {
                var rs = await client.SendMessageAsync(cmd);

#if DEBUG
            WriteLog("<= {0}", rs as Command);
#endif

                return rs as Command;
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
            //var dic2 = dic.OrderBy(e => e.Key).ToDictionary(e => e.Key, e => e.Value);
            foreach (var item in dic)
            {
                if (item.Value != null) ms.Write(item.Value.GetBytes());
            }
            // Body
            cmd.Payload?.CopyTo(ms);

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
            if (body is Byte[] buf)
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

            //var rs = TaskEx.Run(() => SendAsync(cmd)).Result;
            var rs = SendAsync(cmd).Result;

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
        private void Client_Received(Object sender, ReceivedEventArgs e)
        {
            var cmd = e.Message as Command;
            if (cmd == null || cmd.Reply) return;

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