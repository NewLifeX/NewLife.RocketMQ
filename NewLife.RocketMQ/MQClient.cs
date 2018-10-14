using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using NewLife.Net;
using NewLife.RocketMQ.Client;
using NewLife.RocketMQ.Protocol;

namespace NewLife.RocketMQ
{
    public abstract class MQClient : DisposeBase
    {
        #region 属性
        /// <summary>超时。默认3000ms</summary>
        public Int32 Timeout { get; set; } = 3_000;

        public MQAdmin Config { get; set; }

        private TcpClient _Client;
        private Stream _Stream;
        #endregion

        #region 构造
        protected override void OnDispose(Boolean disposing)
        {
            base.OnDispose(disposing);

            _Client.TryDispose();
        }
        #endregion

        #region 方法
        /// <summary>开始</summary>
        public virtual void Start()
        {
            var uri = GetServer();

            var client = new TcpClient();

            var timeout = Timeout;
            // 采用异步来解决连接超时设置问题
            var ar = client.BeginConnect(uri.Address, uri.Port, null, null);
            if (!ar.AsyncWaitHandle.WaitOne(timeout, false))
            {
                client.Close();
                throw new TimeoutException($"连接[{uri}][{timeout}ms]超时！");
            }

            _Client = client;

            _Stream = new BufferedStream(client.GetStream());
        }

        protected abstract NetUri GetServer();

        private Int32 g_id;
        /// <summary>发送命令</summary>
        /// <param name="cmd"></param>
        /// <returns></returns>
        public Command Send(Command cmd)
        {
            if (cmd.Header.Opaque == 0) cmd.Header.Opaque = g_id++;

            cmd.Write(_Stream);
            //var ms = new MemoryStream();
            //cmd.Write(ms);
            //XTrace.WriteLine(ms.ToArray().ToHex());

            var rs = new Command();
            rs.Read(_Stream);

            return rs;
        }

        /// <summary>发送指定类型的命令</summary>
        /// <param name="request"></param>
        /// <param name="extFields"></param>
        /// <returns></returns>
        public Command Send(RequestCode request, Object extFields = null)
        {
            var header = new Header
            {
                Code = (Int32)request,
            };

            var cmd = new Command
            {
                Header = header,
            };

            if (extFields != null)
            {
                header.ExtFields.Merge(extFields);// = extFields.ToDictionary().ToDictionary(e => e.Key, e => e.Value + "");
            }

            OnBuild(header);

            var rs = Send(cmd);

            // 判断异常响应
            if (rs.Header.Code != 0) throw new ResponseException((ResponseCode)rs.Header.Code, rs.Header.Remark);

            return rs;
        }

        /// <summary>建立命令时，处理头部</summary>
        /// <param name="header"></param>
        protected virtual void OnBuild(Header header)
        {
            // 阿里云支持 CSharp
            var cfg = Config;
            if (!cfg.AccessKey.IsNullOrEmpty()) header.Language = "CSharp";

            // 阿里云密钥
            if (!cfg.AccessKey.IsNullOrEmpty())
            {
                var dic = header.ExtFields;

                dic["AccessKey"] = cfg.AccessKey;
                dic["SecretKey"] = cfg.SecretKey;

                if (!cfg.OnsChannel.IsNullOrEmpty()) dic["OnsChannel"] = cfg.OnsChannel;
            }
        }
        #endregion
    }
}