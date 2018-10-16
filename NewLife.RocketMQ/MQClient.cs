using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using NewLife.Net;
using NewLife.RocketMQ.Client;
using NewLife.RocketMQ.Protocol;
using NewLife.Serialization;

namespace NewLife.RocketMQ
{
    /// <summary>客户端</summary>
    public abstract class MQClient : DisposeBase
    {
        #region 属性
        /// <summary>编号</summary>
        public String Id { get; set; }

        /// <summary>超时。默认3000ms</summary>
        public Int32 Timeout { get; set; } = 3_000;

        /// <summary>配置</summary>
        public MqBase Config { get; set; }

        private TcpClient _Client;
        private Stream _Stream;
        #endregion

        #region 构造
        /// <summary>销毁</summary>
        /// <param name="disposing"></param>
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

        /// <summary>获取服务器地址</summary>
        /// <returns></returns>
        protected abstract NetUri GetServer();

        private Int32 g_id;
        /// <summary>发送命令</summary>
        /// <param name="cmd"></param>
        /// <returns></returns>
        public Command Send(Command cmd)
        {
            if (cmd.Header.Opaque == 0) cmd.Header.Opaque = g_id++;

            //// 签名。阿里云ONS需要反射消息具体字段，把值转字符串后拼起来，再加上body后，取HmacSHA1
            //var cfg = Config;
            //if (!cfg.AccessKey.IsNullOrEmpty())
            //{
            //    var sha = new HMACSHA1(cfg.SecretKey.GetBytes());

            //    var ms = new MemoryStream();
            //    cmd.Write(ms);
            //    if (cmd.Body != null && cmd.Body.Length > 0) ms.Write(cmd.Body);

            //    var sign = sha.ComputeHash(ms.ToArray());

            //    var dic = cmd.Header.ExtFields;

            //    dic["Signature"] = sign.ToBase64();
            //}

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
        /// <param name="body"></param>
        /// <param name="extFields"></param>
        /// <returns></returns>
        internal Command Send(RequestCode request, Object body, Object extFields = null)
        {
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
                cmd.Body = body.ToJson().GetBytes();

            if (extFields != null)
            {
                //header.ExtFields.Merge(extFields);// = extFields.ToDictionary().ToDictionary(e => e.Key, e => e.Value + "");
                var dic = header.ExtFields;
                foreach (var item in extFields.ToDictionary())
                {
                    dic[item.Key] = item.Value + "";
                }
            }

            OnBuild(header);

            var rs = Send(cmd);

            // 判断异常响应
            if (rs.Header.Code != 0) throw new ResponseException(rs.Header.Code, rs.Header.Remark);

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