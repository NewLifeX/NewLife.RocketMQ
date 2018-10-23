using System;
using System.Collections.Generic;
using System.IO;
using NewLife.Collections;
using NewLife.Data;
using NewLife.Messaging;
using NewLife.Serialization;

namespace NewLife.RocketMQ.Protocol
{
    /// <summary>命令</summary>
    public class Command : IAccessor, IMessage
    {
        #region 属性
        /// <summary>头部</summary>
        public Header Header { get; set; }

        /// <summary>主体</summary>
        public Byte[] Body { get; set; }
        #endregion

        #region 扩展属性
        /// <summary>是否响应</summary>
        public Boolean Reply => Header == null ? false : ((Header.Flag & 1) == 1);

        Packet IMessage.Payload { get; set; }
        #endregion

        #region 读写
        /// <summary>从数据流中读取</summary>
        /// <param name="stream"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public Boolean Read(Stream stream, Object context = null)
        {
            var bn = new Binary
            {
                Stream = stream,
                IsLittleEndian = false,
            };

            try
            {
                var len = bn.Read<Int32>();
                if (len < 4 || len > 4 * 1024 * 1024) return false;

                // 读取头部
                var hlen = bn.Read<Int32>();
                if (hlen <= 0 || hlen > 8 * 1024) return false;

                var json = bn.ReadBytes(hlen).ToStr();
                Header = json.ToJsonEntity<Header>();

                //  读取主体
                if (len > 4 + hlen)
                {
                    Body = bn.ReadBytes(len - 4 - hlen);
                }
            }
            catch { return false; }

            return true;
        }

        /// <summary>读取Body作为Json返回</summary>
        /// <returns></returns>
        public IDictionary<String, Object> ReadBodyAsJson()
        {
            var buf = Body;
            if (buf == null || buf.Length == 0) return null;

            return new JsonParser(buf.ToStr()).Decode() as IDictionary<String, Object>;
        }

        /// <summary>写入命令到数据流</summary>
        /// <param name="stream"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public Boolean Write(Stream stream, Object context = null)
        {
            // 计算头部
            //var json = Header.ToJson();
            var json = JsonWriter.ToJson(Header, false, false, false);
            var hs = json.GetBytes();
            var buf = Body;

            // 计算长度
            var len = 4 + hs.Length;
            if (buf != null) len += buf.Length;

            // 写入总长度
            var bn = new Binary
            {
                Stream = stream,
                IsLittleEndian = false,
            };
            bn.Write(len);

            // 写入头部
            bn.Write(hs.Length);
            stream.Write(hs);

            // 写入主体
            if (buf != null && buf.Length > 0) stream.Write(buf);

            return true;
        }

        /// <summary>命令转字节数组</summary>
        /// <returns></returns>
        public Packet ToPacket()
        {
            var ms = new MemoryStream();
            Write(ms, null);
            ms.Position = 0;

            return new Packet(ms);
        }

        /// <summary>创建响应</summary>
        /// <returns></returns>
        public IMessage CreateReply()
        {
            if (Header == null || Reply || Header.Flag == 2) throw new Exception("不能创建响应命令");

            var head = new Header
            {
                Flag = 1,
                Opaque = Header.Opaque,
            };

            var cmd = new Command
            {
                Header = head,
            };

            return cmd;
        }

        Boolean IMessage.Read(Packet pk) => Read(pk.GetStream());
        #endregion

        #region 辅助
        /// <summary>友好字符串</summary>
        /// <returns></returns>
        public override String ToString()
        {
            var h = Header;
            if (h == null) return base.ToString();

            var sb = Pool.StringBuilder.Get();
            // 请求与响应
            if ((h.Flag & 1) == 1)
            {
                sb.Append("#");
                sb.Append((ResponseCode)h.Code);
            }
            else
            {
                sb.Append((RequestCode)h.Code);
            }
            sb.AppendFormat("({0})[{1}]", h.Opaque, Body == null ? 0 : Body.Length);

            return sb.Put(true);
        }
        #endregion
    }
}