using System;
using System.Collections.Generic;
using System.IO;
using NewLife.Serialization;

namespace NewLife.RocketMQ.Protocol
{
    /// <summary>命令</summary>
    public class Command : IAccessor
    {
        #region 属性
        /// <summary>头部</summary>
        public Header Header { get; set; }

        /// <summary>主体</summary>
        public Byte[] Body { get; set; }
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
            var len = bn.Read<Int32>();

            // 读取头部
            var hlen = bn.Read<Int32>();
            var json = bn.ReadBytes(hlen).ToStr();
            Header = json.ToJsonEntity<Header>();

            //  读取主体
            if (len > 4 + hlen)
            {
                Body = bn.ReadBytes(len - 4 - hlen);
            }

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
            var json = Header.ToJson();
            //var json = JsonWriter.ToJson(Header, false, false, true);
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
        #endregion
    }
}