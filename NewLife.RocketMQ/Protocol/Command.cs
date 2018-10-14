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

        /// <summary>响应数据。自动解码Json</summary>
        public IDictionary<String, Object> Data { get; private set; }
        #endregion

        #region 读写
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

                // 自动解码Json
                if (Header.SerializeTypeCurrentRPC.EqualIgnoreCase("json"))
                    Data = new JsonParser(Body.ToStr()).Decode() as IDictionary<String, Object>;
            }

            return true;
        }

        public Boolean Write(Stream stream, Object context = null)
        {
            // 计算头部
            var json = Header.ToJson();
            //var js = new JsonWriter { LowerCaseName = true };
            //var json = js.ToJson(Header, false);
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