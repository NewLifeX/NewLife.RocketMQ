using System;
using System.Collections.Generic;
using System.Linq;
using NewLife.Data;
using NewLife.Log;
using NewLife.Model;
using NewLife.Net.Handlers;

namespace NewLife.RocketMQ.Protocol
{
    /// <summary>编码器</summary>
    class MqCodec : MessageCodec<Command>
    {
        /// <summary>实例化编码器</summary>
        public MqCodec() => UserPacket = false;

        /// <summary>编码</summary>
        /// <param name="context"></param>
        /// <param name="msg"></param>
        /// <returns></returns>
        protected override Object Encode(IHandlerContext context, Command msg)
        {
            if (msg is Command cmd) return cmd.ToPacket();

            return null;
        }

        /// <summary>加入队列</summary>
        /// <param name="context"></param>
        /// <param name="msg"></param>
        /// <returns></returns>
        protected override void AddToQueue(IHandlerContext context, Command msg)
        {
            if (!msg.Reply) base.AddToQueue(context, msg);
        }

        /// <summary>解码</summary>
        /// <param name="context"></param>
        /// <param name="pk"></param>
        /// <returns></returns>
        protected override IList<Command> Decode(IHandlerContext context, Packet pk)
        {
            var ss = context.Owner as IExtend;
            var mcp = ss["CodecItem"] as CodecItem;
            if (mcp == null) ss["CodecItem"] = mcp = new CodecItem();

            var pks = Parse(pk, mcp, p => GetLength(p, 0, -4));
            var list = pks.Select(e =>
            {
                var msg = new Command();
                if (!msg.Read(e.GetStream())) return null;

                return msg;
            }).ToList();

            //XTrace.WriteLine("Decode {0}=>{1} CodecItem={2}/{3} {4}", pks.Count, list.Count, mcp.Stream?.Position, mcp.Stream?.Length, list.FirstOrDefault());

            return list;
        }

        /// <summary>是否匹配响应</summary>
        /// <param name="request"></param>
        /// <param name="response"></param>
        /// <returns></returns>
        protected override Boolean IsMatch(Object request, Object response)
        {
            return request is Command req && req.Header != null &&
                response is Command res && res.Header != null &&
                req.Header.Opaque == res.Header.Opaque;
        }
    }
}