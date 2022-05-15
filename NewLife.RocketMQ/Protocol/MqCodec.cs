using NewLife.Data;
using NewLife.Messaging;
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
            if (ss["Codec"] is not PacketCodec pc) 
                ss["Codec"] = pc = new PacketCodec { GetLength = p => GetLength(p, 0, -4) };

            var pks = pc.Parse(pk);
            var list = pks.Select(e =>
            {
                var msg = new Command();
                if (!msg.Read(e.GetStream())) return null;

                return msg;
            }).ToList();

            return list;
        }

        /// <summary>连接关闭时，清空粘包编码器</summary>
        /// <param name="context"></param>
        /// <param name="reason"></param>
        /// <returns></returns>
        public override Boolean Close(IHandlerContext context, String reason)
        {
            if (context.Owner is IExtend ss) ss["Codec"] = null;

            return base.Close(context, reason);
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