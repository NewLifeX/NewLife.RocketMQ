using System;
using System.Collections.Generic;
using NewLife.RocketMQ.Protocol;

namespace NewLife.RocketMQ.MessageTrace
{
    /// <summary>
    /// 
    /// </summary>
    public interface ISendMessageHook
    {

        /// <summary>
        /// 
        /// </summary>
        /// <param name="context"></param>
        void ExecuteHookBefore(SendMessageContext context);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="context"></param>
        void ExecuteHookAfter(SendMessageContext context);
    }

    /// <summary>
    /// 
    /// </summary>
    public interface IConsumeMessageHook
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="context"></param>
        void ExecuteHookBefore(ConsumeMessageContext context);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="context"></param>
        void ExecuteHookAfter(ConsumeMessageContext context);
    }

    /// <summary>
    /// 
    /// </summary>
    public class SendMessageContext
    {
        /// <summary>
        /// 
        /// </summary>
        public String ProducerGroup;

        /// <summary>
        /// 
        /// </summary>
        public Message Message;

        /// <summary>
        /// 
        /// </summary>
        public MessageQueue Mq;

        /// <summary>
        /// 
        /// </summary>
        public String BrokerAddr;

        /// <summary>
        /// 
        /// </summary>
        public SendResult SendResult;

        /// <summary>
        /// 
        /// </summary>
        public Exception E;

        /// <summary>
        /// 
        /// </summary>
        public Object MqTraceContext;

        /// <summary>
        /// 
        /// </summary>
        public IDictionary<String, String> Props;

        /// <summary>
        /// 
        /// </summary>
        public TraceContext TraceContext;

        /// <summary>
        /// 
        /// </summary>
        public String MsgType;

        /// <summary>
        /// 
        /// </summary>
        public DateTime BornHost;
    }

    /// <summary>
    /// 
    /// </summary>
    public class ConsumeMessageContext
    {
        /// <summary>
        /// 
        /// </summary>
        public String ConsumerGroup;

        /// <summary>

        public List<MessageExt> MsgList;

        /// <summary>
        /// 
        /// </summary>
        public MessageQueue Mq;

        /// <summary>
        /// 
        /// </summary>
        public Boolean Success;

        /// <summary>
        /// 
        /// </summary>
        public IDictionary<String, String> Props;

        /// <summary>
        /// 
        /// </summary>
        public TraceContext TraceContext;

        /// <summary>
        /// 
        /// </summary>
        public String MsgType;

        /// <summary>
        /// 
        /// </summary>
        public DateTime BornHost;
    }

    /// <summary>
    /// 轨迹类型
    /// </summary>
    public enum TraceType
    {
        Pub,
        SubBefore,
        SubAfter,
    }

    /// <summary>
    /// 轨迹上下文
    /// </summary>
    public class TraceContext
    {
        /// <summary>轨迹类型</summary>
        public TraceType TraceType { get; set; }

        /// <summary>时间戳</summary>
        public DateTime TimeStamp { get; set; }

        /// <summary>区域ID</summary>
        public String RegionId { get; set; }

        /// <summary>分组名</summary>
        public String GroupName { get; set; }

        /// <summary>耗时</summary>
        public int CostTime { get; set; }

        /// <summary>是否成功</summary>
        public bool Success { get; set; }

        /// <summary>请求ID，例如消息ID</summary>
        public String RequestId { get; set; }

        /// <summary>轨迹豆</summary>
        public IList<TraceBean> TraceBeans { get; set; } = new List<TraceBean>();
    }

    /// <summary>
    /// 轨迹豆，包含轨迹上下文信息
    /// </summary>
    public class TraceBean
    {
        /// <summary>主题</summary>
        public String Topic { get; set; }

        /// <summary>消息ID</summary>
        public String MsgId { get; set; }

        /// <summary>偏移消息ID</summary>
        public String OffsetMsgId { get; set; }

        /// <summary>标签</summary>
        public String Tags { get; set; }

        /// <summary>键</summary>
        public String Keys { get; set; }

        /// <summary>存储主机</summary>
        public String StoreHost { get; set; }

        /// <summary>消息体长度</summary>
        public int BodyLength { get; set; }

        /// <summary>客户端主机</summary>
        public String ClientHost { get; set; }

        /// <summary>消息类型</summary>
        public String MsgType { get; set; }

        /// <summary>存储时间</summary>
        public long StoreTime { get; set; }
    }
}
