using System;
using NewLife.RocketMQ.Client;
using NewLife.RocketMQ.Protocol;
using NewLife.Serialization;

namespace NewLife.RocketMQ
{
    /// <summary>生产者</summary>
    public class Producer : MqBase
    {
        #region 属性
        //public Int32 DefaultTopicQueueNums { get; set; } = 4;

        //public Int32 SendMsgTimeout { get; set; } = 3_000;

        //public Int32 CompressMsgBodyOverHowmuch { get; set; } = 4096;

        //public Int32 RetryTimesWhenSendFailed { get; set; } = 2;

        //public Int32 RetryTimesWhenSendAsyncFailed { get; set; } = 2;

        //public Boolean RetryAnotherBrokerWhenNotStoreOK { get; set; }

        //public Int32 MaxMessageSize { get; set; } = 4 * 1024 * 1024;
        #endregion

        #region 基础方法
        //public override void Start()
        //{
        //    base.Start();
        //}

        #endregion

        #region 发送消息
        private static readonly DateTime _dt1970 = new DateTime(1970, 1, 1);
        /// <summary>发送消息</summary>
        /// <param name="msg"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public virtual SendResult Send(Message msg, Int32 timeout = -1)
        {
            // 选择队列分片
            var mq = _NameServer.SelectQueue();
            mq.Topic = Topic;

            // 构造请求头
            var ts = DateTime.Now - _dt1970;
            var smrh = new SendMessageRequestHeader
            {
                ProducerGroup = Group,
                Topic = Topic,
                QueueId = mq.QueueId,
                SysFlag = 0,
                BornTimestamp = (Int64)ts.TotalMilliseconds,
                Flag = msg.Flag,
                Properties = msg.GetProperties(),
                ReconsumeTimes = 0,
                UnitMode = UnitMode,
            };

            // 根据队列获取Broker客户端
            var bk = GetBroker(mq.BrokerName);
            var rs = bk.Invoke(RequestCode.SEND_MESSAGE_V2, msg.Body, smrh.GetProperties());

            // 包装结果
            var sr = new SendResult
            {
                Status = SendStatus.SendOK,
                Queue = mq
            };
            sr.Read(rs.Header.ExtFields);

            return sr;
        }

        /// <summary>发布消息</summary>
        /// <param name="body"></param>
        /// <param name="tags"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public virtual SendResult Send(Object body, String tags = null, Int32 timeout = -1)
        {
            if (!(body is Byte[] buf))
            {
                if (!(body is String str)) str = body.ToJson();

                buf = str.GetBytes();
            }

            return Send(new Message { Body = buf, Tags = tags }, timeout);
        }
        #endregion

        #region 连接池
        #endregion

        #region 业务方法
        /// <summary>创建主题</summary>
        /// <param name="key"></param>
        /// <param name="newTopic"></param>
        /// <param name="queueNum"></param>
        /// <param name="topicSysFlag"></param>
        public void CreateTopic(String key, String newTopic, Int32 queueNum, Int32 topicSysFlag = 0)
        {
            throw new NotImplementedException();
        }
        #endregion
    }
}