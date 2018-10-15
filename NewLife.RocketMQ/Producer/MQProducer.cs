using System;
using NewLife.RocketMQ.Client;
using NewLife.RocketMQ.Protocol;

namespace NewLife.RocketMQ.Producer
{
    public class MQProducer : MQAdmin
    {
        #region 属性
        public Int32 DefaultTopicQueueNums { get; set; } = 4;

        public Int32 SendMsgTimeout { get; set; } = 3_000;

        public Int32 CompressMsgBodyOverHowmuch { get; set; } = 4096;

        public Int32 RetryTimesWhenSendFailed { get; set; } = 2;

        public Int32 RetryTimesWhenSendAsyncFailed { get; set; } = 2;

        public Boolean RetryAnotherBrokerWhenNotStoreOK { get; set; }

        public Int32 MaxMessageSize { get; set; } = 4 * 1024 * 1024;
        #endregion

        #region 基础方法
        //public override void Start()
        //{
        //    base.Start();
        //}

        public virtual void UnRegisterClient(String group)
        {
            var bk = GetBroker();

            if (group.IsNullOrEmpty()) group = "CLIENT_INNER_PRODUCER";

            var rs = bk.Send(RequestCode.UNREGISTER_CLIENT, new
            {
                ClientId,
                ProducerGroup = group,
            });
        }
        #endregion

        #region 发送消息
        private static readonly DateTime _dt1970 = new DateTime(1970, 1, 1);
        /// <summary>发送消息</summary>
        /// <param name="msg"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public virtual SendResult Send(Message msg, Int32 timeout = -1)
        {
            var ts = DateTime.Now - _dt1970;
            var smrh = new SendMessageRequestHeader
            {
                ProducerGroup = Group,
                Topic = msg.Topic.IsNullOrEmpty() ? Topic : msg.Topic,
                QueueId = 0,
                SysFlag = 0,
                BornTimestamp = (Int64)ts.TotalMilliseconds,
                Flag = msg.Flag,
                Properties = msg.GetProperties(),
                ReconsumeTimes = 0,
                UnitMode = UnitMode,
            };
            var dic = smrh.GetProperties();

            var bk = GetBroker();

            var rs = bk.Send(RequestCode.SEND_MESSAGE_V2, msg.Body, dic);

            var sr = new SendResult { Status = SendStatus.SendOK };
            sr.Read(rs.Header.ExtFields);

            return sr;
        }
        #endregion

        #region 连接池
        #endregion

        #region 业务方法
        public override void CreateTopic(String key, String newTopic, Int32 queueNum, Int32 topicSysFlag = 0)
        {
            throw new NotImplementedException();
        }

        public override Int64 SearchOffset(MessageQueue mq, Int64 timestamp)
        {
            throw new NotImplementedException();
        }

        public override Int64 MaxOffset(MessageQueue mq)
        {
            throw new NotImplementedException();
        }

        public override Int64 MinOffset(MessageQueue mq)
        {
            throw new NotImplementedException();
        }

        public override Int64 EarliestMsgStoreTime(MessageQueue mq)
        {
            throw new NotImplementedException();
        }

        public override QueryResult QueryMessage(String topic, String key, Int32 maxNum, Int64 begin, Int64 end)
        {
            throw new NotImplementedException();
        }

        public override MessageExt ViewMessage(String offsetMsgId)
        {
            throw new NotImplementedException();
        }

        public override MessageExt ViewMessage(String topic, String msgId)
        {
            throw new NotImplementedException();
        }
        #endregion
    }
}