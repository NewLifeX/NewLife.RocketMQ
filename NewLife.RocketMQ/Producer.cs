using System;
using System.Collections.Concurrent;
using System.Linq;
using NewLife.RocketMQ.Client;
using NewLife.RocketMQ.Protocol;

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

        /// <summary>注销客户端</summary>
        /// <param name="group"></param>
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

            var mq = SelectQueue(smrh.Topic);
            if (mq != null) smrh.QueueId = mq.QueueId;

            var dic = smrh.GetProperties();

            var bk = GetBroker();

            var rs = bk.Send(RequestCode.SEND_MESSAGE_V2, msg.Body, dic);

            var sr = new SendResult
            {
                Status = SendStatus.SendOK,
                Queue = mq
            };
            sr.Read(rs.Header.ExtFields);

            return sr;
        }

        private readonly ConcurrentDictionary<String, Int32> _qs = new ConcurrentDictionary<String, Int32>();
        /// <summary>根据topic选择队列</summary>
        /// <param name="topic"></param>
        /// <returns></returns>
        private MessageQueue SelectQueue(String topic)
        {
            var list = Queues.Where(e => e.Topic.EqualIgnoreCase(topic)).ToList();
            if (list.Count == 0) return null;

            // 轮询使用
            var idx = _qs.GetOrAdd(topic, -1);
            var old = idx++;
            if (idx >= list.Count) idx = 0;
            _qs.TryUpdate(topic, idx, old);

            return list[idx];
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