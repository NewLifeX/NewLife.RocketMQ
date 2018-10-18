using System;
using System.Collections.Generic;
using System.Linq;
using NewLife.Log;
using NewLife.RocketMQ.Client;
using NewLife.RocketMQ.Common;
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
        /// <summary>启动</summary>
        /// <returns></returns>
        public override Boolean Start()
        {
            if (!base.Start()) return false;

            if (_NameServer != null)
            {
                _NameServer.OnBrokerChange += (s, e) =>
                {
                    _brokers = null;
                    _robin = null;
                };
            }

            return true;
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
            // 选择队列分片
            var mq = SelectQueue();
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

        #region 业务方法
        /// <summary>创建主题</summary>
        /// <param name="topic"></param>
        /// <param name="queueNum"></param>
        /// <param name="topicSysFlag"></param>
        public void CreateTopic(String topic, Int32 queueNum, Int32 topicSysFlag = 0)
        {
            var header = new
            {
                topic,
                defaultTopic = Topic,
                readQueueNums = queueNum,
                writeQueueNums = queueNum,
                perm = 7,
                topicFilterType = "SINGLE_TAG",
                topicSysFlag,
                order = false,
            };

            // 在所有Broker上创建Topic
            foreach (var item in Brokers)
            {
                WriteLog("在Broker[{0}]上创建主题：{1}", item.Name, topic);
                try
                {
                    var bk = GetBroker(item.Name);
                    var rs = bk.Invoke(RequestCode.UPDATE_AND_CREATE_TOPIC, null, header);
                }
                catch (Exception ex)
                {
                    XTrace.WriteException(ex);
                }
            }
        }
        #endregion

        #region 选择Broker队列
        private IList<BrokerInfo> _brokers;
        private WeightRoundRobin _robin;
        /// <summary>选择队列</summary>
        /// <returns></returns>
        public MessageQueue SelectQueue()
        {
            if (_robin == null)
            {
                var list = Brokers.Where(e => e.Permission.HasFlag(Permissions.Write) && e.WriteQueueNums > 0).ToList();
                if (list.Count == 0) return null;

                var total = list.Sum(e => e.WriteQueueNums);
                if (total <= 0) return null;

                _brokers = list;
                _robin = new WeightRoundRobin(list.Select(e => e.WriteQueueNums).ToArray());
            }

            // 构造排序列表。希望能够均摊到各Broker
            var idx = _robin.Get(out var times);
            var bk = _brokers[idx];
            return new MessageQueue { BrokerName = bk.Name, QueueId = (times - 1) % bk.WriteQueueNums };
        }
        #endregion
    }
}