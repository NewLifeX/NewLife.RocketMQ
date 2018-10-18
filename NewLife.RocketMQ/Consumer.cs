using System;
using System.Collections.Generic;
using NewLife.Log;
using NewLife.RocketMQ.Client;
using NewLife.RocketMQ.Protocol;

namespace NewLife.RocketMQ
{
    /// <summary>消费者</summary>
    public class Consumer : MqBase
    {
        #region 拉取消息
        /// <summary>从指定队列拉取消息</summary>
        /// <param name="mq"></param>
        /// <param name="offset"></param>
        /// <param name="maxNums"></param>
        /// <returns></returns>
        public PullResult Pull(MessageQueue mq, Int64 offset, Int32 maxNums)
        {
            var header = new PullMessageRequestHeader
            {
                ConsumerGroup = Group,
                Topic = Topic,
                QueueId = mq.QueueId,
                QueueOffset = offset,
                MaxMsgNums = maxNums,
                SysFlag = 6,
            };

            var dic = header.GetProperties();
            var bk = GetBroker(mq.BrokerName);

            var rs = bk.Invoke(RequestCode.PULL_MESSAGE, null, dic);

            var pr = new PullResult
            {
                Status = PullStatus.Found,
            };
            pr.Read(rs.Header.ExtFields);

            // 读取内容
            if (rs.Body != null && rs.Body.Length > 0) pr.Messages = MessageExt.ReadAll(rs.Body).ToArray();

            return pr;
        }
        #endregion

        #region 业务方法
        /// <summary>查询指定队列的偏移量</summary>
        /// <param name="mq"></param>
        /// <returns></returns>
        public Int64 QueryOffset(MessageQueue mq)
        {
            var bk = GetBroker(mq.BrokerName);
            var rs = bk.Invoke(RequestCode.QUERY_CONSUMER_OFFSET, null, new
            {
                consumerGroup = Group,
                topic = Topic,
                queueId = mq.QueueId,
            });

            return rs.Header.ExtFields["offset"].ToLong();
        }

        /// <summary>根据时间戳查询偏移</summary>
        /// <param name="mq"></param>
        /// <param name="timestamp"></param>
        /// <returns></returns>
        public Int64 SearchOffset(MessageQueue mq, Int64 timestamp)
        {
            throw new NotImplementedException();
        }

        /// <summary>获取消费者下所有消费者</summary>
        /// <param name="group"></param>
        public void GetConsumers(String group = null)
        {
            if (group.IsNullOrEmpty()) group = Group;

            var header = new
            {
                consumerGroup = group,
            };

            // 在所有Broker上查询
            foreach (var item in Brokers)
            {
                try
                {
                    var bk = GetBroker(item.Name);
                    bk.Ping();
                    var rs = bk.Invoke(RequestCode.GET_CONSUMER_LIST_BY_GROUP, null, header);
                    WriteLog(rs + "");
                }
                catch (Exception ex)
                {
                    XTrace.WriteException(ex);
                }
            }
        }
        #endregion
    }
}