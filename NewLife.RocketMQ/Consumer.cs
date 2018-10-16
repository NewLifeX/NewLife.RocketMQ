using System;
using NewLife.RocketMQ.Client;
using NewLife.RocketMQ.Protocol;

namespace NewLife.RocketMQ
{
    /// <summary>消费者</summary>
    public class Consumer : MQAdmin
    {
        #region 拉取消息
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

            var rs = bk.Send(RequestCode.PULL_MESSAGE, null, dic);

            var pr = new PullResult
            {
                Status = PullStatus.Found,
            };
            pr.Read(rs.Header.ExtFields);

            return pr;
        }
        #endregion

        #region 业务方法
        public Int64 QueryOffset(MessageQueue mq)
        {
            var bk = GetBroker(mq.BrokerName);
            var rs = bk.Send(RequestCode.QUERY_CONSUMER_OFFSET, null, new
            {
                consumerGroup = Group,
                topic = Topic,
                queueId = mq.QueueId,
            });

            return rs.Header.ExtFields["offset"].ToLong();
        }

        public Int64 SearchOffset(MessageQueue mq, Int64 timestamp)
        {
            throw new NotImplementedException();
        }
        #endregion
    }
}