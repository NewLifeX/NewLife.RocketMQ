using System;
using NewLife.RocketMQ.Client;

namespace NewLife.RocketMQ.Consumer
{
    public class MQPullConsumer : MQConsumer
    {
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
    }
}