using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NewLife.RocketMQ.Client
{
    public abstract class MQAdmin
    {
        public abstract void CreateTopic(String key, String newTopic, Int32 queueNum, Int32 topicSysFla);

        public abstract Int64 SearchOffset(MessageQueue mq, Int64 timestamp);

        public abstract Int64 MaxOffset(MessageQueue mq);

        public abstract Int64 MinOffset(MessageQueue mq);

        public abstract Int64 EarliestMsgStoreTime(MessageQueue mq);

        public abstract MessageExt ViewMessage(String offsetMsgId);

        public abstract QueryResult QueryMessage(String topic, String key, Int32 maxNum, Int64 begin, Int64 end);

        public abstract MessageExt ViewMessage(String topic, String msgId);
    }
}