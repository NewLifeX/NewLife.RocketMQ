using System;
using System.Linq;
using NewLife.Log;
using NewLife.RocketMQ.Client;
using NewLife.RocketMQ.Protocol;

namespace NewLife.RocketMQ.Producer
{
    public class MQProducer : MQAdmin
    {
        #region 属性
        public String ProducerGroup { get; set; } = "DEFAULT_PRODUCER";

        public String CreateTopicKey { get; set; } = "TBW102";

        public Int32 DefaultTopicQueueNums { get; set; } = 4;

        public Int32 SendMsgTimeout { get; set; } = 3_000;

        public Int32 CompressMsgBodyOverHowmuch { get; set; } = 4096;

        public Int32 RetryTimesWhenSendFailed { get; set; } = 2;

        public Int32 RetryTimesWhenSendAsyncFailed { get; set; } = 2;

        public Boolean RetryAnotherBrokerWhenNotStoreOK { get; set; }

        public Int32 MaxMessageSize { get; set; } = 4 * 1024 * 1024;

        private ServiceState State { get; set; } = ServiceState.CreateJust;

        private NameClient _Client;
        #endregion

        #region 基础方法
        public override void Start()
        {
            base.Start();

            switch (State)
            {
                case ServiceState.CreateJust:
                    State = ServiceState.CreateJust;

                    var client = new NameClient(ClientId, this);
                    client.Start();

                    var rs = client.GetRouteInfo(CreateTopicKey);
                    foreach (var item in rs)
                    {
                        XTrace.WriteLine("发现Broker[{0}]: {1}", item.Key, item.Value);
                    }

                    _Client = client;

                    State = ServiceState.Running;
                    break;
                case ServiceState.Running:
                case ServiceState.ShutdownAlready:
                case ServiceState.StartFailed:
                    throw new Exception("已启动！");
            }
        }
        #endregion

        #region 发送消息
        private BrokerClient _Broker;
        protected BrokerClient GetBroker()
        {
            if (_Broker != null) return _Broker;

            var bk = _Client.Brokers?.FirstOrDefault();
            if (bk == null) return null;

            var addr = bk.Value.Value?.Split(";").FirstOrDefault();
            if (addr.IsNullOrEmpty()) return null;

            var client = new BrokerClient(addr);
            client.Config = this;
            client.Start();

            return _Broker = client;
        }

        public virtual SendResult Send(Message msg, Int32 timeout = -1)
        {
            var bk = GetBroker();

            return null;
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