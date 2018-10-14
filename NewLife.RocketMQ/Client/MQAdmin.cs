using System;
using System.Net.Http;
using NewLife.RocketMQ.Protocol;

namespace NewLife.RocketMQ.Client
{
    public abstract class MQAdmin : DisposeBase
    {
        #region 属性
        /// <summary>名称服务器地址</summary>
        public String NameServerAddress { get; set; }

        /// <summary>本地IP地址</summary>
        public String ClientIP { get; set; } = NetHelper.MyIP() + "";

        /// <summary>本地端口</summary>
        public Int32 ClientPort { get; set; }

        /// <summary>实例名</summary>
        public String InstanceName { get; set; } = "DEFAULT";

        /// <summary>客户端回调执行线程数。默认CPU数</summary>
        public Int32 ClientCallbackExecutorThreads { get; set; } = Environment.ProcessorCount;

        /// <summary>拉取名称服务器间隔。默认30_000ms</summary>
        public Int32 PollNameServerInterval { get; set; } = 30_000;

        /// <summary>Broker心跳间隔。默认30_000ms</summary>
        public Int32 HeartbeatBrokerInterval { get; set; } = 30_000;

        /// <summary>持久化消费偏移间隔。默认5_000ms</summary>
        public Int32 PersistConsumerOffsetInterval { get; set; } = 5_000;

        public String UnitName { get; set; }

        public Boolean UnitMode { get; set; }

        public Boolean VipChannelEnabled { get; set; } = true;
        #endregion

        #region 阿里云属性
        /// <summary>获取名称服务器地址的http地址</summary>
        public String Server { get; set; }

        /// <summary>访问令牌</summary>
        public String AccessKey { get; set; }

        /// <summary>访问密钥</summary>
        public String SecretKey { get; set; }

        /// <summary>阿里云MQ通道</summary>
        public String OnsChannel { get; set; } = "ALIYUN";
        #endregion

        #region 扩展属性
        public String ClientId
        {
            get
            {
                var str = $"{ClientIP}@{InstanceName}";
                if (!UnitName.IsNullOrEmpty()) str += "@" + UnitName;
                return str;
            }
        }
        #endregion

        #region 基础方法
        public virtual void Start()
        {
            // 获取阿里云ONS的名称服务器地址
            var addr = Server;
            if (!addr.IsNullOrEmpty() && addr.StartsWithIgnoreCase("http"))
            {
                var http = new HttpClient();
                var rs = http.GetStringAsync(addr).Result;
                if (!rs.IsNullOrWhiteSpace()) NameServerAddress = rs.Trim();
            }
        }
        #endregion

        public abstract void CreateTopic(String key, String newTopic, Int32 queueNum, Int32 topicSysFlag = 0);

        public abstract Int64 SearchOffset(MessageQueue mq, Int64 timestamp);

        public abstract Int64 MaxOffset(MessageQueue mq);

        public abstract Int64 MinOffset(MessageQueue mq);

        public abstract Int64 EarliestMsgStoreTime(MessageQueue mq);

        public abstract QueryResult QueryMessage(String topic, String key, Int32 maxNum, Int64 begin, Int64 end);

        public abstract MessageExt ViewMessage(String offsetMsgId);

        public abstract MessageExt ViewMessage(String topic, String msgId);
    }
}