namespace NewLife.RocketMQ.Protocol
{
    /// <summary>请求代码</summary>
    public enum RequestCode
    {
        /// <summary>发消息</summary>
        SEND_MESSAGE = 10,

        /// <summary>收消息</summary>
        PULL_MESSAGE = 11,

        QUERY_MESSAGE = 12,
        QUERY_BROKER_OFFSET = 13,

        /// <summary>查询消费偏移</summary>
        QUERY_CONSUMER_OFFSET = 14,

        UPDATE_CONSUMER_OFFSET = 15,
        UPDATE_AND_CREATE_TOPIC = 17,

        /// <summary>用于向brokers查询所有的topic和它们的配置</summary>
        GET_ALL_TOPIC_CONFIG = 21,
        GET_TOPIC_CONFIG_LIST = 22,

        GET_TOPIC_NAME_LIST = 23,

        UPDATE_BROKER_CONFIG = 25,

        /// <summary>获取代理配置</summary>
        GET_BROKER_CONFIG = 26,

        TRIGGER_DELETE_FILES = 27,

        /// <summary>获取代理运行时信息,包括broker版本、磁盘容量、系统负载等</summary>
        GET_BROKER_RUNTIME_INFO = 28,
        SEARCH_OFFSET_BY_TIMESTAMP = 29,

        /// <summary>获取topic/队列偏移量的最大值</summary>
        GET_MAX_OFFSET = 30,

        /// <summary>获取topic/队列偏移量的最小值</summary>
        GET_MIN_OFFSET = 31,

        GET_EARLIEST_MSG_STORETIME = 32,

        VIEW_MESSAGE_BY_ID = 33,

        /// <summary>发送心跳</summary>
        HEART_BEAT = 34,

        /// <summary>注销</summary>
        UNREGISTER_CLIENT = 35,

        /// <summary>当consumer客户端无法处理消息时,将这些消息发送回brokers,以便将来将这些消息重新发送给consumers</summary>
        CONSUMER_SEND_MSG_BACK = 36,

        END_TRANSACTION = 37,

        /// <summary>查询每个consumer group的存活成员</summary>
        GET_CONSUMER_LIST_BY_GROUP = 38,

        CHECK_TRANSACTION_STATE = 39,

        /// <summary>当broker得知一个consumer宕机时,它会通知其他工作的consumers尽快重新平衡</summary>
        NOTIFY_CONSUMER_IDS_CHANGED = 40,

        LOCK_BATCH_MQ = 41,

        UNLOCK_BATCH_MQ = 42,

        /// <summary>获取所有的消费的偏移量</summary>
        GET_ALL_CONSUMER_OFFSET = 43,

        /// <summary>获取延迟topic的偏移量</summary>
        GET_ALL_DELAY_OFFSET = 45,

        CHECK_CLIENT_CONFIG = 46,

        PUT_KV_CONFIG = 100,

        GET_KV_CONFIG = 101,

        DELETE_KV_CONFIG = 102,

        REGISTER_BROKER = 103,
        UNREGISTER_BROKER = 104,

        /// <summary>获取topic路由信息</summary>
        GET_ROUTEINTO_BY_TOPIC = 105,

        /// <summary>获取群集信息</summary>
        GET_BROKER_CLUSTER_INFO = 106,

        /// <summary>创建新的consumer group或更新现有的consumer group以更改属性</summary>
        UPDATE_AND_CREATE_SUBSCRIPTIONGROUP = 200,

        /// <summary>查询所有已知的consumer group配置</summary>
        GET_ALL_SUBSCRIPTIONGROUP_CONFIG = 201,

        /// <summary>查询topic相关的统计信息</summary>
        GET_TOPIC_STATS_INFO = 202,
        GET_CONSUMER_CONNECTION_LIST = 203,
        GET_PRODUCER_CONNECTION_LIST = 204,
        WIPE_WRITE_PERM_OF_BROKER = 205,

        /// <summary>查询所有topic</summary>
        GET_ALL_TOPIC_LIST_FROM_NAMESERVER = 206,

        DELETE_SUBSCRIPTIONGROUP = 207,
        GET_CONSUME_STATS = 208,

        SUSPEND_CONSUMER = 209,

        RESUME_CONSUMER = 210,
        RESET_CONSUMER_OFFSET_IN_CONSUMER = 211,
        RESET_CONSUMER_OFFSET_IN_BROKER = 212,

        ADJUST_CONSUMER_THREAD_POOL = 213,

        WHO_CONSUME_THE_MESSAGE = 214,

        DELETE_TOPIC_IN_BROKER = 215,

        DELETE_TOPIC_IN_NAMESRV = 216,
        GET_KVLIST_BY_NAMESPACE = 219,

        RESET_CONSUMER_CLIENT_OFFSET = 220,

        GET_CONSUMER_STATUS_FROM_CLIENT = 221,

        /// <summary>要求broker从consumer客户端按给定的时间戳重置偏移量</summary>
        INVOKE_BROKER_TO_RESET_OFFSET = 222,

        INVOKE_BROKER_TO_GET_CONSUMER_STATUS = 223,

        QUERY_TOPIC_CONSUME_BY_WHO = 300,

        GET_TOPICS_BY_CLUSTER = 224,

        REGISTER_FILTER_SERVER = 301,
        REGISTER_MESSAGE_FILTER_CLASS = 302,

        QUERY_CONSUME_TIME_SPAN = 303,

        GET_SYSTEM_TOPIC_LIST_FROM_NS = 304,
        GET_SYSTEM_TOPIC_LIST_FROM_BROKER = 305,

        CLEAN_EXPIRED_CONSUMEQUEUE = 306,

        GET_CONSUMER_RUNNING_INFO = 307,

        QUERY_CORRECTION_OFFSET = 308,
        CONSUME_MESSAGE_DIRECTLY = 309,

        SEND_MESSAGE_V2 = 310,

        GET_UNIT_TOPIC_LIST = 311,

        GET_HAS_UNIT_SUB_TOPIC_LIST = 312,

        GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST = 313,

        CLONE_GROUP_OFFSET = 314,

        VIEW_BROKER_STATS_DATA = 315,

        CLEAN_UNUSED_TOPIC = 316,

        GET_BROKER_CONSUME_STATS = 317,

        /// <summary>update the config of name server</summary>
        UPDATE_NAMESRV_CONFIG = 318,

        /// <summary>get config from name server</summary>
        GET_NAMESRV_CONFIG = 319,

        /// <summary>批处理模式发送消息</summary>
        SEND_BATCH_MESSAGE = 320,

        QUERY_CONSUME_QUEUE = 321,

        QUERY_DATA_VERSION = 322,
    }
}