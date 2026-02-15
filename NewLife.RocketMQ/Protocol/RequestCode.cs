namespace NewLife.RocketMQ.Protocol;

/// <summary>请求代码</summary>
public enum RequestCode
{
    /// <summary>发消息</summary>
    SEND_MESSAGE = 10,

    /// <summary>收消息</summary>
    PULL_MESSAGE = 11,

    /// <summary>查询消息</summary>
    QUERY_MESSAGE = 12,

    /// <summary>查询Broker偏移</summary>
    QUERY_BROKER_OFFSET = 13,

    /// <summary>查询消费偏移</summary>
    QUERY_CONSUMER_OFFSET = 14,

    /// <summary>更新消费者偏移</summary>
    UPDATE_CONSUMER_OFFSET = 15,

    /// <summary>更新或创建Topic</summary>
    UPDATE_AND_CREATE_TOPIC = 17,

    /// <summary>用于向brokers查询所有的topic和它们的配置</summary>
    GET_ALL_TOPIC_CONFIG = 21,

    /// <summary>获取Topic配置列表</summary>
    GET_TOPIC_CONFIG_LIST = 22,

    /// <summary>获取Topic名列表</summary>
    GET_TOPIC_NAME_LIST = 23,

    /// <summary>更新Broker配置</summary>
    UPDATE_BROKER_CONFIG = 25,

    /// <summary>获取代理配置</summary>
    GET_BROKER_CONFIG = 26,

    /// <summary>触发删除文件</summary>
    TRIGGER_DELETE_FILES = 27,

    /// <summary>获取代理运行时信息,包括broker版本、磁盘容量、系统负载等</summary>
    GET_BROKER_RUNTIME_INFO = 28,

    /// <summary>按时间戳搜索偏移</summary>
    SEARCH_OFFSET_BY_TIMESTAMP = 29,

    /// <summary>获取topic/队列偏移量的最大值</summary>
    GET_MAX_OFFSET = 30,

    /// <summary>获取topic/队列偏移量的最小值</summary>
    GET_MIN_OFFSET = 31,

    /// <summary>获取最早消息存储时间</summary>
    GET_EARLIEST_MSG_STORETIME = 32,

    /// <summary>按消息ID查看消息</summary>
    VIEW_MESSAGE_BY_ID = 33,

    /// <summary>发送心跳</summary>
    HEART_BEAT = 34,

    /// <summary>注销</summary>
    UNREGISTER_CLIENT = 35,

    /// <summary>当consumer客户端无法处理消息时,将这些消息发送回brokers,以便将来将这些消息重新发送给consumers</summary>
    CONSUMER_SEND_MSG_BACK = 36,

    /// <summary>结束事务</summary>
    END_TRANSACTION = 37,

    /// <summary>查询每个consumer group的存活成员</summary>
    GET_CONSUMER_LIST_BY_GROUP = 38,

    /// <summary>检查事务状态</summary>
    CHECK_TRANSACTION_STATE = 39,

    /// <summary>当broker得知一个consumer宕机时,它会通知其他工作的consumers尽快重新平衡</summary>
    NOTIFY_CONSUMER_IDS_CHANGED = 40,

    /// <summary>锁定批</summary>
    LOCK_BATCH_MQ = 41,

    /// <summary>解锁批</summary>
    UNLOCK_BATCH_MQ = 42,

    /// <summary>获取所有的消费的偏移量</summary>
    GET_ALL_CONSUMER_OFFSET = 43,

    /// <summary>获取延迟topic的偏移量</summary>
    GET_ALL_DELAY_OFFSET = 45,

    /// <summary>检查客户端配置</summary>
    CHECK_CLIENT_CONFIG = 46,

    /// <summary>提交KV配置</summary>
    PUT_KV_CONFIG = 100,

    /// <summary>获取KV配置</summary>
    GET_KV_CONFIG = 101,

    /// <summary>删除KV配置</summary>
    DELETE_KV_CONFIG = 102,

    /// <summary>注册Broker</summary>
    REGISTER_BROKER = 103,
    /// <summary>取消注册Broker</summary>
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

    /// <summary>获取消费者连接列表</summary>
    GET_CONSUMER_CONNECTION_LIST = 203,

    /// <summary>获取生产者连接列表</summary>
    GET_PRODUCER_CONNECTION_LIST = 204,

    /// <summary>写Broker权限</summary>
    WIPE_WRITE_PERM_OF_BROKER = 205,

    /// <summary>查询所有topic</summary>
    GET_ALL_TOPIC_LIST_FROM_NAMESERVER = 206,

    /// <summary>删除订阅组</summary>
    DELETE_SUBSCRIPTIONGROUP = 207,

    /// <summary>获取消费者状态</summary>
    GET_CONSUME_STATS = 208,

    /// <summary>挂起消费者</summary>
    SUSPEND_CONSUMER = 209,

    /// <summary>恢复消费者</summary>
    RESUME_CONSUMER = 210,

    /// <summary>重置消费者偏移</summary>
    RESET_CONSUMER_OFFSET_IN_CONSUMER = 211,

    /// <summary>重置Broker中的消费者偏移</summary>
    RESET_CONSUMER_OFFSET_IN_BROKER = 212,

    /// <summary>对齐消费者线程池</summary>
    ADJUST_CONSUMER_THREAD_POOL = 213,

    /// <summary>查询谁消费了指定消息</summary>
    WHO_CONSUME_THE_MESSAGE = 214,

    /// <summary>在Broker中删除Topic</summary>
    DELETE_TOPIC_IN_BROKER = 215,

    /// <summary>在Name服务器中删除Topic</summary>
    DELETE_TOPIC_IN_NAMESRV = 216,

    /// <summary>根据命名空间获取KV列表</summary>
    GET_KVLIST_BY_NAMESPACE = 219,

    /// <summary>重置消费者客户端偏移</summary>
    RESET_CONSUMER_CLIENT_OFFSET = 220,

    /// <summary>从客户端获取消费者状态</summary>
    GET_CONSUMER_STATUS_FROM_CLIENT = 221,

    /// <summary>要求broker从consumer客户端按给定的时间戳重置偏移量</summary>
    INVOKE_BROKER_TO_RESET_OFFSET = 222,

    /// <summary>要求Broker获取消费者状态</summary>
    INVOKE_BROKER_TO_GET_CONSUMER_STATUS = 223,

    /// <summary>查询谁消费了指定Topic</summary>
    QUERY_TOPIC_CONSUME_BY_WHO = 300,

    /// <summary>获取集群中Topic</summary>
    GET_TOPICS_BY_CLUSTER = 224,

    /// <summary>注册文件服务器</summary>
    REGISTER_FILTER_SERVER = 301,

    /// <summary>注册消息过滤类</summary>
    REGISTER_MESSAGE_FILTER_CLASS = 302,

    /// <summary>查询消费时间</summary>
    QUERY_CONSUME_TIME_SPAN = 303,

    /// <summary>从名称服务器查询系统Topic列表</summary>
    GET_SYSTEM_TOPIC_LIST_FROM_NS = 304,

    /// <summary>从Broker服务器查询系统Topic列表</summary>
    GET_SYSTEM_TOPIC_LIST_FROM_BROKER = 305,

    /// <summary>清理过滤消费队列</summary>
    CLEAN_EXPIRED_CONSUMEQUEUE = 306,

    /// <summary>获取消费者运行消息</summary>
    GET_CONSUMER_RUNNING_INFO = 307,

    /// <summary>查询协调器偏移</summary>
    QUERY_CORRECTION_OFFSET = 308,

    /// <summary>直接消费消息</summary>
    CONSUME_MESSAGE_DIRECTLY = 309,

    /// <summary>发送消息V2</summary>
    SEND_MESSAGE_V2 = 310,

    /// <summary>获取单元Topic列表</summary>
    GET_UNIT_TOPIC_LIST = 311,

    /// <summary></summary>
    GET_HAS_UNIT_SUB_TOPIC_LIST = 312,

    /// <summary></summary>
    GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST = 313,

    /// <summary>克隆组偏移</summary>
    CLONE_GROUP_OFFSET = 314,

    /// <summary>查看Broker状态数据</summary>
    VIEW_BROKER_STATS_DATA = 315,

    /// <summary>清理未使用Topic</summary>
    CLEAN_UNUSED_TOPIC = 316,

    /// <summary>获取消费状态</summary>
    GET_BROKER_CONSUME_STATS = 317,

    /// <summary>update the config of name server</summary>
    UPDATE_NAMESRV_CONFIG = 318,

    /// <summary>get config from name server</summary>
    GET_NAMESRV_CONFIG = 319,

    /// <summary>批处理模式发送消息</summary>
    SEND_BATCH_MESSAGE = 320,

    /// <summary>查询消费队列</summary>
    QUERY_CONSUME_QUEUE = 321,

    /// <summary>查询数据版本</summary>
    QUERY_DATA_VERSION = 322,

    /// <summary>请求消息(Request-Reply特性)</summary>
    REQUEST_MESSAGE = 323,

    /// <summary>发送回复消息</summary>
    SEND_REPLY_MESSAGE = 324,

    /// <summary>发送回复消息V2</summary>
    SEND_REPLY_MESSAGE_V2 = 325,
}