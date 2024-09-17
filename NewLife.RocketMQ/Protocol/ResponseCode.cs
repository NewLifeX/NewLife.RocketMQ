namespace NewLife.RocketMQ.Protocol;

/// <summary>响应码</summary>
public enum ResponseCode
{
    /// <summary>成功</summary>
    SUCCESS = 0,

    /// <summary>系统错误</summary>
    SYSTEM_ERROR = 1,

    /// <summary>系统忙</summary>
    SYSTEM_BUSY = 2,

    /// <summary>不支持的请求码</summary>
    REQUEST_CODE_NOT_SUPPORTED = 3,

    /// <summary>事务失败</summary>
    TRANSACTION_FAILED = 4,

    /// <summary>刷磁盘超时</summary>
    FLUSH_DISK_TIMEOUT = 10,

    /// <summary>从机不可用</summary>
    SLAVE_NOT_AVAILABLE = 11,

    /// <summary>刷从机超时</summary>
    FLUSH_SLAVE_TIMEOUT = 12,

    /// <summary>非法消息</summary>
    MESSAGE_ILLEGAL = 13,

    /// <summary>服务不可用</summary>
    SERVICE_NOT_AVAILABLE = 14,

    /// <summary>不支持的版本</summary>
    VERSION_NOT_SUPPORTED = 15,

    /// <summary>没有权限</summary>
    NO_PERMISSION = 16,

    /// <summary>主题不存在</summary>
    TOPIC_NOT_EXIST = 17,

    /// <summary>主题已存在</summary>
    TOPIC_EXIST_ALREADY = 18,

    /// <summary>拉取未发现</summary>
    PULL_NOT_FOUND = 19,

    /// <summary>请重试拉取</summary>
    PULL_RETRY_IMMEDIATELY = 20,

    /// <summary>拉取偏移被移动</summary>
    PULL_OFFSET_MOVED = 21,

    /// <summary>查询未发现</summary>
    QUERY_NOT_FOUND = 22,

    /// <summary>订阅分析失败</summary>
    SUBSCRIPTION_PARSE_FAILED = 23,

    /// <summary>订阅不存在</summary>
    SUBSCRIPTION_NOT_EXIST = 24,

    /// <summary>订阅不是最新</summary>
    SUBSCRIPTION_NOT_LATEST = 25,

    /// <summary>订阅组不存在</summary>
    SUBSCRIPTION_GROUP_NOT_EXIST = 26,

    /// <summary>过滤数据不存在</summary>
    FILTER_DATA_NOT_EXIST = 27,

    /// <summary>过滤数据不是最新</summary>
    FILTER_DATA_NOT_LATEST = 28,

    /// <summary>事务应该提交</summary>
    TRANSACTION_SHOULD_COMMIT = 200,

    /// <summary>事务应该回滚</summary>
    TRANSACTION_SHOULD_ROLLBACK = 201,

    /// <summary>事务状态未知</summary>
    TRANSACTION_STATE_UNKNOW = 202,

    /// <summary>事务状态组错误</summary>
    TRANSACTION_STATE_GROUP_WRONG = 203,

    /// <summary>未购买</summary>
    NO_BUYER_ID = 204,

    /// <summary>不在当前单元</summary>
    NOT_IN_CURRENT_UNIT = 205,

    /// <summary>消费者不在线</summary>
    CONSUMER_NOT_ONLINE = 206,

    /// <summary>消费消息超时</summary>
    CONSUME_MSG_TIMEOUT = 207,

    /// <summary>没有消息</summary>
    NO_MESSAGE = 208,
}