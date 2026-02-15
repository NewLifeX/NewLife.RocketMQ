namespace NewLife.RocketMQ.Grpc;

/// <summary>gRPC响应状态码</summary>
/// <remarks>参考 apache.rocketmq.v2.Code</remarks>
public enum GrpcCode
{
    /// <summary>成功</summary>
    OK = 20000,

    /// <summary>多个结果</summary>
    MULTIPLE_RESULTS = 30000,

    /// <summary>错误请求</summary>
    BAD_REQUEST = 40000,

    /// <summary>非法访问点</summary>
    ILLEGAL_ACCESS_POINT = 40001,

    /// <summary>非法主题</summary>
    ILLEGAL_TOPIC = 40002,

    /// <summary>非法消费组</summary>
    ILLEGAL_CONSUMER_GROUP = 40003,

    /// <summary>非法消息标签</summary>
    ILLEGAL_MESSAGE_TAG = 40004,

    /// <summary>非法消息Key</summary>
    ILLEGAL_MESSAGE_KEY = 40005,

    /// <summary>非法消息分组</summary>
    ILLEGAL_MESSAGE_GROUP = 40006,

    /// <summary>非法消息属性Key</summary>
    ILLEGAL_MESSAGE_PROPERTY_KEY = 40007,

    /// <summary>无效事务ID</summary>
    INVALID_TRANSACTION_ID = 40008,

    /// <summary>非法消息ID</summary>
    ILLEGAL_MESSAGE_ID = 40009,

    /// <summary>非法过滤表达式</summary>
    ILLEGAL_FILTER_EXPRESSION = 40010,

    /// <summary>非法不可见时间</summary>
    ILLEGAL_INVISIBLE_TIME = 40011,

    /// <summary>非法投递时间</summary>
    ILLEGAL_DELIVERY_TIME = 40012,

    /// <summary>无效收据句柄</summary>
    INVALID_RECEIPT_HANDLE = 40013,

    /// <summary>消息属性与类型冲突</summary>
    MESSAGE_PROPERTY_CONFLICT_WITH_TYPE = 40014,

    /// <summary>未认证</summary>
    UNAUTHORIZED = 40100,

    /// <summary>禁止</summary>
    FORBIDDEN = 40300,

    /// <summary>未找到</summary>
    NOT_FOUND = 40400,

    /// <summary>消息未找到</summary>
    MESSAGE_NOT_FOUND = 40401,

    /// <summary>主题未找到</summary>
    TOPIC_NOT_FOUND = 40402,

    /// <summary>消费组未找到</summary>
    CONSUMER_GROUP_NOT_FOUND = 40403,

    /// <summary>请求超时</summary>
    REQUEST_TIMEOUT = 40800,

    /// <summary>消息体过大</summary>
    PAYLOAD_TOO_LARGE = 41300,

    /// <summary>前置条件失败</summary>
    PRECONDITION_FAILED = 42800,

    /// <summary>请求过于频繁</summary>
    TOO_MANY_REQUESTS = 42900,

    /// <summary>内部错误</summary>
    INTERNAL_ERROR = 50000,

    /// <summary>内部服务器错误</summary>
    INTERNAL_SERVER_ERROR = 50001,

    /// <summary>不支持</summary>
    UNSUPPORTED = 50100,

    /// <summary>消费消息失败</summary>
    FAILED_TO_CONSUME_MESSAGE = 60000,
}

/// <summary>消息类型</summary>
public enum GrpcMessageType
{
    /// <summary>普通消息</summary>
    NORMAL = 0,

    /// <summary>顺序消息（FIFO）</summary>
    FIFO = 1,

    /// <summary>延迟消息</summary>
    DELAY = 2,

    /// <summary>事务消息</summary>
    TRANSACTION = 3,
}

/// <summary>客户端类型</summary>
public enum GrpcClientType
{
    /// <summary>生产者</summary>
    PRODUCER = 1,

    /// <summary>推送消费者</summary>
    PUSH_CONSUMER = 2,

    /// <summary>简单消费者</summary>
    SIMPLE_CONSUMER = 3,

    /// <summary>Pull消费者</summary>
    PULL_CONSUMER = 4,
}

/// <summary>地址类型</summary>
public enum AddressScheme
{
    /// <summary>IPv4</summary>
    IPv4 = 0,

    /// <summary>IPv6</summary>
    IPv6 = 1,

    /// <summary>域名</summary>
    DOMAIN_NAME = 2,
}

/// <summary>队列权限</summary>
public enum GrpcPermission
{
    /// <summary>无权限</summary>
    NONE = 0,

    /// <summary>只读</summary>
    READ = 1,

    /// <summary>只写</summary>
    WRITE = 2,

    /// <summary>读写</summary>
    READ_WRITE = 3,
}

/// <summary>过滤类型</summary>
public enum GrpcFilterType
{
    /// <summary>Tag过滤</summary>
    TAG = 1,

    /// <summary>SQL过滤</summary>
    SQL = 2,
}

/// <summary>摘要类型</summary>
public enum GrpcDigestType
{
    /// <summary>CRC32</summary>
    CRC32 = 0,

    /// <summary>MD5</summary>
    MD5 = 1,

    /// <summary>SHA1</summary>
    SHA1 = 2,
}

/// <summary>消息编码</summary>
public enum GrpcEncoding
{
    /// <summary>不压缩</summary>
    IDENTITY = 0,

    /// <summary>GZIP压缩</summary>
    GZIP = 1,
}

/// <summary>事务来源</summary>
public enum GrpcTransactionSource
{
    /// <summary>服务端回查</summary>
    SOURCE_SERVER_CHECK = 0,

    /// <summary>客户端提交</summary>
    SOURCE_CLIENT = 1,
}

/// <summary>事务决议</summary>
public enum GrpcTransactionResolution
{
    /// <summary>提交</summary>
    COMMIT = 0,

    /// <summary>回滚</summary>
    ROLLBACK = 1,
}
