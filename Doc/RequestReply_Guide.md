# RocketMQ Request-Reply 使用指南

## 概述

从 RocketMQ 4.6.0 版本开始，引入了 Request-Reply 特性，该特性允许生产者在发送消息后同步或异步等待消费者消费完消息并返回响应消息，实现类似 RPC 调用的效果。

NewLife.RocketMQ 现已支持此特性，并兼容 RocketMQ 5.0+。

## 主要特性

- **同步请求**：发送请求后阻塞等待响应
- **异步请求**：发送请求后异步等待响应
- **超时控制**：支持设置请求超时时间
- **自动关联**：自动管理请求和响应的关联
- **简单易用**：API 设计简洁，易于集成

## 使用示例

### 1. 生产者端 - 发送请求

#### 同步请求

```csharp
using NewLife.RocketMQ;

// 创建生产者
var producer = new Producer
{
    Topic = "request_topic",
    NameServerAddress = "127.0.0.1:9876",
    RequestTimeout = 3000  // 设置默认超时时间为3秒
};
producer.Start();

try
{
    // 发送请求并同步等待响应
    var requestBody = "这是一个请求消息";
    var response = producer.Request(requestBody, timeout: 5000);
    
    Console.WriteLine($"收到响应: {response.BodyString}");
}
finally
{
    producer.Stop();
    producer.Dispose();
}
```

#### 异步请求

```csharp
using NewLife.RocketMQ;

// 创建生产者
var producer = new Producer
{
    Topic = "request_topic",
    NameServerAddress = "127.0.0.1:9876",
    RequestTimeout = 3000
};
producer.Start();

try
{
    // 异步发送请求并等待响应
    var requestBody = "这是一个异步请求消息";
    var response = await producer.RequestAsync(requestBody, timeout: 5000);
    
    Console.WriteLine($"收到响应: {response.BodyString}");
}
finally
{
    producer.Stop();
    producer.Dispose();
}
```

### 2. 消费者端 - 处理请求并发送回复

#### 同步处理

```csharp
using NewLife.RocketMQ;

// 创建消费者
var consumer = new Consumer
{
    Topic = "request_topic",
    Group = "request_consumer_group",
    NameServerAddress = "127.0.0.1:9876",
    FromLastOffset = false
};

// 设置消息处理回调
consumer.OnConsume = (queue, messages) =>
{
    foreach (var message in messages)
    {
        Console.WriteLine($"收到请求: {message.BodyString}");
        
        // 检查是否是请求消息
        if (!String.IsNullOrEmpty(message.CorrelationId))
        {
            // 处理业务逻辑
            var result = ProcessRequest(message.BodyString);
            
            // 发送回复
            consumer.SendReply(message, result);
            
            Console.WriteLine($"已发送回复: {result}");
        }
    }
    return true;
};

consumer.Start();

// 保持运行
Console.WriteLine("消费者已启动，按任意键退出...");
Console.ReadKey();

consumer.Stop();
consumer.Dispose();

string ProcessRequest(string request)
{
    // 实现你的业务逻辑
    return $"处理结果: {request}";
}
```

#### 异步处理

```csharp
using NewLife.RocketMQ;

// 创建消费者
var consumer = new Consumer
{
    Topic = "request_topic",
    Group = "request_consumer_group",
    NameServerAddress = "127.0.0.1:9876",
    FromLastOffset = false
};

// 设置异步消息处理回调
consumer.OnConsumeAsync = async (queue, messages, cancellationToken) =>
{
    foreach (var message in messages)
    {
        Console.WriteLine($"收到请求: {message.BodyString}");
        
        // 检查是否是请求消息
        if (!String.IsNullOrEmpty(message.CorrelationId))
        {
            // 异步处理业务逻辑
            var result = await ProcessRequestAsync(message.BodyString, cancellationToken);
            
            // 异步发送回复
            await consumer.SendReplyAsync(message, result, cancellationToken);
            
            Console.WriteLine($"已发送回复: {result}");
        }
    }
    return true;
};

consumer.Start();

// 保持运行
Console.WriteLine("消费者已启动，按任意键退出...");
Console.ReadKey();

consumer.Stop();
consumer.Dispose();

async Task<string> ProcessRequestAsync(string request, CancellationToken ct)
{
    // 实现你的异步业务逻辑
    await Task.Delay(100, ct);  // 模拟异步操作
    return $"处理结果: {request}";
}
```

### 3. 超时处理

```csharp
using NewLife.RocketMQ;

var producer = new Producer
{
    Topic = "request_topic",
    NameServerAddress = "127.0.0.1:9876",
    RequestTimeout = 1000  // 默认超时1秒
};
producer.Start();

try
{
    var response = await producer.RequestAsync("请求消息");
    Console.WriteLine($"收到响应: {response.BodyString}");
}
catch (TimeoutException ex)
{
    Console.WriteLine($"请求超时: {ex.Message}");
}
finally
{
    producer.Stop();
    producer.Dispose();
}
```

## API 参考

### Producer 类

#### 属性

- `RequestTimeout`：请求超时时间（毫秒），默认 3000ms

#### 方法

- `MessageExt Request(Message message, Int32 timeout = -1)`
  - 发送请求消息，同步等待响应
  - 参数：
    - `message`：请求消息
    - `timeout`：超时时间（毫秒），-1 表示使用默认超时时间
  - 返回：响应消息
  - 异常：`TimeoutException` - 请求超时

- `MessageExt Request(Object body, Int32 timeout = -1)`
  - 发送请求消息，同步等待响应（简化版本）
  - 参数：
    - `body`：消息体内容
    - `timeout`：超时时间（毫秒）
  - 返回：响应消息

- `Task<MessageExt> RequestAsync(Message message, Int32 timeout = -1, CancellationToken cancellationToken = default)`
  - 异步发送请求消息并等待响应
  - 参数：
    - `message`：请求消息
    - `timeout`：超时时间（毫秒）
    - `cancellationToken`：取消令牌
  - 返回：响应消息
  - 异常：`TimeoutException` - 请求超时

- `Task<MessageExt> RequestAsync(Object body, Int32 timeout = -1, CancellationToken cancellationToken = default)`
  - 异步发送请求消息并等待响应（简化版本）

### Consumer 类

#### 方法

- `SendResult SendReply(MessageExt requestMessage, Object replyBody)`
  - 发送回复消息
  - 参数：
    - `requestMessage`：原始请求消息
    - `replyBody`：回复消息内容
  - 返回：发送结果
  - 异常：
    - `ArgumentNullException` - 参数为空
    - `InvalidOperationException` - 请求消息缺少必要属性

- `Task<SendResult> SendReplyAsync(MessageExt requestMessage, Object replyBody, CancellationToken cancellationToken = default)`
  - 异步发送回复消息
  - 参数：
    - `requestMessage`：原始请求消息
    - `replyBody`：回复消息内容
    - `cancellationToken`：取消令牌
  - 返回：发送结果

### Message 类新增属性

- `ReplyToClient`：回复地址，指示回复消息应发送到的客户端ID
- `CorrelationId`：关联ID，用于将回复消息与请求消息关联
- `MessageType`：消息类型，用于区分普通消息和回复消息（"REQUEST"/"REPLY"）
- `RequestTimeout`：请求超时时间（毫秒）

## 注意事项

1. **版本要求**：需要 RocketMQ 服务器版本 4.6.0 或更高
2. **超时设置**：合理设置超时时间，避免长时间阻塞
3. **异常处理**：务必捕获 `TimeoutException` 处理超时情况
4. **资源释放**：使用完毕后及时释放 Producer 和 Consumer 资源
5. **Topic 规划**：建议为 Request-Reply 使用独立的 Topic
6. **消费者处理**：消费者必须检查 `CorrelationId` 属性来判断是否为请求消息

## 兼容性

- 支持 .NET Framework 4.5+
- 支持 .NET Standard 2.0+
- 支持 .NET Core 2.0+
- 支持 .NET 5.0+
- 兼容 RocketMQ 4.6.0 或以上版本
- 兼容 RocketMQ 5.0 或以上版本

## 性能建议

1. 复用 Producer 和 Consumer 实例，避免频繁创建销毁
2. 合理设置超时时间，避免资源浪费
3. 对于高并发场景，建议使用异步 API
4. 监控回复消息的处理时间，及时优化业务逻辑

## 故障排查

### 请求超时

- 检查消费者是否正常运行
- 检查网络连接是否正常
- 检查消费者处理逻辑是否耗时过长
- 适当增加超时时间

### 收不到回复

- 确认消费者正确调用了 `SendReply` 或 `SendReplyAsync`
- 检查消费者日志，确认是否有异常
- 确认消息的 `CorrelationId` 属性正确设置
- 检查 Topic 配置是否正确

## 更多示例

更多使用示例请参考项目源码中的单元测试：`XUnitTestRocketMQ/RequestReplyTests.cs`
