using System;
using System.Threading.Tasks;
using NewLife;
using NewLife.Log;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Protocol;
using Xunit;

namespace XUnitTestRocketMQ;

/// <summary>Request-Reply 特性测试</summary>
public class RequestReplyTests
{
    [Fact(Skip = "需要RocketMQ服务器支持")]
    public void RequestSyncTest()
    {
        var set = BasicTest.GetConfig();
        
        // 创建生产者
        using var producer = new Producer
        {
            Topic = "nx_request_test",
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
        };
        producer.Start();

        // 创建消费者
        using var consumer = new Consumer
        {
            Topic = "nx_request_test",
            Group = "nx_request_group",
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
            FromLastOffset = true,
        };

        // 消费者处理请求并返回回复
        consumer.OnConsume = (q, ms) =>
        {
            foreach (var item in ms)
            {
                XTrace.WriteLine("收到请求: {0}", item.BodyString);
                
                // 如果是请求消息，发送回复
                if (!String.IsNullOrEmpty(item.CorrelationId))
                {
                    var replyBody = $"Reply to: {item.BodyString}";
                    consumer.SendReply(item, replyBody);
                }
            }
            return true;
        };

        consumer.Start();

        // 发送请求并等待响应
        var requestBody = "Hello, this is a request!";
        var response = producer.Request(requestBody, 5000);

        Assert.NotNull(response);
        Assert.Contains("Reply to:", response.BodyString);
        XTrace.WriteLine("收到响应: {0}", response.BodyString);
    }

    [Fact(Skip = "需要RocketMQ服务器支持")]
    public async Task RequestAsyncTest()
    {
        var set = BasicTest.GetConfig();
        
        // 创建生产者
        using var producer = new Producer
        {
            Topic = "nx_request_test_async",
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
        };
        producer.Start();

        // 创建消费者
        using var consumer = new Consumer
        {
            Topic = "nx_request_test_async",
            Group = "nx_request_async_group",
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
            FromLastOffset = true,
        };

        // 消费者异步处理请求并返回回复
        consumer.OnConsumeAsync = async (q, ms, ct) =>
        {
            foreach (var item in ms)
            {
                XTrace.WriteLine("收到请求: {0}", item.BodyString);
                
                // 如果是请求消息，发送回复
                if (!String.IsNullOrEmpty(item.CorrelationId))
                {
                    var replyBody = $"Async Reply to: {item.BodyString}";
                    await consumer.SendReplyAsync(item, replyBody, ct).ConfigureAwait(false);
                }
            }
            return true;
        };

        consumer.Start();

        // 异步发送请求并等待响应
        var requestBody = "Hello, this is an async request!";
        var response = await producer.RequestAsync(requestBody, 5000).ConfigureAwait(false);

        Assert.NotNull(response);
        Assert.Contains("Async Reply to:", response.BodyString);
        XTrace.WriteLine("收到响应: {0}", response.BodyString);
    }

    [Fact(Skip = "需要RocketMQ服务器支持")]
    public async Task RequestTimeoutTest()
    {
        var set = BasicTest.GetConfig();
        
        // 创建生产者
        using var producer = new Producer
        {
            Topic = "nx_request_timeout_test",
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
            RequestTimeout = 1000, // 1秒超时
        };
        producer.Start();

        // 不启动消费者，请求应该超时

        // 发送请求，期望超时
        var requestBody = "This should timeout";
        
        await Assert.ThrowsAsync<TimeoutException>(async () =>
        {
            await producer.RequestAsync(requestBody).ConfigureAwait(false);
        });

        XTrace.WriteLine("请求超时测试通过");
    }

    [Fact]
    public void MessagePropertiesTest()
    {
        // 测试消息属性
        var message = new Message
        {
            Topic = "test_topic",
            ReplyToClient = "client_123",
            CorrelationId = "corr_456",
            MessageType = "REQUEST",
            RequestTimeout = 3000
        };
        message.SetBody("test body");

        Assert.Equal("client_123", message.ReplyToClient);
        Assert.Equal("corr_456", message.CorrelationId);
        Assert.Equal("REQUEST", message.MessageType);
        Assert.Equal(3000, message.RequestTimeout);

        // 测试属性序列化
        var props = message.GetProperties();
        Assert.Contains("REPLY_TO_CLIENT", props);
        Assert.Contains("CORRELATION_ID", props);
        Assert.Contains("MSG_TYPE", props);
        Assert.Contains("REQUEST_TIMEOUT", props);
    }
}
