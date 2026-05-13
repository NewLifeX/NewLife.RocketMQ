using System;
using System.ComponentModel;
using System.Threading.Tasks;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Protocol;
using Xunit;

namespace XUnitTestRocketMQ;

/// <summary>Consumer.SendReply / SendReplyAsync 消息构建与参数校验测试</summary>
public class SendReplyMessageTests
{
    #region 参数校验 - SendReply
    [Fact]
    [DisplayName("SendReply_requestMessage为null抛出ArgumentNullException")]
    public void SendReply_NullRequestMessage_ThrowsArgumentNullException()
    {
        using var consumer = new Consumer();
        Assert.Throws<ArgumentNullException>(() => consumer.SendReply(null, "reply body"));
    }

    [Fact]
    [DisplayName("SendReply_replyBody为null抛出ArgumentNullException")]
    public void SendReply_NullReplyBody_ThrowsArgumentNullException()
    {
        using var consumer = new Consumer();
        var requestMsg = new MessageExt { Topic = "TestTopic" };
        requestMsg.ReplyToClient = "client-001";
        requestMsg.CorrelationId = "corr-001";

        Assert.Throws<ArgumentNullException>(() => consumer.SendReply(requestMsg, null));
    }

    [Fact]
    [DisplayName("SendReply_缺少ReplyToClient属性抛出InvalidOperationException")]
    public void SendReply_MissingReplyToClient_ThrowsInvalidOperationException()
    {
        using var consumer = new Consumer();
        // ReplyToClient 未设置
        var requestMsg = new MessageExt { Topic = "TestTopic" };
        requestMsg.CorrelationId = "corr-001";

        var ex = Assert.Throws<InvalidOperationException>(() => consumer.SendReply(requestMsg, "body"));
        Assert.Contains("ReplyToClient", ex.Message);
    }

    [Fact]
    [DisplayName("SendReply_ReplyToClient为空字符串抛出InvalidOperationException")]
    public void SendReply_EmptyReplyToClient_ThrowsInvalidOperationException()
    {
        using var consumer = new Consumer();
        var requestMsg = new MessageExt { Topic = "TestTopic" };
        requestMsg.ReplyToClient = "";
        requestMsg.CorrelationId = "corr-001";

        Assert.Throws<InvalidOperationException>(() => consumer.SendReply(requestMsg, "body"));
    }

    [Fact]
    [DisplayName("SendReply_缺少CorrelationId属性抛出InvalidOperationException")]
    public void SendReply_MissingCorrelationId_ThrowsInvalidOperationException()
    {
        using var consumer = new Consumer();
        var requestMsg = new MessageExt { Topic = "TestTopic" };
        requestMsg.ReplyToClient = "client-001";
        // CorrelationId 未设置

        var ex = Assert.Throws<InvalidOperationException>(() => consumer.SendReply(requestMsg, "body"));
        Assert.Contains("CorrelationId", ex.Message);
    }

    [Fact]
    [DisplayName("SendReply_CorrelationId为空字符串抛出InvalidOperationException")]
    public void SendReply_EmptyCorrelationId_ThrowsInvalidOperationException()
    {
        using var consumer = new Consumer();
        var requestMsg = new MessageExt { Topic = "TestTopic" };
        requestMsg.ReplyToClient = "client-001";
        requestMsg.CorrelationId = "";

        Assert.Throws<InvalidOperationException>(() => consumer.SendReply(requestMsg, "body"));
    }
    #endregion

    #region 参数校验 - SendReplyAsync
    [Fact]
    [DisplayName("SendReplyAsync_requestMessage为null抛出ArgumentNullException")]
    public async Task SendReplyAsync_NullRequestMessage_ThrowsArgumentNullException()
    {
        using var consumer = new Consumer();
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            consumer.SendReplyAsync(null, "reply body"));
    }

    [Fact]
    [DisplayName("SendReplyAsync_replyBody为null抛出ArgumentNullException")]
    public async Task SendReplyAsync_NullReplyBody_ThrowsArgumentNullException()
    {
        using var consumer = new Consumer();
        var requestMsg = new MessageExt { Topic = "TestTopic" };
        requestMsg.ReplyToClient = "client-001";
        requestMsg.CorrelationId = "corr-001";

        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            consumer.SendReplyAsync(requestMsg, null));
    }

    [Fact]
    [DisplayName("SendReplyAsync_缺少ReplyToClient属性抛出InvalidOperationException")]
    public async Task SendReplyAsync_MissingReplyToClient_ThrowsInvalidOperationException()
    {
        using var consumer = new Consumer();
        var requestMsg = new MessageExt { Topic = "TestTopic" };
        requestMsg.CorrelationId = "corr-001";

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            consumer.SendReplyAsync(requestMsg, "body"));
        Assert.Contains("ReplyToClient", ex.Message);
    }

    [Fact]
    [DisplayName("SendReplyAsync_缺少CorrelationId属性抛出InvalidOperationException")]
    public async Task SendReplyAsync_MissingCorrelationId_ThrowsInvalidOperationException()
    {
        using var consumer = new Consumer();
        var requestMsg = new MessageExt { Topic = "TestTopic" };
        requestMsg.ReplyToClient = "client-001";

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            consumer.SendReplyAsync(requestMsg, "body"));
        Assert.Contains("CorrelationId", ex.Message);
    }
    #endregion

    #region RequestMessage 属性构建验证
    [Fact]
    [DisplayName("MessageExt_设置ReplyToClient和CorrelationId属性后可正确读取")]
    public void MessageExt_ReplyProperties_ReadWriteCorrectly()
    {
        var msg = new MessageExt { Topic = "test-topic" };
        msg.ReplyToClient = "172.17.0.1@12345";
        msg.CorrelationId = "req-corr-abc123";

        Assert.Equal("172.17.0.1@12345", msg.ReplyToClient);
        Assert.Equal("req-corr-abc123", msg.CorrelationId);
    }

    [Fact]
    [DisplayName("Message_回复消息应设置CorrelationId和MessageType")]
    public void ReplyMessage_ShouldHaveCorrelationIdAndMessageType()
    {
        // 验证回复消息的属性构建逻辑（脱离网络层）
        var correlationId = "corr-xyz-789";
        var replyMessage = new Message
        {
            Topic = "request-topic",
            CorrelationId = correlationId,
            MessageType = "REPLY"
        };
        replyMessage.SetBody("reply content");

        Assert.Equal(correlationId, replyMessage.CorrelationId);
        Assert.Equal("REPLY", replyMessage.MessageType);
        Assert.Equal("request-topic", replyMessage.Topic);
        Assert.NotNull(replyMessage.Body);
        Assert.NotEmpty(replyMessage.Body);
    }
    #endregion
}
