using System;
using System.ComponentModel;
using NewLife;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Protocol;
using NewLife.Serialization;
using Xunit;

namespace XUnitTest.Models;

public class MessageTests
{
    [Fact]
    public void SetBody_WithString_SetsBodyCorrectly()
    {
        // Arrange
        var message = new Message();
        var body = "Hello, World!";

        // Act
        message.SetBody(body);

        // Assert
        Assert.Equal(body, message.BodyString);
        Assert.Equal(body.GetBytes(), message.Body);
    }

    [Fact]
    public void SetBody_WithByteArray_SetsBodyCorrectly()
    {
        // Arrange
        var message = new Message();
        var body = new byte[] { 1, 2, 3, 4 };

        // Act
        message.SetBody(body);

        // Assert
        Assert.Equal(body, message.Body);
    }

    [Fact]
    public void GetProperties_ReturnsCorrectProperties()
    {
        // Arrange
        var message = new Message
        {
            Tags = "Tag1",
            Keys = "Key1",
            DelayTimeLevel = 2,
            WaitStoreMsgOK = false
        };

        // Act
        var properties = message.GetProperties();

        // Assert
        Assert.Contains("TAGS\u0001Tag1\u0002", properties);
        Assert.Contains("KEYS\u0001Key1\u0002", properties);
        Assert.Contains("DELAY\u00012\u0002", properties);
        Assert.Contains("WAIT\u0001False\u0002", properties);

        var header = new SendMessageRequestHeader
        {
            ProducerGroup = "TestGroup",
            Topic = "TestTopic",
            SysFlag = 0,
            BornTimestamp = DateTime.UtcNow.ToLong(),
            Flag = message.Flag,
            Properties = message.GetProperties(),
        };

        var ext = header.GetProperties();
        //Assert.Equal(11, ext.Count);
        Assert.Equal("TAGS\u0001Tag1\u0002KEYS\u0001Key1\u0002DELAY\u00012\u0002WAIT\u0001False\u0002", ext["i"]);

        var broker = new BrokerClient([""]);
        var cmd = broker.CreateCommand(RequestCode.SEND_MESSAGE_V2, null, ext);
        var json = cmd.Header.ToJson(false, false, false);
        var js = new SystemJson();
        var json2 = js.Write(cmd.Header, false, false, true);
        //Assert.Equal(json, json2);
    }

    [Fact]
    public void ParseProperties_SetsPropertiesCorrectly()
    {
        // Arrange
        var message = new Message();
        var properties = "TAGS\u0001Tag1\u0002KEYS\u0001Key1\u0002DELAY\u00012\u0002WAIT\u0001False\u0002";

        // Act
        var result = message.ParseProperties(properties);

        // Assert
        Assert.Equal("Tag1", message.Tags);
        Assert.Equal("Key1", message.Keys);
        Assert.Equal(2, message.DelayTimeLevel);
        Assert.False(message.WaitStoreMsgOK);
    }

    #region 扩展覆盖

    [Fact]
    [DisplayName("SetBody_JSON对象_序列化为JSON字符串")]
    public void SetBody_WithJsonObject_SerializesToJson()
    {
        var message = new Message();
        var obj = new { Name = "Alice", Age = 30 };

        message.SetBody(obj);

        Assert.NotNull(message.Body);
        Assert.Contains("Alice", message.BodyString);
        Assert.Contains("30", message.BodyString);
    }

    [Fact]
    [DisplayName("SetBody_Message类型_抛出ArgumentOutOfRangeException")]
    public void SetBody_MessageType_ThrowsArgumentOutOfRangeException()
    {
        // Producer.CreateMessage 禁止 body 是 Message 类型
        var producer = new Producer();
        var innerMsg = new Message();

        Assert.Throws<ArgumentOutOfRangeException>(() => producer.Publish(innerMsg, "TagA", null));
    }

    [Fact]
    [DisplayName("PutUserProperty_设置并获取用户属性")]
    public void PutUserProperty_GetUserProperty_Works()
    {
        var message = new Message();
        message.PutUserProperty("env", "prod");

        Assert.Equal("prod", message.GetUserProperty("env"));
    }

    [Fact]
    [DisplayName("PutUserProperty_key为空_抛出ArgumentNullException")]
    public void PutUserProperty_EmptyKey_Throws()
    {
        var message = new Message();
        Assert.Throws<ArgumentNullException>(() => message.PutUserProperty("", "value"));
    }

    [Fact]
    [DisplayName("PutUserProperty_value为空_抛出ArgumentNullException")]
    public void PutUserProperty_EmptyValue_Throws()
    {
        var message = new Message();
        Assert.Throws<ArgumentNullException>(() => message.PutUserProperty("key", ""));
    }

    [Fact]
    [DisplayName("GetUserProperty_不存在的Key_返回null")]
    public void GetUserProperty_MissingKey_ReturnsNull()
    {
        var message = new Message();
        Assert.Null(message.GetUserProperty("nonexistent"));
    }

    [Fact]
    [DisplayName("Message_ReplyToClient属性读写正确")]
    public void Message_ReplyToClient_ReadWrite()
    {
        var message = new Message();
        Assert.Null(message.ReplyToClient);

        message.ReplyToClient = "client-001";
        Assert.Equal("client-001", message.ReplyToClient);
        Assert.Equal("client-001", message.Properties["REPLY_TO_CLIENT"]);
    }

    [Fact]
    [DisplayName("Message_CorrelationId属性读写正确")]
    public void Message_CorrelationId_ReadWrite()
    {
        var message = new Message();
        Assert.Null(message.CorrelationId);

        message.CorrelationId = "corr-abc-123";
        Assert.Equal("corr-abc-123", message.CorrelationId);
    }

    [Fact]
    [DisplayName("Message_MessageType属性读写正确")]
    public void Message_MessageType_ReadWrite()
    {
        var message = new Message();
        Assert.Null(message.MessageType);

        message.MessageType = "reply";
        Assert.Equal("reply", message.MessageType);
    }

    [Fact]
    [DisplayName("Message_RequestTimeout属性读写正确")]
    public void Message_RequestTimeout_ReadWrite()
    {
        var message = new Message();
        Assert.Equal(0, message.RequestTimeout);

        message.RequestTimeout = 5000;
        Assert.Equal(5000, message.RequestTimeout);
    }

    [Fact]
    [DisplayName("Message_TransactionId属性读写正确")]
    public void Message_TransactionId_ReadWrite()
    {
        var message = new Message();
        Assert.Null(message.TransactionId);

        message.TransactionId = "tx-0001";
        Assert.Equal("tx-0001", message.TransactionId);
    }

    [Fact]
    [DisplayName("Message_WaitStoreMsgOK_默认为true")]
    public void Message_WaitStoreMsgOK_DefaultTrue()
    {
        var message = new Message();
        Assert.True(message.WaitStoreMsgOK);
    }

    [Fact]
    [DisplayName("Message_DelayTimeLevel_默认为0")]
    public void Message_DelayTimeLevel_DefaultZero()
    {
        var message = new Message();
        Assert.Equal(0, message.DelayTimeLevel);
    }

    [Fact]
    [DisplayName("Message_ToString_返回消息体字符串")]
    public void Message_ToString_ReturnsBodyString()
    {
        var message = new Message();
        message.SetBody("Hello-ToString");
        Assert.Equal("Hello-ToString", message.ToString());
    }

    #endregion
}
