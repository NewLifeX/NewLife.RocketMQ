using System;
using System.ComponentModel;
using NewLife.RocketMQ.Protocol;
using Xunit;

namespace XUnitTestRocketMQ;

/// <summary>Message扩展属性测试（补充覆盖）</summary>
public class MessageExtendedTests
{
    #region Request-Reply属性
    [Fact]
    [DisplayName("Message_ReplyToClient属性读写")]
    public void Message_ReplyToClient()
    {
        var msg = new Message();
        Assert.Null(msg.ReplyToClient);

        msg.ReplyToClient = "client-001";
        Assert.Equal("client-001", msg.ReplyToClient);
    }

    [Fact]
    [DisplayName("Message_CorrelationId属性读写")]
    public void Message_CorrelationId()
    {
        var msg = new Message();
        Assert.Null(msg.CorrelationId);

        msg.CorrelationId = "CID-001";
        Assert.Equal("CID-001", msg.CorrelationId);
    }

    [Fact]
    [DisplayName("Message_MessageType属性读写")]
    public void Message_MessageType()
    {
        var msg = new Message();
        Assert.Null(msg.MessageType);

        msg.MessageType = "reply";
        Assert.Equal("reply", msg.MessageType);
    }

    [Fact]
    [DisplayName("Message_RequestTimeout属性读写")]
    public void Message_RequestTimeout()
    {
        var msg = new Message();
        Assert.Equal(0, msg.RequestTimeout);

        msg.RequestTimeout = 5000;
        Assert.Equal(5000, msg.RequestTimeout);
    }
    #endregion

    #region DelayTimeLevel
    [Fact]
    [DisplayName("Message_DelayTimeLevel属性读写")]
    public void Message_DelayTimeLevel()
    {
        var msg = new Message();
        Assert.Equal(0, msg.DelayTimeLevel);

        msg.DelayTimeLevel = 3;
        Assert.Equal(3, msg.DelayTimeLevel);
    }
    #endregion

    #region WaitStoreMsgOK
    [Fact]
    [DisplayName("Message_WaitStoreMsgOK默认为true")]
    public void Message_WaitStoreMsgOK_DefaultTrue()
    {
        var msg = new Message();
        Assert.True(msg.WaitStoreMsgOK);
    }

    [Fact]
    [DisplayName("Message_WaitStoreMsgOK可设置为false")]
    public void Message_WaitStoreMsgOK_SetFalse()
    {
        var msg = new Message { WaitStoreMsgOK = false };
        Assert.False(msg.WaitStoreMsgOK);
    }
    #endregion

    #region TransactionId
    [Fact]
    [DisplayName("Message_TransactionId属性读写")]
    public void Message_TransactionId()
    {
        var msg = new Message();
        Assert.Null(msg.TransactionId);

        msg.TransactionId = "TX-001";
        Assert.Equal("TX-001", msg.TransactionId);
    }
    #endregion

    #region PutUserProperty / GetUserProperty
    [Fact]
    [DisplayName("PutUserProperty_存入自定义属性")]
    public void PutUserProperty_StoresProperty()
    {
        var msg = new Message();
        msg.PutUserProperty("orderId", "12345");

        Assert.Equal("12345", msg.GetUserProperty("orderId"));
    }

    [Fact]
    [DisplayName("PutUserProperty_Key为null时抛异常")]
    public void PutUserProperty_NullKey_ThrowsException()
    {
        var msg = new Message();
        Assert.Throws<ArgumentNullException>(() => msg.PutUserProperty(null, "value"));
    }

    [Fact]
    [DisplayName("PutUserProperty_Value为null时抛异常")]
    public void PutUserProperty_NullValue_ThrowsException()
    {
        var msg = new Message();
        Assert.Throws<ArgumentNullException>(() => msg.PutUserProperty("key", null));
    }

    [Fact]
    [DisplayName("GetUserProperty_不存在的Key返回null")]
    public void GetUserProperty_NonExistentKey_ReturnsNull()
    {
        var msg = new Message();
        Assert.Null(msg.GetUserProperty("nonexistent"));
    }
    #endregion

    #region SetBody
    [Fact]
    [DisplayName("SetBody_设置对象自动JSON序列化")]
    public void SetBody_Object_SerializesToJson()
    {
        var msg = new Message();
        msg.SetBody(new { Name = "test", Value = 42 });

        Assert.NotNull(msg.Body);
        Assert.Contains("test", msg.BodyString);
        Assert.Contains("42", msg.BodyString);
    }

    [Fact]
    [DisplayName("SetBody_Byte数组直接设置")]
    public void SetBody_ByteArray_SetDirectly()
    {
        var msg = new Message();
        var data = new Byte[] { 1, 2, 3 };
        msg.SetBody(data);

        Assert.Equal(data, msg.Body);
    }
    #endregion

    #region ToString
    [Fact]
    [DisplayName("Message_ToString有Body时返回BodyString")]
    public void Message_ToString_WithBody()
    {
        var msg = new Message();
        msg.SetBody("Hello");

        Assert.Equal("Hello", msg.ToString());
    }

    [Fact]
    [DisplayName("Message_ToString无Body时返回类型名")]
    public void Message_ToString_WithoutBody()
    {
        var msg = new Message();

        var str = msg.ToString();
        Assert.NotNull(str);
    }
    #endregion

    #region GetProperties序列化
    [Fact]
    [DisplayName("GetProperties_多个属性序列化为分隔字符串")]
    public void GetProperties_MultipleProperties()
    {
        var msg = new Message
        {
            Tags = "TagA",
            Keys = "Key1"
        };

        var props = msg.GetProperties();

        Assert.NotNull(props);
        Assert.Contains("TAGS", props);
        Assert.Contains("TagA", props);
        Assert.Contains("KEYS", props);
        Assert.Contains("Key1", props);
    }

    [Fact]
    [DisplayName("GetProperties_空属性返回空字符串")]
    public void GetProperties_EmptyProperties()
    {
        var msg = new Message();

        var props = msg.GetProperties();

        Assert.NotNull(props);
        Assert.Equal(String.Empty, props);
    }
    #endregion

    #region ParseProperties
    [Fact]
    [DisplayName("ParseProperties_解析标签和键")]
    public void ParseProperties_ParsesTagsAndKeys()
    {
        var msg = new Message();
        msg.ParseProperties("TAGS\u0001TagA\u0002KEYS\u0001Key1\u0002");

        Assert.Equal("TagA", msg.Tags);
        Assert.Equal("Key1", msg.Keys);
    }

    [Fact]
    [DisplayName("ParseProperties_解析延迟等级")]
    public void ParseProperties_ParsesDelayLevel()
    {
        var msg = new Message();
        msg.ParseProperties("DELAY\u00013\u0002");

        Assert.Equal(3, msg.DelayTimeLevel);
    }

    [Fact]
    [DisplayName("ParseProperties_空字符串返回原Properties")]
    public void ParseProperties_EmptyString_ReturnsOriginal()
    {
        var msg = new Message();
        msg.PutUserProperty("existing", "value");

        var result = msg.ParseProperties("");

        Assert.Equal("value", msg.GetUserProperty("existing"));
    }
    #endregion
}
