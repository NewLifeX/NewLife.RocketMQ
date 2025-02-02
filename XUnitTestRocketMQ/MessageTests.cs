using NewLife;
using NewLife.RocketMQ.Protocol;
using Xunit;

namespace XUnitTestRocketMQ;

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
}
