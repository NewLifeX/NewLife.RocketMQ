using System;
using System.ComponentModel;
using NewLife.RocketMQ.Protocol;
using Xunit;

namespace XUnitTestRocketMQ;

/// <summary>ResponseException响应异常测试</summary>
public class ResponseExceptionTests
{
    [Fact]
    [DisplayName("构造函数_设置Code和Message")]
    public void Constructor_SetsCodeAndMessage()
    {
        var ex = new ResponseException(ResponseCode.SYSTEM_ERROR, "test error");

        Assert.Equal(ResponseCode.SYSTEM_ERROR, ex.Code);
        Assert.Contains("SYSTEM_ERROR", ex.Message);
        Assert.Contains("test error", ex.Message);
    }

    [Fact]
    [DisplayName("构造函数_不同ResponseCode")]
    public void Constructor_DifferentCodes()
    {
        var ex1 = new ResponseException(ResponseCode.SUCCESS, "ok");
        var ex2 = new ResponseException(ResponseCode.TOPIC_NOT_EXIST, "topic missing");
        var ex3 = new ResponseException(ResponseCode.NO_PERMISSION, "denied");

        Assert.Equal(ResponseCode.SUCCESS, ex1.Code);
        Assert.Equal(ResponseCode.TOPIC_NOT_EXIST, ex2.Code);
        Assert.Equal(ResponseCode.NO_PERMISSION, ex3.Code);
    }

    [Fact]
    [DisplayName("异常可以被捕获")]
    public void Exception_CanBeCaught()
    {
        try
        {
            throw new ResponseException(ResponseCode.SYSTEM_ERROR, "Test");
        }
        catch (ResponseException ex)
        {
            Assert.Equal(ResponseCode.SYSTEM_ERROR, ex.Code);
            return;
        }

        Assert.Fail("异常未被捕获");
    }

    [Fact]
    [DisplayName("异常继承自Exception")]
    public void Exception_InheritsFromException()
    {
        var ex = new ResponseException(ResponseCode.SUCCESS, "msg");

        Assert.IsAssignableFrom<Exception>(ex);
    }

    [Fact]
    [DisplayName("Null消息不抛异常")]
    public void Constructor_NullMessage_NoThrow()
    {
        var ex = new ResponseException(ResponseCode.SUCCESS, null);

        Assert.Equal(ResponseCode.SUCCESS, ex.Code);
        Assert.NotNull(ex.Message);
    }
}
