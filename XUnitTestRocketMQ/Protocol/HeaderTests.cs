using System;
using System.Collections.Generic;
using System.ComponentModel;
using NewLife.RocketMQ.Protocol;
using Xunit;

namespace XUnitTestRocketMQ;

/// <summary>Header协议头测试</summary>
public class HeaderTests
{
    #region 默认值
    [Fact]
    [DisplayName("Header_默认Language为CPP")]
    public void Header_DefaultLanguage()
    {
        var header = new Header();

        Assert.Equal("CPP", header.Language);
    }

    [Fact]
    [DisplayName("Header_默认SerializeType为JSON")]
    public void Header_DefaultSerializeType()
    {
        var header = new Header();

        Assert.Equal("JSON", header.SerializeTypeCurrentRPC);
    }

    [Fact]
    [DisplayName("Header_默认Version为V4_8_0")]
    public void Header_DefaultVersion()
    {
        var header = new Header();

        Assert.Equal(MQVersion.V4_8_0, header.Version);
    }

    [Fact]
    [DisplayName("Header_默认ExtFields为null")]
    public void Header_DefaultExtFieldsNull()
    {
        var header = new Header();

        Assert.Null(header.ExtFields);
    }
    #endregion

    #region GetExtFields
    [Fact]
    [DisplayName("GetExtFields_首次调用创建空字典")]
    public void GetExtFields_CreatesNewDictionary()
    {
        var header = new Header();

        var fields = header.GetExtFields();

        Assert.NotNull(fields);
        Assert.Empty(fields);
    }

    [Fact]
    [DisplayName("GetExtFields_多次调用返回同一实例")]
    public void GetExtFields_ReturnsSameInstance()
    {
        var header = new Header();

        var fields1 = header.GetExtFields();
        var fields2 = header.GetExtFields();

        Assert.Same(fields1, fields2);
    }

    [Fact]
    [DisplayName("GetExtFields_已有ExtFields时返回原字典")]
    public void GetExtFields_ExistingFields_ReturnsSame()
    {
        var existing = new Dictionary<String, String> { ["key"] = "value" };
        var header = new Header { ExtFields = existing };

        var fields = header.GetExtFields();

        Assert.Same(existing, fields);
        Assert.Equal("value", fields["key"]);
    }

    [Fact]
    [DisplayName("GetExtFields_大小写不敏感")]
    public void GetExtFields_CaseInsensitive()
    {
        var header = new Header();

        var fields = header.GetExtFields();
        fields["TestKey"] = "hello";

        Assert.Equal("hello", fields["testkey"]);
        Assert.Equal("hello", fields["TESTKEY"]);
    }
    #endregion

    #region CreateException
    [Fact]
    [DisplayName("CreateException_基本异常创建")]
    public void CreateException_BasicCreation()
    {
        var header = new Header
        {
            Code = (Int32)ResponseCode.SYSTEM_ERROR,
            Remark = "Something went wrong"
        };

        var ex = header.CreateException();

        Assert.NotNull(ex);
        Assert.Equal(ResponseCode.SYSTEM_ERROR, ex.Code);
        Assert.Contains("Something went wrong", ex.Message);
    }

    [Fact]
    [DisplayName("CreateException_解析Exception后缀")]
    public void CreateException_ParsesExceptionSuffix()
    {
        var header = new Header
        {
            Code = (Int32)ResponseCode.TOPIC_NOT_EXIST,
            Remark = "org.apache.rocketmq.client.exception.MQClientException: No topic route info"
        };

        var ex = header.CreateException();

        Assert.Equal(ResponseCode.TOPIC_NOT_EXIST, ex.Code);
        Assert.Contains("No topic route info", ex.Message);
    }

    [Fact]
    [DisplayName("CreateException_解析逗号后文本")]
    public void CreateException_ParsesCommaDelimited()
    {
        var header = new Header
        {
            Code = (Int32)ResponseCode.SYSTEM_ERROR,
            Remark = "SomeException: Error message, extra info"
        };

        var ex = header.CreateException();

        Assert.Contains("Error message", ex.Message);
        Assert.DoesNotContain("extra info", ex.Message);
    }

    [Fact]
    [DisplayName("CreateException_空Remark不抛异常")]
    public void CreateException_EmptyRemark_NoThrow()
    {
        var header = new Header
        {
            Code = (Int32)ResponseCode.SUCCESS,
            Remark = null
        };

        var ex = header.CreateException();

        Assert.NotNull(ex);
        Assert.Equal(ResponseCode.SUCCESS, ex.Code);
    }

    [Fact]
    [DisplayName("CreateException_无Exception关键字保持原文")]
    public void CreateException_NoExceptionKeyword_KeepsOriginal()
    {
        var header = new Header
        {
            Code = (Int32)ResponseCode.NO_PERMISSION,
            Remark = "Access denied for this topic"
        };

        var ex = header.CreateException();

        Assert.Contains("Access denied for this topic", ex.Message);
    }
    #endregion
}
