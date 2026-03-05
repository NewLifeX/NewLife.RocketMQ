using System;
using System.Collections.Generic;
using System.ComponentModel;
using NewLife.RocketMQ.Protocol;
using Xunit;

namespace XUnitTestRocketMQ;

/// <summary>PullResult拉取结果测试</summary>
public class PullResultTests
{
    #region Read方法
    [Fact]
    [DisplayName("Read_解析所有偏移字段")]
    public void Read_ParsesAllOffsetFields()
    {
        var dic = new Dictionary<String, String>
        {
            ["MinOffset"] = "100",
            ["MaxOffset"] = "9999",
            ["NextBeginOffset"] = "500",
        };

        var result = new PullResult();
        result.Read(dic);

        Assert.Equal(100, result.MinOffset);
        Assert.Equal(9999, result.MaxOffset);
        Assert.Equal(500, result.NextBeginOffset);
    }

    [Fact]
    [DisplayName("Read_Null字典不抛异常")]
    public void Read_NullDictionary_NoException()
    {
        var result = new PullResult();
        result.Read(null);

        Assert.Equal(0, result.MinOffset);
        Assert.Equal(0, result.MaxOffset);
        Assert.Equal(0, result.NextBeginOffset);
    }

    [Fact]
    [DisplayName("Read_空字典不影响属性")]
    public void Read_EmptyDictionary_NoEffect()
    {
        var result = new PullResult();
        result.Read(new Dictionary<String, String>());

        Assert.Equal(0, result.MinOffset);
    }

    [Fact]
    [DisplayName("Read_部分字段解析")]
    public void Read_PartialFields()
    {
        var dic = new Dictionary<String, String>
        {
            ["MaxOffset"] = "1000",
        };

        var result = new PullResult();
        result.Read(dic);

        Assert.Equal(0, result.MinOffset);
        Assert.Equal(1000, result.MaxOffset);
        Assert.Equal(0, result.NextBeginOffset);
    }

    [Fact]
    [DisplayName("Read_大小写不敏感")]
    public void Read_CaseInsensitive()
    {
        var dic = new Dictionary<String, String>
        {
            ["minoffset"] = "50",
            ["MAXOFFSET"] = "200",
        };

        var result = new PullResult();
        result.Read(dic);

        Assert.Equal(50, result.MinOffset);
        Assert.Equal(200, result.MaxOffset);
    }
    #endregion

    #region ToString
    [Fact]
    [DisplayName("ToString_包含状态和偏移信息")]
    public void ToString_ContainsStatusAndOffsets()
    {
        var result = new PullResult
        {
            Status = PullStatus.Found,
            MinOffset = 10,
            MaxOffset = 100,
            Messages = [new MessageExt(), new MessageExt()]
        };

        var str = result.ToString();

        Assert.Contains("Found", str);
        Assert.Contains("10", str);
        Assert.Contains("100", str);
        Assert.Contains("2", str);
    }

    [Fact]
    [DisplayName("ToString_无消息时显示0")]
    public void ToString_NullMessages_ShowsZero()
    {
        var result = new PullResult { Status = PullStatus.NoNewMessage };

        var str = result.ToString();

        Assert.Contains("NoNewMessage", str);
        Assert.Contains("0", str);
    }
    #endregion

    #region 枚举
    [Fact]
    [DisplayName("PullStatus_枚举值正确")]
    public void PullStatus_EnumValues()
    {
        Assert.Equal(0, (Int32)PullStatus.Found);
        Assert.Equal(1, (Int32)PullStatus.NoNewMessage);
        Assert.Equal(2, (Int32)PullStatus.NoMatchedMessage);
        Assert.Equal(3, (Int32)PullStatus.OffsetIllegal);
        Assert.Equal(4, (Int32)PullStatus.Unknown);
    }
    #endregion
}
