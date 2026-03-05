using System;
using System.Collections.Generic;
using System.ComponentModel;
using NewLife.RocketMQ.Protocol;
using Xunit;

namespace XUnitTestRocketMQ;

/// <summary>SendResult发送结果测试</summary>
public class SendResultTests
{
    #region Read方法
    [Fact]
    [DisplayName("Read_解析所有字段")]
    public void Read_ParsesAllFields()
    {
        var dic = new Dictionary<String, String>
        {
            ["MsgId"] = "0A0A0A0A00002A9F0000000000000001",
            ["OffsetMsgId"] = "0A0A0A0A00002A9F0000000000000002",
            ["QueueOffset"] = "12345",
            ["TransactionId"] = "TX001",
            ["RegionId"] = "DefaultRegion",
        };

        var result = new SendResult();
        result.Read(dic);

        Assert.Equal("0A0A0A0A00002A9F0000000000000001", result.MsgId);
        Assert.Equal("0A0A0A0A00002A9F0000000000000002", result.OffsetMsgId);
        Assert.Equal(12345, result.QueueOffset);
        Assert.Equal("TX001", result.TransactionId);
        Assert.Equal("DefaultRegion", result.RegionId);
    }

    [Fact]
    [DisplayName("Read_MSG_REGION设置RegionId")]
    public void Read_MsgRegion_SetsRegionId()
    {
        var dic = new Dictionary<String, String>
        {
            ["MSG_REGION"] = "us-east-1",
        };

        var result = new SendResult();
        result.Read(dic);

        Assert.Equal("us-east-1", result.RegionId);
    }

    [Fact]
    [DisplayName("Read_Null字典不抛异常")]
    public void Read_NullDictionary_NoException()
    {
        var result = new SendResult();
        result.Read(null);

        Assert.Null(result.MsgId);
        Assert.Equal(0, result.QueueOffset);
    }

    [Fact]
    [DisplayName("Read_空字典不影响属性")]
    public void Read_EmptyDictionary_NoEffect()
    {
        var result = new SendResult();
        result.Read(new Dictionary<String, String>());

        Assert.Null(result.MsgId);
        Assert.Null(result.OffsetMsgId);
        Assert.Equal(0, result.QueueOffset);
    }

    [Fact]
    [DisplayName("Read_部分字段解析")]
    public void Read_PartialFields()
    {
        var dic = new Dictionary<String, String>
        {
            ["MsgId"] = "ABC123",
        };

        var result = new SendResult();
        result.Read(dic);

        Assert.Equal("ABC123", result.MsgId);
        Assert.Null(result.OffsetMsgId);
        Assert.Null(result.TransactionId);
    }

    [Fact]
    [DisplayName("Read_大小写不敏感")]
    public void Read_CaseInsensitive()
    {
        var dic = new Dictionary<String, String>
        {
            ["msgid"] = "LOWER_ID",
            ["QUEUEOFFSET"] = "999",
        };

        var result = new SendResult();
        result.Read(dic);

        Assert.Equal("LOWER_ID", result.MsgId);
        Assert.Equal(999, result.QueueOffset);
    }
    #endregion

    #region ToString
    [Fact]
    [DisplayName("ToString_包含所有关键信息")]
    public void ToString_ContainsAllInfo()
    {
        var result = new SendResult
        {
            Status = SendStatus.SendOK,
            MsgId = "MSG001",
            OffsetMsgId = "OFFSET001",
            QueueOffset = 42,
            Queue = new MessageQueue { BrokerName = "broker-a", QueueId = 3 }
        };

        var str = result.ToString();

        Assert.Contains("SendOK", str);
        Assert.Contains("MSG001", str);
        Assert.Contains("OFFSET001", str);
        Assert.Contains("42", str);
    }
    #endregion

    #region 属性
    [Fact]
    [DisplayName("SendStatus_枚举值正确")]
    public void SendStatus_EnumValues()
    {
        Assert.Equal(0, (Int32)SendStatus.SendOK);
        Assert.Equal(1, (Int32)SendStatus.FlushDiskTimeout);
        Assert.Equal(2, (Int32)SendStatus.FlushSlaveTimeout);
        Assert.Equal(3, (Int32)SendStatus.SlaveNotAvailable);
        Assert.Equal(4, (Int32)SendStatus.SendError);
    }
    #endregion
}
