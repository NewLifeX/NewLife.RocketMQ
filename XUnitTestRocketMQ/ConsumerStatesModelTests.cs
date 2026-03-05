using System;
using System.ComponentModel;
using NewLife.RocketMQ.Protocol.ConsumerStates;
using Xunit;

namespace XUnitTestRocketMQ;

/// <summary>消费者状态模型测试</summary>
public class ConsumerStatesModelTests
{
    #region ConsumerStatesModel
    [Fact]
    [DisplayName("ConsumerStatesModel_默认值")]
    public void ConsumerStatesModel_Defaults()
    {
        var model = new ConsumerStatesModel();

        Assert.Equal(0, model.ConsumeTps);
        Assert.Null(model.OffsetTable);
    }

    [Fact]
    [DisplayName("ConsumerStatesModel_设置属性")]
    public void ConsumerStatesModel_SetProperties()
    {
        var mqModel = new MessageQueueModel { BrokerName = "broker-a", QueueId = 0 };
        var model = new ConsumerStatesModel
        {
            ConsumeTps = 1500.5,
            OffsetTable = new System.Collections.Generic.Dictionary<MessageQueueModel, OffsetWrapperModel>
            {
                [mqModel] = new OffsetWrapperModel { BrokerOffset = 100, ConsumerOffset = 90 }
            }
        };

        Assert.Equal(1500.5, model.ConsumeTps);
        Assert.NotNull(model.OffsetTable);
        Assert.Single(model.OffsetTable);
        Assert.Equal(100, model.OffsetTable[mqModel].BrokerOffset);
    }
    #endregion

    #region MessageQueueModel
    [Fact]
    [DisplayName("MessageQueueModel_默认值")]
    public void MessageQueueModel_Defaults()
    {
        var model = new MessageQueueModel();

        Assert.Null(model.BrokerName);
        Assert.Equal(0, model.QueueId);
        Assert.Null(model.Topic);
    }

    [Fact]
    [DisplayName("MessageQueueModel_设置属性")]
    public void MessageQueueModel_SetProperties()
    {
        var model = new MessageQueueModel
        {
            BrokerName = "broker-a",
            QueueId = 3,
            Topic = "test_topic",
        };

        Assert.Equal("broker-a", model.BrokerName);
        Assert.Equal(3, model.QueueId);
        Assert.Equal("test_topic", model.Topic);
    }
    #endregion

    #region OffsetWrapperModel
    [Fact]
    [DisplayName("OffsetWrapperModel_默认值")]
    public void OffsetWrapperModel_Defaults()
    {
        var model = new OffsetWrapperModel();

        Assert.Equal(0, model.BrokerOffset);
        Assert.Equal(0, model.ConsumerOffset);
        Assert.Equal(0, model.LastTimestamp);
        Assert.Equal(0, model.PullOffset);
    }

    [Fact]
    [DisplayName("OffsetWrapperModel_设置所有属性")]
    public void OffsetWrapperModel_SetProperties()
    {
        var model = new OffsetWrapperModel
        {
            BrokerOffset = 1000,
            ConsumerOffset = 900,
            LastTimestamp = 1234567890L,
            PullOffset = 950,
        };

        Assert.Equal(1000, model.BrokerOffset);
        Assert.Equal(900, model.ConsumerOffset);
        Assert.Equal(1234567890L, model.LastTimestamp);
        Assert.Equal(950, model.PullOffset);
    }

    [Fact]
    [DisplayName("OffsetWrapperModel_偏移差计算")]
    public void OffsetWrapperModel_OffsetDifference()
    {
        var model = new OffsetWrapperModel
        {
            BrokerOffset = 1000,
            ConsumerOffset = 800,
        };

        var diff = model.BrokerOffset - model.ConsumerOffset;
        Assert.Equal(200, diff);
    }
    #endregion
}
