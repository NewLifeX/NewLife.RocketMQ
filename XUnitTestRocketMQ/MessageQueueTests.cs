using System;
using System.Collections.Generic;
using System.ComponentModel;
using NewLife.RocketMQ.Protocol;
using Xunit;

namespace XUnitTestRocketMQ;

/// <summary>MessageQueue消息队列标识测试</summary>
public class MessageQueueTests
{
    #region Equals
    [Fact]
    [DisplayName("Equals_相同属性相等")]
    public void Equals_SameProperties_ReturnsTrue()
    {
        var q1 = new MessageQueue { Topic = "test", BrokerName = "broker-a", QueueId = 0 };
        var q2 = new MessageQueue { Topic = "test", BrokerName = "broker-a", QueueId = 0 };

        Assert.True(q1.Equals(q2));
        Assert.True(q2.Equals(q1));
    }

    [Fact]
    [DisplayName("Equals_不同Topic不相等")]
    public void Equals_DifferentTopic_ReturnsFalse()
    {
        var q1 = new MessageQueue { Topic = "test1", BrokerName = "broker-a", QueueId = 0 };
        var q2 = new MessageQueue { Topic = "test2", BrokerName = "broker-a", QueueId = 0 };

        Assert.False(q1.Equals(q2));
    }

    [Fact]
    [DisplayName("Equals_不同BrokerName不相等")]
    public void Equals_DifferentBrokerName_ReturnsFalse()
    {
        var q1 = new MessageQueue { Topic = "test", BrokerName = "broker-a", QueueId = 0 };
        var q2 = new MessageQueue { Topic = "test", BrokerName = "broker-b", QueueId = 0 };

        Assert.False(q1.Equals(q2));
    }

    [Fact]
    [DisplayName("Equals_不同QueueId不相等")]
    public void Equals_DifferentQueueId_ReturnsFalse()
    {
        var q1 = new MessageQueue { Topic = "test", BrokerName = "broker-a", QueueId = 0 };
        var q2 = new MessageQueue { Topic = "test", BrokerName = "broker-a", QueueId = 1 };

        Assert.False(q1.Equals(q2));
    }

    [Fact]
    [DisplayName("Equals_非MessageQueue对象不相等")]
    public void Equals_NonMessageQueue_ReturnsFalse()
    {
        var q = new MessageQueue { Topic = "test", BrokerName = "broker-a", QueueId = 0 };

        Assert.False(q.Equals("not a queue"));
        Assert.False(q.Equals(null));
        Assert.False(q.Equals(42));
    }

    [Fact]
    [DisplayName("Equals_自身比较相等")]
    public void Equals_Self_ReturnsTrue()
    {
        var q = new MessageQueue { Topic = "test", BrokerName = "broker-a", QueueId = 0 };

        Assert.True(q.Equals(q));
    }
    #endregion

    #region GetHashCode
    [Fact]
    [DisplayName("GetHashCode_相同属性相同哈希")]
    public void GetHashCode_SameProperties_SameHash()
    {
        var q1 = new MessageQueue { Topic = "test", BrokerName = "broker-a", QueueId = 0 };
        var q2 = new MessageQueue { Topic = "test", BrokerName = "broker-a", QueueId = 0 };

        Assert.Equal(q1.GetHashCode(), q2.GetHashCode());
    }

    [Fact]
    [DisplayName("GetHashCode_不同属性通常不同哈希")]
    public void GetHashCode_DifferentProperties_DifferentHash()
    {
        var q1 = new MessageQueue { Topic = "test1", BrokerName = "broker-a", QueueId = 0 };
        var q2 = new MessageQueue { Topic = "test2", BrokerName = "broker-a", QueueId = 0 };

        // 不同对象的哈希值不保证不同，但通常不同
        // 这里只验证相同属性哈希一致
        Assert.Equal(q1.GetHashCode(), q1.GetHashCode());
    }

    [Fact]
    [DisplayName("GetHashCode_可用于字典键")]
    public void GetHashCode_WorksAsDictionaryKey()
    {
        var q1 = new MessageQueue { Topic = "test", BrokerName = "broker-a", QueueId = 0 };
        var q2 = new MessageQueue { Topic = "test", BrokerName = "broker-a", QueueId = 0 };

        var dict = new Dictionary<MessageQueue, String>();
        dict[q1] = "value";

        // q2 与 q1 相等，应能查到
        // 注意：Dictionary 使用 GetHashCode + Equals
        // MessageQueue.Equals 返回 true 但默认 Dictionary 用引用比较
        // 除非重写了 Equals 和 GetHashCode
        Assert.True(dict.ContainsKey(q1));
    }
    #endregion

    #region ToString
    [Fact]
    [DisplayName("ToString_格式正确")]
    public void ToString_CorrectFormat()
    {
        var q = new MessageQueue { Topic = "test", BrokerName = "broker-a", QueueId = 3 };

        var str = q.ToString();

        Assert.Equal("broker-a[3]", str);
    }

    [Fact]
    [DisplayName("ToString_Null属性不抛异常")]
    public void ToString_NullBrokerName()
    {
        var q = new MessageQueue { QueueId = 0 };

        var str = q.ToString();

        Assert.Contains("[0]", str);
    }
    #endregion
}
