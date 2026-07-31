using System;
using System.ComponentModel;
using NewLife.RocketMQ;
using Xunit;

namespace XUnitTest.Models;

/// <summary>BrokerInfo代理信息测试</summary>
public class BrokerInfoTests
{
    #region 属性
    [Fact]
    [DisplayName("BrokerInfo_默认值")]
    public void BrokerInfo_Defaults()
    {
        var bi = new BrokerInfo();

        Assert.Null(bi.Name);
        Assert.Null(bi.Cluster);
        Assert.Null(bi.Addresses);
        Assert.Null(bi.MasterAddress);
        Assert.Null(bi.SlaveAddresses);
        Assert.False(bi.IsMaster);
    }

    [Fact]
    [DisplayName("BrokerInfo_设置所有属性")]
    public void BrokerInfo_SetAllProperties()
    {
        var bi = new BrokerInfo
        {
            Name = "broker-a",
            Cluster = "DefaultCluster",
            Addresses = ["10.0.0.1:10911", "10.0.0.2:10911"],
            MasterAddress = "10.0.0.1:10911",
            SlaveAddresses = ["10.0.0.2:10911"],
            Permission = Permissions.Read | Permissions.Write,
            ReadQueueNums = 4,
            WriteQueueNums = 4,
            TopicSynFlag = 0,
            IsMaster = true,
        };

        Assert.Equal("broker-a", bi.Name);
        Assert.Equal("DefaultCluster", bi.Cluster);
        Assert.Equal(2, bi.Addresses.Length);
        Assert.Equal("10.0.0.1:10911", bi.MasterAddress);
        Assert.Single(bi.SlaveAddresses);
        Assert.True(bi.IsMaster);
        Assert.Equal(4, bi.ReadQueueNums);
        Assert.Equal(4, bi.WriteQueueNums);
    }

    [Fact]
    [DisplayName("BrokerInfo_无Slave地址")]
    public void BrokerInfo_NoSlaves()
    {
        var bi = new BrokerInfo
        {
            Addresses = ["10.0.0.1:10911"],
            MasterAddress = "10.0.0.1:10911",
            SlaveAddresses = [],
        };

        Assert.Empty(bi.SlaveAddresses);
    }

    [Fact]
    [DisplayName("BrokerInfo_IsMaster标志")]
    public void BrokerInfo_IsMasterFlag()
    {
        var bi1 = new BrokerInfo { IsMaster = true };
        var bi2 = new BrokerInfo { IsMaster = false };

        Assert.True(bi1.IsMaster);
        Assert.False(bi2.IsMaster);
    }
    #endregion

    #region 相等比较
    [Fact]
    [DisplayName("BrokerInfo_相同属性相等")]
    public void BrokerInfo_SameProperties_Equal()
    {
        var bi1 = new BrokerInfo { Name = "broker-a", Addresses = ["10.0.0.1:10911"], ReadQueueNums = 4, WriteQueueNums = 4 };
        var bi2 = new BrokerInfo { Name = "broker-a", Addresses = ["10.0.0.1:10911"], ReadQueueNums = 4, WriteQueueNums = 4 };

        Assert.True(bi1.Equals(bi2));
    }

    [Fact]
    [DisplayName("BrokerInfo_不同Name不相等")]
    public void BrokerInfo_DifferentName_NotEqual()
    {
        var bi1 = new BrokerInfo { Name = "broker-a", Addresses = ["10.0.0.1:10911"] };
        var bi2 = new BrokerInfo { Name = "broker-b", Addresses = ["10.0.0.1:10911"] };

        Assert.False(bi1.Equals(bi2));
    }

    [Fact]
    [DisplayName("BrokerInfo_与非BrokerInfo不相等")]
    public void BrokerInfo_NonBrokerInfo_NotEqual()
    {
        var bi = new BrokerInfo { Name = "broker-a" };

        Assert.False(bi.Equals("broker-a"));
        Assert.False(bi.Equals(null));
    }

    [Fact]
    [DisplayName("BrokerInfo_相同属性哈希相同")]
    public void BrokerInfo_SameProperties_SameHash()
    {
        var bi1 = new BrokerInfo { Name = "broker-a", Addresses = ["10.0.0.1:10911"], ReadQueueNums = 4, WriteQueueNums = 4 };
        var bi2 = new BrokerInfo { Name = "broker-a", Addresses = ["10.0.0.1:10911"], ReadQueueNums = 4, WriteQueueNums = 4 };

        // 相等性契约：Equals 为 true 的两个实例，GetHashCode 必须相同（Addresses 是不同数组实例也应相同）
        Assert.True(bi1.Equals(bi2));
        Assert.Equal(bi1.GetHashCode(), bi2.GetHashCode());
    }

    [Fact]
    [DisplayName("BrokerInfo_空字段哈希安全")]
    public void BrokerInfo_NullFields_HashSafe()
    {
        var bi = new BrokerInfo { Name = null, Addresses = null };

        // 不抛异常即可（旧实现 Addresses.GetHashCode() 会 NRE）
        _ = bi.GetHashCode();
    }

    [Fact]
    [DisplayName("Permissions_读写标记")]
    public void Permissions_ReadWriteFlags()
    {
        Assert.Equal(2, (Int32)Permissions.Write);
        Assert.Equal(4, (Int32)Permissions.Read);

        var rw = Permissions.Read | Permissions.Write;
        Assert.True(rw.HasFlag(Permissions.Read));
        Assert.True(rw.HasFlag(Permissions.Write));
    }
    #endregion
}
