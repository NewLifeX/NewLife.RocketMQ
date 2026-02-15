using System;
using System.ComponentModel;
using System.Linq;
using NewLife.RocketMQ;
using Xunit;

namespace XUnitTestRocketMQ;

/// <summary>Broker主从切换测试</summary>
public class BrokerFailoverTests
{
    [Fact]
    [DisplayName("BrokerInfo_MasterAddress属性")]
    public void BrokerInfo_MasterAddress()
    {
        var info = new BrokerInfo
        {
            Name = "broker-a",
            MasterAddress = "192.168.1.1:10911",
            SlaveAddresses = ["192.168.1.2:10911"],
            Addresses = ["192.168.1.1:10911", "192.168.1.2:10911"],
            IsMaster = true,
        };

        Assert.Equal("192.168.1.1:10911", info.MasterAddress);
        Assert.Single(info.SlaveAddresses);
        Assert.Equal("192.168.1.2:10911", info.SlaveAddresses[0]);
    }

    [Fact]
    [DisplayName("BrokerInfo_无Slave时SlaveAddresses为空")]
    public void BrokerInfo_NoSlave()
    {
        var info = new BrokerInfo
        {
            Name = "broker-a",
            MasterAddress = "192.168.1.1:10911",
            SlaveAddresses = [],
            Addresses = ["192.168.1.1:10911"],
            IsMaster = true,
        };

        Assert.Empty(info.SlaveAddresses);
    }

    [Fact]
    [DisplayName("BrokerInfo_Addresses以Master在前")]
    public void BrokerInfo_MasterFirst()
    {
        var info = new BrokerInfo
        {
            Name = "broker-a",
            MasterAddress = "192.168.1.1:10911",
            SlaveAddresses = ["192.168.1.2:10911", "192.168.1.3:10911"],
            Addresses = ["192.168.1.1:10911", "192.168.1.2:10911", "192.168.1.3:10911"],
            IsMaster = true,
        };

        Assert.Equal("192.168.1.1:10911", info.Addresses[0]);
        Assert.Equal(3, info.Addresses.Length);
    }

    [Fact]
    [DisplayName("BrokerInfo_IsMaster标记")]
    public void BrokerInfo_IsMaster_Flag()
    {
        var masterInfo = new BrokerInfo { Name = "broker-a", IsMaster = true };
        var slaveInfo = new BrokerInfo { Name = "broker-b", IsMaster = false };

        Assert.True(masterInfo.IsMaster);
        Assert.False(slaveInfo.IsMaster);
    }
}
