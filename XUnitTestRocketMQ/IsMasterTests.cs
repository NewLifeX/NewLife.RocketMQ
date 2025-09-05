using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Moq;
using NewLife;
using NewLife.Data;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Protocol;
using Xunit;

namespace XUnitTestRocketMQ;

/// <summary>测试 IsMaster 标记功能，确保 Producer 仅向 Master 节点发送消息</summary>
public class IsMasterTests
{
    [Fact(DisplayName = "NameClient 应正确设置 Master 节点的 IsMaster=true")]
    public void NameClient_Should_Set_IsMaster_True_When_Key_Is_Zero()
    {
        // 模拟双主双备场景：broker-a 和 broker-b 都有主备节点
        var target = """
            {
                "brokerDatas": [
                    {
                        "brokerAddrs": {"0": "10.2.3.117:10911", "1": "10.2.3.118:10911"},
                        "brokerName": "broker-a",
                        "cluster": "DefaultCluster"
                    },
                    {
                        "brokerAddrs": {"0": "10.2.3.119:10911", "1": "10.2.3.120:10911"},
                        "brokerName": "broker-b", 
                        "cluster": "DefaultCluster"
                    }
                ],
                "queueDatas": [
                    {"brokerName": "broker-a", "perm": 6, "readQueueNums": 4, "writeQueueNums": 4},
                    {"brokerName": "broker-b", "perm": 6, "readQueueNums": 4, "writeQueueNums": 4}
                ]
            }
            """;

        var pb = new Producer();
        var nc = new Mock<NameClient>("clientId", pb) { CallBase = true };
        nc.Setup(e => e.Invoke(RequestCode.GET_ROUTEINTO_BY_TOPIC, null, It.IsAny<Object>(), false))
            .Returns(new Command { Payload = (ArrayPacket)target.GetBytes() });

        var client = nc.Object;
        var brokers = client.GetRouteInfo("test_topic");
        
        Assert.Equal(2, brokers.Count);

        // 验证两个 broker 都被标记为 Master（因为都有 key=0）
        var brokerA = brokers.FirstOrDefault(b => b.Name == "broker-a");
        var brokerB = brokers.FirstOrDefault(b => b.Name == "broker-b");
        
        Assert.NotNull(brokerA);
        Assert.NotNull(brokerB);
        Assert.True(brokerA.IsMaster, "broker-a 应该被标记为 Master");
        Assert.True(brokerB.IsMaster, "broker-b 应该被标记为 Master");
        
        // 验证地址包含主备地址
        Assert.Equal(2, brokerA.Addresses.Length);
        Assert.Equal(2, brokerB.Addresses.Length);
        Assert.Contains("10.2.3.117:10911", brokerA.Addresses);
        Assert.Contains("10.2.3.118:10911", brokerA.Addresses);
    }

    [Fact(DisplayName = "NameClient 应正确设置 Slave 节点的 IsMaster=false")]
    public void NameClient_Should_Set_IsMaster_False_When_No_Key_Zero()
    {
        // 模拟只有备节点的场景
        var target = """
            {
                "brokerDatas": [
                    {
                        "brokerAddrs": {"1": "10.2.3.118:10911", "2": "10.2.3.121:10911"},
                        "brokerName": "broker-slave",
                        "cluster": "DefaultCluster"
                    }
                ],
                "queueDatas": [
                    {"brokerName": "broker-slave", "perm": 4, "readQueueNums": 4, "writeQueueNums": 0}
                ]
            }
            """;

        var pb = new Producer();
        var nc = new Mock<NameClient>("clientId", pb) { CallBase = true };
        nc.Setup(e => e.Invoke(RequestCode.GET_ROUTEINTO_BY_TOPIC, null, It.IsAny<Object>(), false))
            .Returns(new Command { Payload = (ArrayPacket)target.GetBytes() });

        var client = nc.Object;
        var brokers = client.GetRouteInfo("test_topic");
        
        Assert.Single(brokers);
        var broker = brokers[0];
        
        Assert.False(broker.IsMaster, "没有 key=0 的 broker 应该被标记为 Slave");
        Assert.Equal("broker-slave", broker.Name);
        Assert.Equal(2, broker.Addresses.Length);
    }

    [Fact(DisplayName = "BrokerInfo 的 IsMaster 属性应正确工作")]
    public void BrokerInfo_IsMaster_Property_Should_Work_Correctly()
    {
        var masterBroker = new BrokerInfo
        {
            Name = "test-master",
            IsMaster = true
        };
        
        var slaveBroker = new BrokerInfo
        {
            Name = "test-slave", 
            IsMaster = false
        };

        Assert.True(masterBroker.IsMaster);
        Assert.False(slaveBroker.IsMaster);
        
        // 测试默认值
        var defaultBroker = new BrokerInfo();
        Assert.False(defaultBroker.IsMaster); // Boolean 默认值应该是 false
    }

    [Fact(DisplayName = "BrokerInfo.Equals 应考虑 IsMaster 状态以正确检测主备切换")]
    public void BrokerInfo_Equals_Should_Consider_IsMaster_For_Failover_Detection()
    {
        // 创建两个相同的 broker，但主备状态不同
        var masterBroker = new BrokerInfo
        {
            Name = "test-broker",
            Cluster = "test-cluster",
            Addresses = new[] { "192.168.1.10:10911", "192.168.1.11:10911" },
            Permission = Permissions.Read | Permissions.Write,
            ReadQueueNums = 4,
            WriteQueueNums = 4,
            TopicSynFlag = 0,
            IsMaster = true
        };

        var slaveBroker = new BrokerInfo
        {
            Name = "test-broker",
            Cluster = "test-cluster", 
            Addresses = new[] { "192.168.1.10:10911", "192.168.1.11:10911" },
            Permission = Permissions.Read | Permissions.Write,
            ReadQueueNums = 4,
            WriteQueueNums = 4,
            TopicSynFlag = 0,
            IsMaster = false
        };

        // 主备状态不同的 broker 应该被认为是"不相等"的，以确保主备切换时能触发重新平衡
        Assert.False(masterBroker.Equals(slaveBroker), "主备状态不同的 broker 应该被认为不相等");
        
        // 测试相同状态的 broker 应该相等
        var anotherMasterBroker = new BrokerInfo
        {
            Name = "test-broker",
            Cluster = "test-cluster",
            Addresses = new[] { "192.168.1.10:10911", "192.168.1.11:10911" },
            Permission = Permissions.Read | Permissions.Write,
            ReadQueueNums = 4,
            WriteQueueNums = 4,
            TopicSynFlag = 0,
            IsMaster = true
        };
        
        Assert.True(masterBroker.Equals(anotherMasterBroker), "相同状态的 broker 应该相等");
    }
}