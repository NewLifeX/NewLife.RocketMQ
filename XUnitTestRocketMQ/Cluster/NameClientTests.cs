using System;
using System.ComponentModel;
using Moq;
using NewLife;
using NewLife.Data;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Protocol;
using Xunit;

namespace XUnitTestRocketMQ;

public class NameClientTests
{
    [Fact]
    [DisplayName("GetRouteInfo_解析标准JSON格式的路由信息")]
    public void GetRouteInfo()
    {
        var target = """
            {"brokerDatas":[{"brokerAddrs":{"0":"10.2.3.117:10911"},"brokerName":"broker-a","cluster":"DefaultCluster","enableActingMaster":false}],"filterServerTable":{},"queueDatas":[{"brokerName":"broker-a","perm":6,"readQueueNums":8,"topicSysFlag":0,"writeQueueNums":8}]}
            """;

        var pb = new Producer();
        var nc = new Mock<NameClient>("clientId", pb) { CallBase = true };
        nc.Setup(e => e.Invoke(RequestCode.GET_ROUTEINTO_BY_TOPIC, null, It.IsAny<Object>(), false))
            .Returns(new Command { Payload = (ArrayPacket)target.GetBytes() });

        var client = nc.Object;
        var brokers = client.GetRouteInfo(null);
        Assert.Single(brokers);

        var broker = brokers[0];
        Assert.Equal("broker-a", broker.Name);
        Assert.Equal(Permissions.Read | Permissions.Write, broker.Permission);
        Assert.Equal(8, broker.ReadQueueNums);
        Assert.Equal(8, broker.WriteQueueNums);
        Assert.Equal(0, broker.TopicSynFlag);
        Assert.Equal("DefaultCluster", broker.Cluster);
        Assert.Single(broker.Addresses);
        Assert.Equal("10.2.3.117:10911", broker.Addresses[0]);
    }

    [Fact]
    [DisplayName("GetRouteInfo_解析整数Key的brokerAddrs格式")]
    public void GetRouteInfo2()
    {
        var target = """
            {"brokerDatas":[{"brokerAddrs":{0:"10.2.3.117:10911"},"brokerName":"broker-a","cluster":"DefaultCluster","enableActingMaster":false}],"filterServerTable":{},"queueDatas":[{"brokerName":"broker-a","perm":6,"readQueueNums":8,"topicSysFlag":0,"writeQueueNums":8}]}
            """;

        var pb = new Producer();
        var nc = new Mock<NameClient>("clientId", pb) { CallBase = true };
        nc.Setup(e => e.Invoke(RequestCode.GET_ROUTEINTO_BY_TOPIC, null, It.IsAny<Object>(), false))
            .Returns(new Command { Payload = (ArrayPacket)target.GetBytes() });

        var client = nc.Object;
        var brokers = client.GetRouteInfo(null);
        Assert.Single(brokers);

        var broker = brokers[0];
        Assert.Equal("broker-a", broker.Name);
        Assert.Equal(Permissions.Read | Permissions.Write, broker.Permission);
        Assert.Equal(8, broker.ReadQueueNums);
        Assert.Equal(8, broker.WriteQueueNums);
        Assert.Equal(0, broker.TopicSynFlag);
        Assert.Equal("DefaultCluster", broker.Cluster);
        Assert.Single(broker.Addresses);
        Assert.Equal("10.2.3.117:10911", broker.Addresses[0]);
    }

    [Fact]
    [DisplayName("GetRouteInfo_非零topicSysFlag正确解析")]
    public void GetRouteInfo_NonZeroTopicSysFlag()
    {
        var target = """
            {"brokerDatas":[{"brokerAddrs":{"0":"10.0.0.1:10911"},"brokerName":"broker-b","cluster":"TestCluster","enableActingMaster":false}],"filterServerTable":{},"queueDatas":[{"brokerName":"broker-b","perm":6,"readQueueNums":4,"topicSysFlag":3,"writeQueueNums":4}]}
            """;

        var pb = new Producer();
        var nc = new Mock<NameClient>("clientId", pb) { CallBase = true };
        nc.Setup(e => e.Invoke(RequestCode.GET_ROUTEINTO_BY_TOPIC, null, It.IsAny<Object>(), false))
            .Returns(new Command { Payload = (ArrayPacket)target.GetBytes() });

        var client = nc.Object;
        var brokers = client.GetRouteInfo("test_topic");
        Assert.Single(brokers);
        Assert.Equal(3, brokers[0].TopicSynFlag);
    }

    [Fact]
    [DisplayName("GetRouteInfo_指定topic时缓存路由信息可通过GetTopicBrokers获取")]
    public void GetRouteInfo_WithTopic_CachesResult()
    {
        var target = """
            {"brokerDatas":[{"brokerAddrs":{"0":"10.0.0.1:10911"},"brokerName":"broker-a","cluster":"DefaultCluster","enableActingMaster":false}],"filterServerTable":{},"queueDatas":[{"brokerName":"broker-a","perm":6,"readQueueNums":8,"topicSysFlag":0,"writeQueueNums":8}]}
            """;

        var pb = new Producer();
        var nc = new Mock<NameClient>("clientId", pb) { CallBase = true };
        nc.Setup(e => e.Invoke(RequestCode.GET_ROUTEINTO_BY_TOPIC, null, It.IsAny<Object>(), false))
            .Returns(new Command { Payload = (ArrayPacket)target.GetBytes() });

        var client = nc.Object;
        client.GetRouteInfo("my_topic");

        // 缓存应可通过 GetTopicBrokers 取回
        var cached = client.GetTopicBrokers("my_topic");
        Assert.NotNull(cached);
        Assert.Single(cached);
        Assert.Equal("broker-a", cached[0].Name);
    }

    [Fact]
    [DisplayName("GetRouteInfo_null_topic不缓存且不抛出异常")]
    public void GetRouteInfo_NullTopic_DoesNotThrow()
    {
        var target = """
            {"brokerDatas":[{"brokerAddrs":{"0":"10.0.0.1:10911"},"brokerName":"broker-a","cluster":"DefaultCluster","enableActingMaster":false}],"filterServerTable":{},"queueDatas":[{"brokerName":"broker-a","perm":6,"readQueueNums":8,"topicSysFlag":0,"writeQueueNums":8}]}
            """;

        var pb = new Producer();
        var nc = new Mock<NameClient>("clientId", pb) { CallBase = true };
        nc.Setup(e => e.Invoke(RequestCode.GET_ROUTEINTO_BY_TOPIC, null, It.IsAny<Object>(), false))
            .Returns(new Command { Payload = (ArrayPacket)target.GetBytes() });

        var client = nc.Object;
        // null topic 不应抛出 ArgumentNullException
        var brokers = client.GetRouteInfo(null);
        Assert.Single(brokers);
        Assert.Equal("broker-a", brokers[0].Name);
    }

    [Fact]
    [DisplayName("GetRouteInfo_Master地址排首位_Slave地址正确解析")]
    public void GetRouteInfo_MasterSlave_Addresses()
    {
        var target = """
            {"brokerDatas":[{"brokerAddrs":{"0":"10.0.0.1:10911","1":"10.0.0.2:10911"},"brokerName":"broker-a","cluster":"DefaultCluster","enableActingMaster":false}],"filterServerTable":{},"queueDatas":[{"brokerName":"broker-a","perm":6,"readQueueNums":8,"topicSysFlag":0,"writeQueueNums":8}]}
            """;

        var pb = new Producer();
        var nc = new Mock<NameClient>("clientId", pb) { CallBase = true };
        nc.Setup(e => e.Invoke(RequestCode.GET_ROUTEINTO_BY_TOPIC, null, It.IsAny<Object>(), false))
            .Returns(new Command { Payload = (ArrayPacket)target.GetBytes() });

        var client = nc.Object;
        var brokers = client.GetRouteInfo(null);
        Assert.Single(brokers);

        var broker = brokers[0];
        Assert.Equal("10.0.0.1:10911", broker.MasterAddress);
        Assert.Single(broker.SlaveAddresses);
        Assert.Equal("10.0.0.2:10911", broker.SlaveAddresses[0]);
        // Master 地址排在第一位
        Assert.Equal("10.0.0.1:10911", broker.Addresses[0]);
        Assert.Equal(2, broker.Addresses.Length);
        Assert.True(broker.IsMaster);
    }
}
