using System;
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
}
