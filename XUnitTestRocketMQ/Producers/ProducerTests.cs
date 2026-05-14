using NewLife.Log;
using NewLife.RocketMQ;
using Xunit;
using XUnitTest.Integration;

namespace XUnitTest.Producers;

public class ProducerTests
{
    [Fact]
    [System.ComponentModel.DisplayName("Producer_创建主题_返回队列数")]
    public void CreateTopic()
    {
        var set = BasicTest.GetConfig();
        var mq = new Producer
        {
            //Topic = "nx_test",
            NameServerAddress = set.NameServer,

            Log = XTrace.Log,
        };

        mq.Start();

        // 创建topic时，start前不能指定topic，让其使用默认TBW102
        Assert.Equal("TBW102", mq.Topic);

        var rs = mq.CreateTopic("nx_test", 2);
        Assert.True(rs > 0);
    }

    [Fact]
    [System.ComponentModel.DisplayName("Producer_发送消息_不抛异常")]
    public static void ProduceTest()
    {
        var set = BasicTest.GetConfig();
        using var mq = new Producer
        {
            Topic = "nx_test",
            NameServerAddress = set.NameServer,

            Log = XTrace.Log,
        };

        mq.Start();

        for (var i = 0; i < 10; i++)
        {
            var str = "学无先后达者为师" + i;
            //var str = Rand.NextString(1337);

            var sr = mq.Publish(str, "TagA", null);
        }
    }
}