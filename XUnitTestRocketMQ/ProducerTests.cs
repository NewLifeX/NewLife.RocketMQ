using NewLife.Log;
using NewLife.RocketMQ;
using System;
using Xunit;

namespace XUnitTestRocketMQ
{
    public class ProducerTests
    {
        [Fact]
        public void CreateTopic()
        {
            var mq = new Producer
            {
                //Topic = "nx_test",
                NameServerAddress = "127.0.0.1:9876",

                Log = XTrace.Log,
            };

            mq.Start();

            Assert.Equal("TBW102", mq.Topic);

            mq.CreateTopic("nx_test", 2);
        }
    }
}