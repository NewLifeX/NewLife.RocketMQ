using System;
using System.Threading;
using NewLife;
using NewLife.Log;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Protocol;
using Xunit;

namespace XUnitTestRocketMQ
{
    public class ProducerTracerTests
    {
        private const String Topic = "TopicDemo";
        private const String Group = "TraceTestGroup";
        private const String NameServerAddress = "127.0.0.1:9876";

        [Fact]
        public void Producer_And_Consumer_With_Trace_Enabled_Should_Work()
        {
            XTrace.UseConsole();
            // 使用 ManualResetEvent 来同步测试的完成
            var mre = new ManualResetEvent(false);

            // 2. 创建并启动生产者
            var producer = new Producer
            {
                Topic = Topic,
                Group = Group, // 生产者组可以和消费者组不同，这里为了简单使用同一个
                NameServerAddress = NameServerAddress,
                EnableMessageTrace = true, // 启用消息轨迹
                Log = XTrace.Log
            };

            producer.Start();

            // 3. 发送消息
            var messageBody = "Hello, RocketMQ with Message Trace!";
            var sendResult = producer.Publish(messageBody);

            Assert.NotNull(sendResult);
            Assert.Equal(SendStatus.SendOK, sendResult.Status);
            XTrace.WriteLine($"消息发送成功: MsgId={sendResult.MsgId}");

            // 4. 等待消费者处理消息，设置一个超时时间以防测试挂起
            bool consumed = mre.WaitOne(TimeSpan.FromSeconds(300));

            // 5. 清理资源
            producer.Stop();

            // 6. 断言
            Assert.True(consumed, "消费者在超时时间内没有收到消息。");
        }
    }
}
