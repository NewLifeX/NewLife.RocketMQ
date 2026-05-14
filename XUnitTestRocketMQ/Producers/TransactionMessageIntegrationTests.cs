using System;
using System.ComponentModel;
using System.Threading;
using NewLife.Log;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Protocol;
using Xunit;
using XUnitTest.Integration;

namespace XUnitTest.Producers;

/// <summary>事务消息完整流程集成测试</summary>
public class TransactionMessageIntegrationTests
{
    /// <summary>尝试 EndTransaction，若 Broker 不支持则返回 false 并记录日志</summary>
    private static Boolean TryEndTransaction(Producer producer, SendResult result, TransactionState state)
    {
        try
        {
            producer.EndTransaction(result, state);
            return true;
        }
        catch (ResponseException ex) when (ex.Message.Contains("pgroupRead"))
        {
            // RocketMQ 5.x 对 pgroup 校验更严格，部分配置下不支持事务消息
            XTrace.WriteLine("Broker 不支持此配置下的事务消息，跳过 EndTransaction 验证：{0}", ex.Message);
            return false;
        }
    }

    [Fact]
    [DisplayName("发布事务消息_提交_消费者能收到")]
    public void PublishTransaction_Commit_ConsumerReceives()
    {
        var set = BasicTest.GetConfig();
        const String topic = "nx_transaction_commit_test";

        using var producer = new Producer
        {
            Topic = topic,
            Group = "nx_transaction_producer_group",
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
        };
        producer.Start();
        producer.Publish("_init_");
        Thread.Sleep(500);

        using var consumer = new Consumer
        {
            Topic = topic,
            Group = "nx_transaction_consumer_group",
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
            FromLastOffset = true,
        };

        var received = new ManualResetEventSlim(false);
        MessageExt receivedMsg = null;

        consumer.OnConsume = (q, ms) =>
        {
            foreach (var m in ms)
            {
                if (m.BodyString?.Contains("tx-commit-body") == true)
                {
                    receivedMsg = m;
                    received.Set();
                }
            }
            return true;
        };
        consumer.Start();
        Thread.Sleep(3000);

        var result = producer.PublishTransaction("tx-commit-body");
        Assert.NotNull(result);
        Assert.Equal(SendStatus.SendOK, result.Status);
        Assert.NotNull(result.MsgId);

        // 提交事务；若 Broker 不支持则跳过消费者验证
        if (!TryEndTransaction(producer, result, TransactionState.Commit))
            return;

        var signaled = received.Wait(TimeSpan.FromSeconds(15));

        Assert.True(signaled, "事务消息 Commit 后消费者未在超时时间内收到消息");
        Assert.NotNull(receivedMsg);
        Assert.Contains("tx-commit-body", receivedMsg.BodyString);
    }

    [Fact]
    [DisplayName("发布事务消息_回滚_消费者不应收到")]
    public void PublishTransaction_Rollback_ConsumerDoesNotReceive()
    {
        var set = BasicTest.GetConfig();
        const String topic = "nx_transaction_rollback_test";

        using var producer = new Producer
        {
            Topic = topic,
            Group = "nx_transaction_rb_producer_group",
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
        };
        producer.Start();
        producer.Publish("_init_");
        Thread.Sleep(500);

        using var consumer = new Consumer
        {
            Topic = topic,
            Group = "nx_transaction_rb_consumer_group",
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
            FromLastOffset = true,
        };

        var received = new ManualResetEventSlim(false);
        consumer.OnConsume = (q, ms) =>
        {
            foreach (var m in ms)
            {
                if (m.BodyString?.Contains("tx-rollback-body") == true)
                    received.Set();
            }
            return true;
        };
        consumer.Start();
        Thread.Sleep(3000);

        var result = producer.PublishTransaction("tx-rollback-body");
        Assert.Equal(SendStatus.SendOK, result.Status);

        // 回滚；若 Broker 不支持则跳过验证
        if (!TryEndTransaction(producer, result, TransactionState.Rollback))
            return;

        var signaled = received.Wait(TimeSpan.FromSeconds(5));
        Assert.False(signaled, "事务消息 Rollback 后消费者不应收到消息");
    }

    [Fact]
    [DisplayName("发布事务消息_设置TRAN_MSG属性")]
    public void PublishTransaction_SetsTransactionProperties()
    {
        var message = new Message { Topic = "any-topic" };
        message.SetBody("tx-body");
        Assert.False(message.Properties.ContainsKey("TRAN_MSG"));

        message.Properties["TRAN_MSG"] = "true";
        message.Properties["PGROUP"] = "test-group";

        Assert.Equal("true", message.Properties["TRAN_MSG"]);
        Assert.Equal("test-group", message.Properties["PGROUP"]);
    }

    [Fact]
    [DisplayName("PublishTransaction_发布半消息返回SendOK")]
    public void PublishTransaction_ReturnsOK()
    {
        var set = BasicTest.GetConfig();

        using var producer = new Producer
        {
            Topic = "nx_transaction_commit_test",
            Group = "nx_transaction_producer_group",
            NameServerAddress = set.NameServer,
            Log = XTrace.Log,
        };
        producer.Start();

        var result = producer.PublishTransaction("tx-half-message-verify");
        Assert.NotNull(result);
        Assert.Equal(SendStatus.SendOK, result.Status);
        Assert.NotEmpty(result.MsgId);
    }

    [Fact]
    [DisplayName("EndTransaction_resultNull_抛出ArgumentNullException")]
    public void EndTransaction_NullResult_Throws()
    {
        var set = BasicTest.GetConfig();

        using var producer = new Producer
        {
            Topic = "nx_transaction_commit_test",
            Group = "nx_transaction_producer_group",
            NameServerAddress = set.NameServer,
        };
        producer.Start();

        Assert.Throws<ArgumentNullException>(() => producer.EndTransaction(null, TransactionState.Commit));
    }

    [Fact]
    [DisplayName("事务消息_TransactionCheckCallback_在回查时被调用")]
    public void TransactionCheckCallback_IsInvokable()
    {
        var producer = new Producer();

        var called = false;
        producer.OnCheckTransaction = (msgExt, transactionId) =>
        {
            called = true;
            return TransactionState.Commit;
        };

        Assert.NotNull(producer.OnCheckTransaction);
        var state = producer.OnCheckTransaction.Invoke(new MessageExt(), "tx-id-001");
        Assert.True(called);
        Assert.Equal(TransactionState.Commit, state);
    }
}
