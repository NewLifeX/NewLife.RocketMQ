using System;
using System.ComponentModel;
using System.Threading.Tasks;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Protocol;
using Xunit;

namespace XUnitTestRocketMQ;

/// <summary>事务回查功能测试</summary>
public class TransactionCheckTests
{
    [Fact]
    [DisplayName("OnCheckTransaction_默认为null")]
    public void OnCheckTransaction_DefaultNull()
    {
        using var producer = new Producer();
        Assert.Null(producer.OnCheckTransaction);
        Assert.Null(producer.OnCheckTransactionAsync);
    }

    [Fact]
    [DisplayName("OnCheckTransaction_可设置回调委托")]
    public void OnCheckTransaction_CanSetCallback()
    {
        var callbackInvoked = false;
        using var producer = new Producer
        {
            OnCheckTransaction = (msg, transactionId) =>
            {
                callbackInvoked = true;
                return TransactionState.Commit;
            }
        };

        Assert.NotNull(producer.OnCheckTransaction);
        var state = producer.OnCheckTransaction(new MessageExt(), "test-txid");
        Assert.True(callbackInvoked);
        Assert.Equal(TransactionState.Commit, state);
    }

    [Fact]
    [DisplayName("OnCheckTransactionAsync_可设置异步回调委托")]
    public async Task OnCheckTransactionAsync_CanSetCallback()
    {
        var callbackInvoked = false;
        using var producer = new Producer
        {
            OnCheckTransactionAsync = async (msg, transactionId, ct) =>
            {
                await Task.CompletedTask;
                callbackInvoked = true;
                return TransactionState.Rollback;
            }
        };

        Assert.NotNull(producer.OnCheckTransactionAsync);
        var state = await producer.OnCheckTransactionAsync(new MessageExt(), "test-txid", default);
        Assert.True(callbackInvoked);
        Assert.Equal(TransactionState.Rollback, state);
    }
}
