using System;
using System.ComponentModel;
using NewLife.RocketMQ.Common;
using Xunit;

namespace XUnitTestRocketMQ;

/// <summary>带权重负载均衡算法测试</summary>
public class WeightRoundRobinTests
{
    #region Set方法
    [Fact]
    [DisplayName("Set_Null参数抛出异常")]
    public void Set_NullWeights_ThrowsArgumentNullException()
    {
        var lb = new WeightRoundRobin();

        Assert.Throws<ArgumentNullException>(() => lb.Set(null));
    }

    [Fact]
    [DisplayName("Set_设置权重后Ready为true")]
    public void Set_ValidWeights_SetsReadyTrue()
    {
        var lb = new WeightRoundRobin();
        Assert.False(lb.Ready);

        lb.Set([1, 2, 3]);

        Assert.True(lb.Ready);
        Assert.Equal(3, lb.Weights.Length);
    }

    [Fact]
    [DisplayName("Set_相同权重不重复设置")]
    public void Set_SameWeights_NoReset()
    {
        var lb1 = new WeightRoundRobin();
        var lb2 = new WeightRoundRobin();

        // 初次设置相同权重
        lb1.Set([1, 2, 3]);
        lb2.Set([1, 2, 3]);

        // 先各选一次改变状态
        lb1.Get();
        lb2.Get();

        // 对 lb1 再次设置相同权重，不应重置状态
        lb1.Set([1, 2, 3]);

        // Ready 仍应为 true
        Assert.True(lb1.Ready);

        // 后续多次 Get 的返回序列应与未再次 Set 的 lb2 完全一致
        for (var i = 0; i < 10; i++)
        {
            var expected = lb2.Get();
            var actual = lb1.Get();
            Assert.Equal(expected, actual);
        }
    }

    [Fact]
    [DisplayName("Set_不同权重重新初始化")]
    public void Set_DifferentWeights_Reinitializes()
    {
        var lb = new WeightRoundRobin();
        lb.Set([1, 2]);

        lb.Set([3, 4, 5]);

        Assert.Equal(3, lb.Weights.Length);
        Assert.Equal(3, lb.Weights[0]);
        Assert.Equal(4, lb.Weights[1]);
        Assert.Equal(5, lb.Weights[2]);
    }
    #endregion

    #region Get方法
    [Fact]
    [DisplayName("Get_未初始化时返回0")]
    public void Get_NotInitialized_ReturnsZero()
    {
        var lb = new WeightRoundRobin();

        var idx = lb.Get(out var times);

        Assert.Equal(0, idx);
        Assert.Equal(1, times);
    }

    [Fact]
    [DisplayName("Get_等权重均匀分配")]
    public void Get_EqualWeights_EvenDistribution()
    {
        var lb = new WeightRoundRobin();
        lb.Set([1, 1, 1]);

        var counts = new Int32[3];
        for (var i = 0; i < 30; i++)
        {
            var idx = lb.Get();
            counts[idx]++;
        }

        // 等权重应该近似均匀分配
        Assert.Equal(10, counts[0]);
        Assert.Equal(10, counts[1]);
        Assert.Equal(10, counts[2]);
    }

    [Fact]
    [DisplayName("Get_不等权重按比例分配")]
    public void Get_UnequalWeights_ProportionalDistribution()
    {
        var lb = new WeightRoundRobin();
        lb.Set([3, 1]);

        var counts = new Int32[2];
        for (var i = 0; i < 40; i++)
        {
            var idx = lb.Get();
            counts[idx]++;
        }

        // 权重3:1，40次中应大约30次和10次
        Assert.True(counts[0] > counts[1], $"权重高的选中次数({counts[0]})应多于权重低的({counts[1]})");
    }

    [Fact]
    [DisplayName("Get_输出正确的次数")]
    public void Get_ReturnsTimes_Correctly()
    {
        var lb = new WeightRoundRobin();
        lb.Set([1, 1]);

        lb.Get(out var times1);
        Assert.Equal(1, times1);

        lb.Get(out _);
        lb.Get(out var times3);
        // 第一个索引被选中第2次
        Assert.Equal(2, times3);
    }

    [Fact]
    [DisplayName("Get_单个权重总是返回0")]
    public void Get_SingleWeight_AlwaysReturnsZero()
    {
        var lb = new WeightRoundRobin();
        lb.Set([5]);

        for (var i = 0; i < 10; i++)
        {
            var idx = lb.Get();
            Assert.Equal(0, idx);
        }
    }

    [Fact]
    [DisplayName("Get无参版本与有参版本一致")]
    public void Get_NoOutParam_SameAsWithOutParam()
    {
        var lb1 = new WeightRoundRobin();
        var lb2 = new WeightRoundRobin();
        lb1.Set([2, 3, 1]);
        lb2.Set([2, 3, 1]);

        for (var i = 0; i < 20; i++)
        {
            var idx1 = lb1.Get();
            var idx2 = lb2.Get(out _);
            Assert.Equal(idx1, idx2);
        }
    }
    #endregion
}
