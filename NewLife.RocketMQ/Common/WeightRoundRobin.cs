namespace NewLife.RocketMQ.Common;

/// <summary>带权重负载均衡算法</summary>
public class WeightRoundRobin : ILoadBalance
{
    #region 属性
    /// <summary>已就绪</summary>
    public Boolean Ready { get; set; }

    /// <summary>权重集合</summary>
    public Int32[] Weights { get; private set; }

    /// <summary>最小权重</summary>
    private Int32 minWeight;

    /// <summary>状态值</summary>
    private Int32[] _states;

    /// <summary>次数</summary>
    private Int32[] _times;
    #endregion

    #region 方法
    /// <summary>设置每个选项的权重数据</summary>
    /// <param name="weights"></param>
    public void Set(Int32[] weights)
    {
        if (weights == null) throw new ArgumentNullException(nameof(weights));
        if (Weights != null && Weights.SequenceEqual(weights)) return;

        Weights = weights;

        minWeight = weights.Min();

        _states = new Int32[weights.Length];
        _times = new Int32[weights.Length];

        Ready = true;
    }

    /// <summary>根据权重选择，并返回该项是第几次选中</summary>
    /// <param name="times">第几次选中</param>
    /// <returns></returns>
    public Int32 Get(out Int32 times)
    {
        // 选择状态最大值
        var cur = GetMax(_states, out var idx);

        // 如果所有状态都不达标，则集体加盐
        if (cur < minWeight)
        {
            for (var i = 0; i < Weights.Length; i++)
            {
                _states[i] += Weights[i];
            }

            // 重新选择状态最大值
            cur = GetMax(_states, out idx);
        }

        // 已选择，减状态
        _states[idx] -= minWeight;

        times = ++_times[idx];

        return idx;
    }

    /// <summary>根据权重选择</summary>
    /// <returns></returns>
    public Int32 Get() => Get(out var n);

    private Int32 GetMax(Int32[] ds, out Int32 idx)
    {
        var n = Int32.MinValue;
        idx = 0;
        for (var i = 0; i < ds.Length; i++)
        {
            if (ds[i] > n)
            {
                n = ds[i];
                idx = i;
            }
        }

        return n;
    }
    #endregion
}