using System;

namespace NewLife.RocketMQ.Common
{
    /// <summary>负载均衡接口</summary>
    public interface ILoadBalance
    {
        /// <summary>已就绪</summary>
        Boolean Ready { get; set; }

        /// <summary>设置每个选项的权重数据</summary>
        /// <param name="weights"></param>
        void Set(Int32[] weights);

        /// <summary>根据权重选择，并返回该项是第几次选中</summary>
        /// <param name="times">第几次选中</param>
        /// <returns></returns>
        Int32 Get(out Int32 times);
    }
}