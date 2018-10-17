using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace NewLife.RocketMQ.Common
{
    /// <summary>带权重负载均衡算法</summary>
    /// <typeparam name="T"></typeparam>
    public class WeightRoundRobin<T> where T : class
    {
        #region 属性
        /// <summary>最大权重</summary>
        private readonly Int32 maxWeight;

        /// <summary>权重的最大公约数</summary>
        private readonly Int32 gcdWeight;

        /// <summary>数据源</summary>
        public IList<T> Source { get; set; }

        /// <summary>获取权重的委托</summary>
        public Func<T, Int32> GetWeight;
        #endregion

        #region 构造
        /// <summary>实例化</summary>
        public WeightRoundRobin(IList<T> source, Func<T, Int32> getWeight)
        {
            Source = source;
            GetWeight = getWeight;

            maxWeight = source.Max(GetWeight);
            gcdWeight = GreatestCommonDivisor(source);
        }
        #endregion

        #region 核心方法
        /// <summary>上次选择</summary>
        private Int32 currentIndex = -1;

        /// <summary>当前调度的权值</summary>
        private Int32 currentWeight = 0;

        private readonly ConcurrentDictionary<T, Int32> _cache = new ConcurrentDictionary<T, Int32>();

        /// <summary>根据权重选择，并返回该项是第几次选中</summary>
        /// <remarks>
        /// 算法流程：  
        /// 假设有一组服务器 S = {S0, S1, …, Sn-1} 
        /// 有相应的权重，变量currentIndex表示上次选择的服务器 
        /// 权值currentWeight初始化为0，currentIndex初始化为-1 ，当第一次的时候返回 权值取最大的那个服务器，
        /// 通过权重的不断递减 寻找 适合的服务器返回
        /// </remarks>
        /// <returns></returns>
        public T Get(out Int32 times)
        {
            var list = Source;
            while (true)
            {
                currentIndex = Interlocked.Increment(ref currentIndex) % list.Count;
                if (currentIndex == 0)
                {
                    currentWeight = Interlocked.Add(ref currentWeight, -gcdWeight);
                    if (currentWeight <= 0)
                    {
                        currentWeight = maxWeight;
                        //if (currentWeight == 0) return null;
                    }
                }

                var item = list[currentIndex];
                if (GetWeight(item) >= currentWeight)
                {
                    // 该项是第几次选中
                    var old = 0;
                    do
                    {
                        old = times = _cache.GetOrAdd(item, 0);
                        times++;
                    } while (!_cache.TryUpdate(item, times, old));

                    return item;
                }
            }
        }

        /// <summary>根据权重选择</summary>
        /// <returns></returns>
        public T Get() => Get(out var n);
        #endregion

        #region 最大公约数
        /// <summary>得到两值的最大公约数</summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns></returns>
        public Int32 GreaterCommonDivisor(Int32 a, Int32 b)
        {
            var k = a % b;
            if (k == 0) return b;

            return GreaterCommonDivisor(b, k);
        }

        /// <summary>得到list中所有权重的最大公约数</summary>
        /// <remarks>
        /// 实际上是两两取最大公约数d，然后得到的d与下一个权重取最大公约数，直至遍历完
        /// </remarks>
        /// <param name="list"></param>
        /// <returns></returns>
        public Int32 GreatestCommonDivisor(IList<T> list)
        {
            var divisor = 0;
            for (var i = 0; i < list.Count - 1; i++)
            {
                var w = GetWeight(list[i]);
                var w2 = GetWeight(list[i + 1]);
                if (i == 0)
                    divisor = GreaterCommonDivisor(w, w2);
                else
                    divisor = GreaterCommonDivisor(divisor, w);
            }
            return divisor;
        }
        #endregion
    }
}