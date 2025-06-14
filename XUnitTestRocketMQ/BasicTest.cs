using NewLife.Log;
using NewLife.RocketMQ;
using Xunit;

// 所有测试用例放入一个汇编级集合，除非单独指定Collection特性
[assembly: CollectionBehavior(CollectionBehavior.CollectionPerAssembly)]

namespace XUnitTestRocketMQ;

[Collection("Basic")]
public class BasicTest
{
    private static MqSetting _config;
    public static MqSetting GetConfig()
    {
        if (_config != null) return _config;
        lock (typeof(BasicTest))
        {
            if (_config != null) return _config;

            var set = MqSetting.Current;
            if (set.IsNew)
            {
                set.NameServer = "rocketmq.newlifex.com:9876";
                set.Save();
            }

            XTrace.WriteLine("RocketMQ配置：{0}", set.NameServer);

            return _config = set;
        }
    }
}
