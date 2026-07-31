using System.Reflection;
using System.Xml.Serialization;
using NewLife.Reflection;

namespace NewLife.RocketMQ.Protocol;

/// <summary>拉取信息请求头</summary>
public class PullMessageRequestHeader
{
    #region 属性
    /// <summary>消费组</summary>
    public String ConsumerGroup { get; set; }

    /// <summary>主题</summary>
    public String Topic { get; set; }

    ///// <summary>表达式类型</summary>
    //public String ExpressionType { get; set; } = "TAG";

    /// <summary>订阅表达式</summary>
    public String Subscription { get; set; } = "*";

    /// <summary>表达式类型。TAG或SQL92，默认TAG</summary>
    public String ExpressionType { get; set; } = "TAG";

    /// <summary>挂起超时时间。默认20_000ms</summary>
    public Int32 SuspendTimeoutMillis { get; set; } = 20_000;

    /// <summary>子版本</summary>
    public Int64 SubVersion { get; set; }

    /// <summary>队列</summary>
    public Int32 QueueId { get; set; }

    /// <summary>队列偏移</summary>
    public Int64 QueueOffset { get; set; }

    /// <summary>最大消息数</summary>
    public Int32 MaxMsgNums { get; set; }

    /// <summary>提交偏移</summary>
    public Int64 CommitOffset { get; set; }

    /// <summary>系统标记</summary>
    public Int32 SysFlag { get; set; }
    #endregion

    #region 方法
    /// <summary>属性元数据缓存。避免每次拉取消息时重复反射</summary>
    private static readonly PropertyInfo[] _props = typeof(PullMessageRequestHeader).GetProperties();

    /// <summary>camelCase 属性名缓存</summary>
    private static readonly String[] _names = _props.Select(e => Char.ToLowerInvariant(e.Name[0]) + e.Name.Substring(1)).ToArray();

    /// <summary>获取属性字典</summary>
    /// <returns></returns>
    public IDictionary<String, Object> GetProperties()
    {
        //var dic = new Dictionary<String, Object>();
        var dic = new SortedList<String, Object>(StringComparer.Ordinal);

        for (var i = 0; i < _props.Length; i++)
        {
            var pi = _props[i];

            dic[_names[i]] = this.GetValue(pi) + "";
        }

        return dic;
    }
    #endregion
}