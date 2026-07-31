using System.Reflection;
using System.Xml.Serialization;
using NewLife.Reflection;

namespace NewLife.RocketMQ.Protocol;

/// <summary>发送消息请求头</summary>
public class SendMessageRequestHeader
{
    #region 属性
    /// <summary>生产组</summary>
    [XmlElement("a")]
    public String ProducerGroup { get; set; }

    /// <summary>主题</summary>
    [XmlElement("b")]
    public String Topic { get; set; }

    /// <summary>默认主题</summary>
    [XmlElement("c")]
    public String DefaultTopic { get; set; }

    /// <summary>默认主题队列数</summary>
    [XmlElement("d")]
    public Int32 DefaultTopicQueueNums { get; set; }

    /// <summary>队列编号</summary>
    [XmlElement("e")]
    public Int32 QueueId { get; set; }

    /// <summary>系统标记</summary>
    [XmlElement("f")]
    public Int32 SysFlag { get; set; }

    /// <summary>生产时间。毫秒</summary>
    [XmlElement("g")]
    public Int64 BornTimestamp { get; set; }

    /// <summary>标记</summary>
    [XmlElement("h")]
    public Int32 Flag { get; set; }

    /// <summary>属性。Tags/Keys等</summary>
    [XmlElement("i")]
    public String Properties { get; set; }

    /// <summary>重新消费次数</summary>
    [XmlElement("j")]
    public Int32 ReconsumeTimes { get; set; }

    /// <summary>单元模式</summary>
    [XmlElement("k")]
    public Boolean UnitMode { get; set; }

    /// <summary>消费重试次数</summary>
    [XmlElement("l")]
    public Int32 ConsumeRetryTimes { get; set; }

    /// <summary>批操作</summary>
    [XmlElement("m")]
    public Boolean Batch { get; set; }

    /// <summary>Broker名称</summary>
    [XmlElement("n")]
    public String BrokerName { get; set; }
    #endregion

    #region 方法
    /// <summary>属性元数据缓存。避免每条消息发送时重复反射</summary>
    private static readonly PropertyInfo[] _props = typeof(SendMessageRequestHeader).GetProperties();

    /// <summary>XmlElement 短名映射（V2 协议单字母 key）</summary>
    private static readonly Dictionary<String, String> _names = BuildNames();

    private static Dictionary<String, String> BuildNames()
    {
        var dic = new Dictionary<String, String>();
        foreach (var pi in _props)
        {
            var att = pi.GetCustomAttribute<XmlElementAttribute>();
            if (att != null && !att.ElementName.IsNullOrEmpty()) dic[pi.Name] = att.ElementName;
        }
        return dic;
    }

    /// <summary>获取属性字典</summary>
    /// <returns></returns>
    public IDictionary<String, Object> GetProperties()
    {
        var dic = new Dictionary<String, Object>();

        foreach (var pi in _props)
        {
            if (pi.GetIndexParameters().Length > 0) continue;
            if (pi.GetCustomAttribute<XmlIgnoreAttribute>() != null) continue;

            var name = _names.TryGetValue(pi.Name, out var shortName) ? shortName : pi.Name;

            dic[name] = this.GetValue(pi);
        }

        return dic;
    }
    #endregion
}