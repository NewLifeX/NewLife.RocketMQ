using System.Runtime.Serialization;
using System.Xml.Serialization;
using NewLife.Collections;
using NewLife.Data;
using NewLife.Serialization;

namespace NewLife.RocketMQ.Protocol;

/// <summary>消息</summary>
public class Message
{
    #region 属性
    /// <summary>主题</summary>
    public String Topic { get; set; }

    /// <summary>标签</summary>
    public String Tags { get; set; }

    /// <summary>键</summary>
    public String Keys { get; set; }

    /// <summary>标记</summary>
    public Int32 Flag { get; set; }

    /// <summary>消息体</summary>
    [XmlIgnore, IgnoreDataMember]
    public Byte[] Body { get; set; }

    private String _BodyString;
    /// <summary>消息体。字符串格式</summary>
    public String BodyString => _BodyString ??= Body?.ToStr();

    /// <summary>等待存储消息</summary>
    public Boolean WaitStoreMsgOK { get; set; } = true;

    /// <summary>延迟时间等级</summary>
    public Int32 DelayTimeLevel { get; set; }
    #endregion

    #region 构造
    /// <summary>友好字符串</summary>
    /// <returns></returns>
    public override String ToString() => Body != null && Body.Length > 0 ? BodyString : base.ToString();
    #endregion

    #region 方法
    /// <summary>
    /// 设置消息体
    /// </summary>
    /// <param name="body"></param>
    public void SetBody(Object body)
    {
        if (body is Packet pk)
            Body = pk.ReadBytes();
        else if (body is Byte[] buf)
            Body = buf;
        else if (body is String str)
        {
            _BodyString = str;
            Body = str.GetBytes();
        }
        else
        {
            str = body.ToJson();
            _BodyString = str;
            Body = str.GetBytes();
        }
    }

    /// <summary>获取属性</summary>
    /// <returns></returns>
    public String GetProperties()
    {
        var sb = Pool.StringBuilder.Get();

        if (!Tags.IsNullOrEmpty()) sb.AppendFormat("{0}\u0001{1}\u0002", "TAGS", Tags);
        if (!Keys.IsNullOrEmpty()) sb.AppendFormat("{0}\u0001{1}\u0002", "KEYS", Keys);
        if (DelayTimeLevel > 0) sb.AppendFormat("{0}\u0001{1}\u0002", "DELAY", DelayTimeLevel);
        sb.AppendFormat("{0}\u0001{1}\u0002", "WAIT", WaitStoreMsgOK);

        return sb.Put(true);
    }

    /// <summary>分析字典属性</summary>
    /// <param name="properties"></param>
    public IDictionary<String, String> ParseProperties(String properties)
    {
        if (properties.IsNullOrEmpty()) return null;

        var dic = properties.SplitAsDictionaryT('\u0001', '\u0002');

        if (TryGetAndRemove(dic, nameof(Tags), out var str)) Tags = str;
        if (TryGetAndRemove(dic, nameof(Keys), out str)) Keys = str;
        if (TryGetAndRemove(dic, "DELAY", out str)) DelayTimeLevel = str.ToInt();
        if (TryGetAndRemove(dic, "WAIT", out str)) WaitStoreMsgOK = str.ToBoolean();

        return dic;
    }

    private Boolean TryGetAndRemove(IDictionary<String, String> dic, String key, out String value)
    {
        if (dic.TryGetValue(key, out var str))
        {
            value = str;
            dic.Remove(key);

            return true;
        }

        value = null;
        return false;
    }
    #endregion
}