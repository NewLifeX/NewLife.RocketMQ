using System.Reflection;
using NewLife.Reflection;

namespace NewLife.RocketMQ.Protocol;

/// <summary>结束事务请求头</summary>
public class EndTransactionRequestHeader
{
    #region 属性
    /// <summary>生产组</summary>
    public String ProducerGroup { get; set; }

    /// <summary>事务状态表偏移</summary>
    public Int64 TranStateTableOffset { get; set; }

    /// <summary>提交日志偏移</summary>
    public Int64 CommitLogOffset { get; set; }

    /// <summary>提交或回滚标记</summary>
    public Int32 CommitOrRollback { get; set; }

    /// <summary>是否来自事务回查</summary>
    public Boolean FromTransactionCheck { get; set; }

    /// <summary>消息编号</summary>
    public String MsgId { get; set; }

    /// <summary>事务编号</summary>
    public String TransactionId { get; set; }
    #endregion

    #region 方法
    /// <summary>属性元数据缓存。避免事务操作时重复反射</summary>
    private static readonly PropertyInfo[] _props = typeof(EndTransactionRequestHeader).GetProperties(BindingFlags.Public | BindingFlags.Instance);

    /// <summary>camelCase 属性名缓存</summary>
    private static readonly String[] _names = _props.Select(e => Char.ToLowerInvariant(e.Name[0]) + e.Name.Substring(1)).ToArray();

    /// <summary>获取属性字典</summary>
    /// <returns></returns>
    public IDictionary<String, Object> GetProperties()
    {
        var dic = new Dictionary<String, Object>();
        for (var i = 0; i < _props.Length; i++)
        {
            var pi = _props[i];
            if (pi.GetIndexParameters().Length > 0) continue;

            dic[_names[i]] = this.GetValue(pi);
        }

        return dic;
    }
    #endregion
}
