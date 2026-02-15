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
    /// <summary>获取属性字典</summary>
    /// <returns></returns>
    public IDictionary<String, Object> GetProperties()
    {
        var dic = new Dictionary<String, Object>();
        foreach (var pi in GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (pi.GetIndexParameters().Length > 0) continue;
            var name = pi.Name;
            if (!name.IsNullOrEmpty()) name = Char.ToLowerInvariant(name[0]) + name.Substring(1);

            dic[name] = this.GetValue(pi);
        }

        return dic;
    }
    #endregion
}
