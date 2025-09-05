namespace NewLife.RocketMQ;

/// <summary>权限</summary>
[Flags]
public enum Permissions
{
    /// <summary>写入</summary>
    Write = 2,

    /// <summary>读取</summary>
    Read = 4,
}

/// <summary>代理信息</summary>
public class BrokerInfo
{
    #region 属性
    /// <summary>名称</summary>
    public String Name { get; set; }

    /// <summary>集群</summary>
    public String Cluster { get; set; }

    /// <summary>地址集合</summary>
    public String[] Addresses { get; set; }

    /// <summary>权限</summary>
    public Permissions Permission { get; set; }

    /// <summary>读队列数</summary>
    public Int32 ReadQueueNums { get; set; }

    /// <summary>写队列数</summary>
    public Int32 WriteQueueNums { get; set; }

    /// <summary>主题同步标记</summary>
    public Int32 TopicSynFlag { get; set; }

    /// <summary>是否主节点</summary>
    public Boolean IsMaster { get; set; }
    #endregion

    #region 相等
    /// <summary>相等比较</summary>
    /// <param name="obj"></param>
    /// <returns></returns>
    public override Boolean Equals(Object obj)
    {
        var x = this;
        if (obj is not BrokerInfo y) return false;

        return x.Name == y.Name && (x.Addresses == y.Addresses || x.Addresses != null && y.Addresses != null && x.Addresses.SequenceEqual(y.Addresses))
            && x.Permission == y.Permission && x.TopicSynFlag == y.TopicSynFlag
            && x.ReadQueueNums == y.ReadQueueNums && x.WriteQueueNums == y.WriteQueueNums
            && x.IsMaster == y.IsMaster;
    }

    /// <summary>计算哈希</summary>
    /// <returns></returns>
    public override Int32 GetHashCode()
    {
        var obj = this;
        return obj.Name.GetHashCode() ^ obj.Addresses.GetHashCode()
            ^ obj.Permission.GetHashCode() ^ obj.TopicSynFlag
            ^ obj.ReadQueueNums ^ obj.WriteQueueNums ^ obj.IsMaster.GetHashCode();
    }
    #endregion
}