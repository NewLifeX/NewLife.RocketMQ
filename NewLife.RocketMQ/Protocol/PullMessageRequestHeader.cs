using System;
using System.Collections.Generic;
using NewLife.Reflection;

namespace NewLife.RocketMQ.Protocol
{
    /// <summary>拉取信息请求头</summary>
    public class PullMessageRequestHeader
    {
        #region 属性
        /// <summary>消费组</summary>
        public String ConsumerGroup { get; set; }

        /// <summary>主题</summary>
        public String Topic { get; set; }

        /// <summary>表达式类型</summary>
        public String ExpressionType { get; set; } = "TAG";

        public String Subscription { get; set; } = "*";

        public Int32 SuspendTimeoutMillis { get; set; } = 20_000;

        public Int32 SubVersion { get; set; }

        /// <summary>队列</summary>
        public Int32 QueueId { get; set; }

        /// <summary>队列偏移</summary>
        public Int64 QueueOffset { get; set; }

        /// <summary>最大消息数</summary>
        public Int32 MaxMsgNums { get; set; }

        /// <summary>提交偏移</summary>
        public Int32 CommitOffset { get; set; }

        public Int32 SysFlag { get; set; }
        #endregion

        #region 方法
        public IDictionary<String, Object> GetProperties()
        {
            var dic = new Dictionary<String, Object>();

            foreach (var pi in GetType().GetProperties())
            {
                //if (pi.GetIndexParameters().Length > 0) continue;
                //if (pi.GetCustomAttribute<XmlIgnoreAttribute>() != null) continue;

                var name = pi.Name;
                //var att = pi.GetCustomAttribute<XmlElementAttribute>();
                //if (att != null && !att.ElementName.IsNullOrEmpty()) name = att.ElementName;
                name = name.Substring(0, 1).ToLower() + name.Substring(1);

                dic[name] = this.GetValue(pi) + "";
            }

            return dic;
        }
        #endregion
    }
}