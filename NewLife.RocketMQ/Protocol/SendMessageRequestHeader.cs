using System;
using System.Collections.Generic;
using System.Reflection;
using System.Xml.Serialization;
using NewLife.Reflection;

namespace NewLife.RocketMQ.Protocol
{
    /// <summary>发送消息请求头</summary>
    public class SendMessageRequestHeader
    {
        #region 属性
        [XmlElement("a")]
        public String ProducerGroup { get; set; }

        [XmlElement("b")]
        public String Topic { get; set; }

        [XmlElement("c")]
        public String DefaultTopic { get; set; }

        [XmlElement("d")]
        public Int32 DefaultTopicQueueNums { get; set; }

        [XmlElement("e")]
        public Int32 QueueId { get; set; }

        [XmlElement("f")]
        public Int32 SysFlag { get; set; }

        [XmlElement("g")]
        public Int64 BornTimestamp { get; set; }

        [XmlElement("h")]
        public Int32 Flag { get; set; }

        [XmlElement("i")]
        public String Properties { get; set; }

        [XmlElement("j")]
        public Int32 ReconsumeTimes { get; set; }

        [XmlElement("k")]
        public Boolean UnitMode { get; set; }
        #endregion

        #region 方法
        public IDictionary<String, Object> GetProperties()
        {
            var dic = new Dictionary<String, Object>();

            foreach (var pi in GetType().GetProperties())
            {
                if (pi.GetIndexParameters().Length > 0) continue;
                if (pi.GetCustomAttribute<XmlIgnoreAttribute>() != null) continue;

                var name = pi.Name;
                var att = pi.GetCustomAttribute<XmlElementAttribute>();
                if (att != null && !att.ElementName.IsNullOrEmpty()) name = att.ElementName;

                dic[name] = this.GetValue(pi);
            }

            return dic;
        }
        #endregion
    }
}