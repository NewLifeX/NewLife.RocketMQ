using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace NewLife.RocketMQ
{
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
    }
}
