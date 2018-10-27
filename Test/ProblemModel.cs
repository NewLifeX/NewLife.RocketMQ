using NewLife.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace Test
{
    /// <summary>问题件</summary>
    public class ProblemModel
    {
        #region 属性
        /// <summary>运单编号</summary>
        [XmlElement(ElementName = "BILL_CODE")]
        public String Code { get; set; }

        /// <summary>扫描时间</summary>
        [XmlElement(ElementName = "SCAN_DATE")]
        public DateTime ScanDate { get; set; }

        /// <summary>登记人编号</summary>
        [XmlElement(ElementName = "REGISTER_MAN_CODE")]
        public String RegisterManCode { get; set; }

        /// <summary>登记网点</summary>
        [XmlElement(ElementName = "REGISTER_SITE_ID")]
        public Int32 RegisterSiteID { get; set; }

        /// <summary>通知网点</summary>
        [XmlElement(ElementName = "SEND_SITE_ID")]
        public Int32 SendSiteID { get; set; }

        /// <summary>登记时间</summary>
        [XmlElement(ElementName = "REGISTER_DATE")]
        public DateTime RegisterDate { get; set; }

        /// <summary>问题件类型编码</summary>
        [XmlElement(ElementName = "TYPE_CODE")]
        public String TypeCode { get; set; }

        public string ScanManCode { get; set; }

        public int ScanSiteID { get; set; }

        public int PreOrNexStaID { get; set; }

        public double Weight { get; set; }

        public bool AutoSort { get; set; }

        //public DateTime InputDate { get; set; }

        public string PdaCode { get; set; }

        /// <summary></summary>
        public DateTime InputDate { get; set; }
        #endregion
    }
}
