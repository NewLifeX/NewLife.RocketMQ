using System;
using System.Collections.Generic;
using System.Xml.Serialization;

namespace NewLife.RocketMQ.Protocol
{
    /// <summary>头部</summary>
    public class Header
    {
        #region 属性
        /// <summary>请求/响应码</summary>
        [XmlElement("code")]
        public Int32 Code { get; set; }

        /// <summary>扩展字段</summary>
        /// <remarks>
        /// 这个字段不通的请求/响应不一样，完全自定义。数据结构上是java的hashmap。
        /// 在Java的每个RemotingCammand中，其实都带有一个CommandCustomHeader的属性成员，可以认为他是一个强类型的extFields，
        /// 再最后传输的时候，这个CommandCustomHeader会被忽略，而传输前会把其中的所有字段全部都原封不动塞到extFields中，以作传输。
        /// </remarks>
        [XmlElement("extFields")]
        public IDictionary<String, String> ExtFields { get; set; }

        /// <summary>标识</summary>
        /// <remarks>
        /// 第0位标识是这次通信是request还是response，0标识request, 1 标识response。
        /// 第1位标识是否是oneway请求，1标识oneway。应答方在处理oneway请求的时候，不会做出响应，请求方也无需等待应答方响应。
        /// </remarks>
        [XmlElement("flag")]
        public Int32 Flag { get; set; }

        /// <summary>由于要支持多语言，所以这一字段可以给通信双方知道对方通信层锁使用的开发语言</summary>
        /// <remarks>这里必须是JAVA，不能是CSharp，甚至Java都不行</remarks>
        [XmlElement("language")]
        public String Language { get; set; } = "CPP";

        /// <summary>请求标识码。在Java版的通信层中，这个只是一个不断自增的整形，为了收到应答方响应的的时候找到对应的请求。</summary>
        [XmlElement("opaque")]
        public Int32 Opaque { get; set; }

        /// <summary>序列化类型</summary>
        [XmlElement("serializeTypeCurrentRPC")]
        public String SerializeTypeCurrentRPC { get; set; } = "JSON";

        /// <summary>给通信层知道对方的版本号，响应方可以以此做兼容老版本等的特殊操作</summary>
        [XmlElement("version")]
        public Int32 Version { get; set; } = 275;

        /// <summary>附带的文本信息。常见的如存放一些broker/nameserver返回的一些异常信息，方便开发人员定位问题。</summary>
        [XmlElement("remark")]
        public String Remark { get; set; }
        #endregion

        #region 方法
        /// <summary>获取扩展字段。如果为空则创建</summary>
        /// <returns></returns>
        public IDictionary<String,String> GetExtFields()
        {
            if (ExtFields == null) ExtFields = new Dictionary<String, String>(StringComparer.OrdinalIgnoreCase);

            return ExtFields;
        }
        #endregion
    }
}