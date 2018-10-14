using System;

namespace NewLife.RocketMQ.Protocol
{
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
        public Byte[] Body { get; set; }

        /// <summary>等待存储消息</summary>
        public Boolean WaitStoreMsgOK { get; set; } = true;
        #endregion

        #region 构造
        #endregion
    }
}