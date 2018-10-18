using System;
using NewLife.Collections;

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

        /// <summary>延迟时间等级</summary>
        public Int32 DelayTimeLevel { get; set; }
        #endregion

        #region 构造
        #endregion

        #region 方法
        /// <summary>获取属性</summary>
        /// <returns></returns>
        public String GetProperties()
        {
            var sb = Pool.StringBuilder.Get();

            if (!Tags.IsNullOrEmpty()) sb.AppendFormat("{0}\u0001{1}\u0002", nameof(Tags), Tags);
            if (!Keys.IsNullOrEmpty()) sb.AppendFormat("{0}\u0001{1}\u0002", nameof(Keys), Keys);
            if (DelayTimeLevel > 0) sb.AppendFormat("{0}\u0001{1}\u0002", "DELAY", DelayTimeLevel);
            sb.AppendFormat("{0}\u0001{1}\u0002", "WAIT", WaitStoreMsgOK);

            return sb.Put(true);
        }
        #endregion
    }
}