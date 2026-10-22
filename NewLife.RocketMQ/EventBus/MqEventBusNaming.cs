using NewLife.Collections;

namespace NewLife.RocketMQ;

/// <summary>事件总线名称转换。把事件主题与消费组转换为 RocketMQ 允许的名称</summary>
/// <remarks>
/// RocketMQ 的主题名与消费组名只允许 <c>a-z A-Z 0-9 _ - %</c>，而事件主题常带 <c>.</c>、本机标识常带 <c>.</c> 与 <c>@</c>。
/// 本类把它们统一替换为 <c>_</c>，并按 RocketMQ 上限截断，保证发布与订阅两侧得到同一个名称。
/// </remarks>
public static class MqEventBusNaming
{
    /// <summary>名称最大长度。RocketMQ 主题名上限为 127 字符</summary>
    public const Int32 MaxNameLength = 127;

    /// <summary>转换事件主题为 RocketMQ 主题名</summary>
    /// <param name="topic">事件主题</param>
    /// <returns>合法的主题名</returns>
    public static String ToTopic(String topic) => Sanitize(topic);

    /// <summary>转换客户端标识为 RocketMQ 消费组名</summary>
    /// <param name="group">客户端标识或组名</param>
    /// <returns>合法的消费组名</returns>
    public static String ToGroup(String group) => Sanitize(group);

    /// <summary>把名称中的非法字符替换为下划线并截断</summary>
    /// <param name="name">原始名称</param>
    /// <returns>合法名称；输入为空时原样返回</returns>
    public static String Sanitize(String name)
    {
        if (name.IsNullOrEmpty()) return name;

        var sb = Pool.StringBuilder.Get();
        foreach (var ch in name)
        {
            if (ch is >= 'a' and <= 'z' or >= 'A' and <= 'Z' or >= '0' and <= '9' or '_' or '-' or '%')
                sb.Append(ch);
            else
                sb.Append('_');
        }

        // Return(true) 才会取回构建结果，false 仅归还构建器并返回空串
        var str = sb.Return(true);

        return str.Length > MaxNameLength ? str[..MaxNameLength] : str;
    }
}
