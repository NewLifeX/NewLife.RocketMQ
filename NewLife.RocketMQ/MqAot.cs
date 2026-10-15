namespace NewLife.RocketMQ;

#if NET10_0_OR_GREATER
using System.Diagnostics.CodeAnalysis;
#endif

/// <summary>NativeAOT 辅助</summary>
/// <remarks>
/// 库内模型（Command/Message/MessageExt 等）已内置源生成成员访问器与元数据根，裁剪后可直接序列化；
/// 使用方自定义类型（POCO 消息体）不在库内登记范围，裁剪器会移除其公开属性，导致对象体被静默序列化为空对象 <c>{}</c>。
/// 在应用启动处对每个用作消息体的类型调用一次 <see cref="RegisterBodyType{T}"/>，即可保留其公开属性。
/// </remarks>
public static class MqAot
{
    /// <summary>保留对象体类型的公开属性元数据</summary>
    /// <typeparam name="T">用作消息体的类型</typeparam>
    /// <remarks>
    /// 仅 net10.0 目标带裁剪保留语义，其余目标为空实现（无裁剪概念，调用无副作用），便于多目标工程共用同一份启动代码。
    /// 自定义类型也可自行声明元数据根达到同样效果：
    /// <code>
    /// [ModuleInitializer]
    /// internal static void Initialize() => Preserve();
    ///
    /// [DynamicDependency(DynamicallyAccessedMemberTypes.PublicProperties, typeof(MyOrder))]
    /// private static void Preserve() { }
    /// </code>
    /// </remarks>
    /// <example>
    /// <code>
    /// // 应用启动处登记所有用作消息体的自定义类型
    /// MqAot.RegisterBodyType&lt;MyOrder&gt;();
    /// MqAot.RegisterBodyType&lt;MyEvent&gt;();
    /// </code>
    /// </example>
#if NET10_0_OR_GREATER
    public static void RegisterBodyType<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>() { }
#else
    public static void RegisterBodyType<T>() { }
#endif
}
