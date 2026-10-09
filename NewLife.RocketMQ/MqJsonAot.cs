#if NET10_0_OR_GREATER
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using NewLife.RocketMQ.Protocol;
using NewLife.RocketMQ.Protocol.ConsumerStates;

namespace NewLife.RocketMQ;

/// <summary>NativeAOT 元数据根。声明本库需要经 JSON 序列化的模型保留公开属性</summary>
/// <remarks>
/// NewLife 序列化按运行时类型反射枚举成员，裁剪后成员丢失会导致输出空对象（{}）。
/// 与 MqJsonContext 的源生成成员访问器配合：访问器负责读写，本类负责让裁剪器保留成员本身。
/// ModuleInitializer 仅做元数据保留与访问器登记，无其它副作用，故忽略 CA2255 对库代码的告警。
/// </remarks>
#pragma warning disable CA2255
internal static class MqJsonAot
{
    [ModuleInitializer]
    internal static void Initialize() => Preserve();

    [DynamicDependency(DynamicallyAccessedMemberTypes.PublicProperties, typeof(Command))]
    [DynamicDependency(DynamicallyAccessedMemberTypes.PublicProperties, typeof(Header))]
    [DynamicDependency(DynamicallyAccessedMemberTypes.PublicProperties, typeof(Message))]
    [DynamicDependency(DynamicallyAccessedMemberTypes.PublicProperties, typeof(MessageExt))]
    [DynamicDependency(DynamicallyAccessedMemberTypes.PublicProperties, typeof(MessageQueue))]
    [DynamicDependency(DynamicallyAccessedMemberTypes.PublicProperties, typeof(SubscriptionData))]
    [DynamicDependency(DynamicallyAccessedMemberTypes.PublicProperties, typeof(HeartbeatData))]
    [DynamicDependency(DynamicallyAccessedMemberTypes.PublicProperties, typeof(ProducerData))]
    [DynamicDependency(DynamicallyAccessedMemberTypes.PublicProperties, typeof(ConsumerData))]
    [DynamicDependency(DynamicallyAccessedMemberTypes.PublicProperties, typeof(MessageQueueModel))]
    [DynamicDependency(DynamicallyAccessedMemberTypes.PublicProperties, typeof(OffsetWrapperModel))]
    [DynamicDependency(DynamicallyAccessedMemberTypes.PublicProperties, typeof(ConsumerStatesModel))]
    private static void Preserve() { }
}
#pragma warning restore CA2255
#endif
