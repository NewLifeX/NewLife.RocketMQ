#if NET10_0_OR_GREATER
using System.Text.Json.Serialization;
using NewLife.RocketMQ.Protocol;
using NewLife.RocketMQ.Protocol.ConsumerStates;

namespace NewLife.RocketMQ;

/// <summary>JSON 序列化上下文。NewLife 源生成器据此为本库模型生成成员访问器，保证 NativeAOT 裁剪后仍可序列化</summary>
/// <remarks>新增需要经 JSON 序列化的模型时，在此追加 <see cref="JsonSerializableAttribute"/> 登记</remarks>
[JsonSerializable(typeof(Command))]
[JsonSerializable(typeof(Header))]
[JsonSerializable(typeof(Message))]
[JsonSerializable(typeof(MessageExt))]
[JsonSerializable(typeof(MessageQueue))]
[JsonSerializable(typeof(SubscriptionData))]
[JsonSerializable(typeof(HeartbeatData))]
[JsonSerializable(typeof(ProducerData))]
[JsonSerializable(typeof(ConsumerData))]
[JsonSerializable(typeof(MessageQueueModel))]
[JsonSerializable(typeof(OffsetWrapperModel))]
[JsonSerializable(typeof(ConsumerStatesModel))]
internal partial class MqJsonContext : JsonSerializerContext { }
#endif
