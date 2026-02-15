namespace NewLife.RocketMQ.Grpc;

/// <summary>Protobuf消息接口。所有gRPC消息类型实现此接口</summary>
public interface IProtoMessage
{
    /// <summary>写入到Protobuf编码器</summary>
    /// <param name="writer">编码器</param>
    void WriteTo(ProtoWriter writer);

    /// <summary>从Protobuf解码器读取</summary>
    /// <param name="reader">解码器</param>
    void ReadFrom(ProtoReader reader);
}
