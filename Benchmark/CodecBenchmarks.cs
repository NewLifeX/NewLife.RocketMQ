using System;
using System.Buffers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using NewLife;
using NewLife.Data;
using NewLife.RocketMQ;
using NewLife.RocketMQ.Protocol;
using NewLife.Serialization;

namespace NewLife.RocketMQ.Benchmarks;

/// <summary>协议与序列化基准（JIT 侧严谨对照；AOT 侧使用 AotSmoke --bench 的同名指标）</summary>
/// <remarks>
/// BenchmarkDotNet 依赖运行时代码生成，无法在 NativeAOT 下运行，故 AOT 与 JIT 的对照方式为：
/// 本基准给出 JIT 侧的严谨数据，AotSmoke 的 --bench 用相同操作给出 AOT 侧数据，二者项名一一对应。
/// </remarks>
[MemoryDiagnoser]
public class CodecBenchmarks
{
    private MqCodec _codec;
    private Command _command;
    private Byte[] _frameData;
    private MessageExt _ext;
    private Grpc.GrpcMessage _grpcMessage;
    private Header _header;
    private String _json;

    [GlobalSetup]
    public void Setup()
    {
        _codec = new MqCodec();
        _command = new Command
        {
            Header = new Header
            {
                Code = (Int32)RequestCode.SEND_MESSAGE_V2,
                Opaque = 1234,
                Language = LanguageCode.DOTNET + "",
                Version = MQVersion.V4_8_0,
                Remark = "bench",
            },
        };
        _command.SetBody(new ArrayPacket("hello aot benchmark".GetBytes()));

        using (var built = _codec.Build(_command)) { }
        _frameData = _codec.Build(_command).ReadBytes();

        _ext = new MessageExt { Topic = "bench-topic", Body = "hello aot benchmark".GetBytes(), Tags = "tag" };
        _grpcMessage = _ext.ToGrpcMessage();

        _header = new Header { Code = 10, Opaque = 5, Language = "DOTNET" };
        _header.GetExtFields()["a"] = "pg";
        _json = _header.ToJson();
    }

    [Benchmark]
    public void FrameBuild()
    {
        using var frame = _codec.Build(_command);
    }

    [Benchmark]
    public Boolean FrameParse()
    {
        var rs = _codec.TryParse(new ReadOnlySequence<Byte>(_frameData));
        return rs != null && !rs.Value.Invalid;
    }

    [Benchmark]
    public void GrpcConvert()
    {
        var grpc = _ext.ToGrpcMessage();
        _ = grpc.ToMessage();
    }

    [Benchmark]
    public String JsonSerialize() => _header.ToJson();

    [Benchmark]
    public Header JsonDeserialize() => _json.ToJsonEntity<Header>();
}

/// <summary>基准入口</summary>
public static class Program
{
    /// <summary>运行全部基准</summary>
    /// <param name="args">命令行参数，透传给 BenchmarkSwitcher</param>
    public static void Main(String[] args) => BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
}
