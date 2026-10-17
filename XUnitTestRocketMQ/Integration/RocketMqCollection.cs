using Xunit;

namespace XUnitTest.Integration;

/// <summary>将所有集成测试放在同一 Collection 中，共享同一个 RocketMqFixture 实例，避免重复建连</summary>
/// <remarks>集成测试共享真实 Broker 的固定主题与消费组，必须串行执行，故禁用与其它集合的并行。</remarks>
[CollectionDefinition("RocketMQ", DisableParallelization = true)]
public class RocketMqCollection : ICollectionFixture<RocketMqFixture> { }
