using Xunit;

namespace XUnitTest.Integration;

/// <summary>将所有集成测试放在同一 Collection 中，共享同一个 RocketMqFixture 实例，避免重复建连</summary>
[CollectionDefinition("RocketMQ")]
public class RocketMqCollection : ICollectionFixture<RocketMqFixture> { }
