using System;
using System.Collections.Generic;
using NewLife.RocketMQ.Protocol;
using NewLife.Serialization;
using Xunit;

namespace XUnitTest.Protocol;

/// <summary>心跳请求体编码测试。必须与 Java Broker fastjson 期望的 camelCase 字段一致（issue #118）</summary>
[System.ComponentModel.DisplayName("心跳编码测试")]
public class HeartbeatEncodeTests
{
    /// <summary>与 BrokerClient.Ping 相同的序列化方式：camelCase 字段名</summary>
    private static String BuildJson(HeartbeatData hb) => hb.ToJson(false, false, true);

    [Fact]
    [System.ComponentModel.DisplayName("HeartbeatData_顶层字段为camelCase")]
    public void Heartbeat_TopFields_CamelCase()
    {
        var hb = new HeartbeatData
        {
            ClientID = "client-001",
            ConsumerDataSet = [new ConsumerData { GroupName = "CG1" }],
            ProducerDataSet = [new ProducerData { GroupName = "PG1" }],
        };

        var json = BuildJson(hb);

        Assert.Contains("\"clientID\":\"client-001\"", json);
        Assert.Contains("\"consumerDataSet\":", json);
        Assert.Contains("\"producerDataSet\":", json);
        Assert.Contains("\"groupName\":", json);

        // 不允许出现 PascalCase 字段名
        Assert.DoesNotContain("\"ClientID\"", json);
        Assert.DoesNotContain("\"ConsumerDataSet\"", json);
        Assert.DoesNotContain("\"GroupName\"", json);
    }

    [Fact]
    [System.ComponentModel.DisplayName("ConsumerData_嵌套字段为camelCase")]
    public void ConsumerData_NestedFields_CamelCase()
    {
        var hb = new HeartbeatData
        {
            ClientID = "client-001",
            ConsumerDataSet =
            [
                new ConsumerData
                {
                    GroupName = "CG1",
                    ConsumeFromWhere = "CONSUME_FROM_LAST_OFFSET",
                    ConsumeType = "CONSUME_ACTIVELY",
                    MessageModel = "CLUSTERING",
                    SubscriptionDataSet =
                    [
                        new SubscriptionData { Topic = "topic1", SubString = "*", ExpressionType = "TAG", TagsSet = ["*"] },
                    ],
                },
            ],
            ProducerDataSet = [new ProducerData { GroupName = "PG1" }],
        };

        var json = BuildJson(hb);

        Assert.Contains("\"groupName\":\"CG1\"", json);
        Assert.Contains("\"consumeFromWhere\":\"CONSUME_FROM_LAST_OFFSET\"", json);
        Assert.Contains("\"consumeType\":\"CONSUME_ACTIVELY\"", json);
        Assert.Contains("\"messageModel\":\"CLUSTERING\"", json);
        Assert.Contains("\"subscriptionDataSet\":", json);
        Assert.Contains("\"topic\":\"topic1\"", json);
        Assert.Contains("\"subString\":\"*\"", json);
        Assert.Contains("\"expressionType\":\"TAG\"", json);

        Assert.DoesNotContain("\"ConsumeFromWhere\"", json);
        Assert.DoesNotContain("\"SubscriptionDataSet\"", json);
    }

    [Fact]
    [System.ComponentModel.DisplayName("心跳JSON_键名与Java5.2样例一致")]
    public void Heartbeat_Keys_SameAsJava()
    {
        // Java 5.2 真实心跳样例（参照 XUnitTest.Protocol.CommandTests.HeartBeat_v520_Java 解码出的 body）
        const String java = """
            {"clientID":"10.1.5.9@40700#955220087822500","consumerDataSet":[],"heartbeatFingerprint":0,"producerDataSet":[{"groupName":"CLIENT_INNER_PRODUCER"},{"groupName":"R01_producer_123"}],"withoutSub":false}
            """;
        Assert.Contains("\"clientID\"", java);
        Assert.Contains("\"consumerDataSet\"", java);
        Assert.Contains("\"producerDataSet\"", java);

        // .NET 侧序列化（Java 5.x 额外的 heartbeatFingerprint/withoutSub 为可选，V1 心跳可不带）
        var hb = new HeartbeatData
        {
            ClientID = "10.1.5.9@40700#955220087822500",
            ConsumerDataSet = [new ConsumerData { GroupName = "CG1" }],
            ProducerDataSet = [new ProducerData { GroupName = "CLIENT_INNER_PRODUCER" }, new ProducerData { GroupName = "R01_producer_123" }],
        };
        var json = BuildJson(hb);

        // JsonParser 反序列化的字典大小写不敏感，此处直接对原始字符串做大小写敏感断言
        Assert.Contains("\"clientID\"", json);
        Assert.Contains("\"consumerDataSet\"", json);
        Assert.Contains("\"producerDataSet\"", json);
        Assert.DoesNotContain("\"ClientID\"", json);
        Assert.DoesNotContain("\"ConsumerDataSet\"", json);
    }
}
