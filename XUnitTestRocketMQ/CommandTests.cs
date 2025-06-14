using System;
using System.IO;
using NewLife;
using NewLife.Data;
using NewLife.RocketMQ.Protocol;
using NewLife.Serialization;
using Xunit;

namespace XUnitTestRocketMQ;

public class CommandTests
{
    [Fact]
    public void DecodeJson()
    {
        var data = """
            00 00 01 06 00 00 00 72 7b 22 63 6f 64 65 22 3a
            33 34 2c 22 6c 61 6e 67 75 61 67 65 22 3a 22 44
            4f 54 4e 45 54 22 2c 22 6f 70 61 71 75 65 22 3a
            31 35 37 35 2c 22 73 65 72 69 61 6c 69 7a 65 54
            79 70 65 43 75 72 72 65 6e 74 52 50 43 22 3a 22
            4a 53 4f 4e 22 2c 22 76 65 72 73 69 6f 6e 22 3a
            33 37 33 2c 22 72 65 6d 61 72 6b 22 3a 22 48 45
            41 52 54 5f 42 45 41 54 22 7d 7b 22 43 6c 69 65
            6e 74 49 44 22 3a 22 31 30 2e 37 2e 36 39 2e 32
            30 35 40 36 33 32 38 34 22 2c 22 43 6f 6e 73 75
            6d 65 72 44 61 74 61 53 65 74 22 3a 5b 5d 2c 22
            50 72 6f 64 75 63 65 72 44 61 74 61 53 65 74 22
            3a 5b 7b 22 47 72 6f 75 70 4e 61 6d 65 22 3a 22
            44 45 46 41 55 4c 54 5f 50 52 4f 44 55 43 45 52
            22 7d 2c 7b 22 47 72 6f 75 70 4e 61 6d 65 22 3a
            22 43 4c 49 45 4e 54 5f 49 4e 4e 45 52 5f 50 52
            4f 44 55 43 45 52 22 7d 5d 7d
            """;
        var ms = new MemoryStream(data.ToHex());

        var cmd = new Command();
        var rs = cmd.Read(ms);
        Assert.True(rs);

        var header = cmd.Header;
        Assert.NotNull(header);
        Assert.Equal((Int32)RequestCode.HEART_BEAT, header.Code);
        Assert.Equal(0, header.Flag);
        Assert.Equal(LanguageCode.DOTNET + "", header.Language);
        Assert.Equal(SerializeType.JSON + "", header.SerializeTypeCurrentRPC);
        Assert.Equal(MQVersion.V4_8_0, header.Version);
        //Assert.Empty(header.Remark);
        Assert.Equal("HEART_BEAT", header.Remark);

        var ext = header.GetExtFields();
        Assert.Empty(ext);

        var pk = cmd.Payload;
        Assert.NotNull(pk);

        var dic = JsonParser.Decode(pk.ToStr());
        Assert.Equal(3, dic.Count);
    }

    [Fact]
    public void DecodeJson2()
    {
        var data = """
            00 00 03 0c 00 00 01 4c 7b 22 63 6f 64 65 22 3a
            33 31 30 2c 22 65 78 74 46 69 65 6c 64 73 22 3a
            7b 22 61 22 3a 22 44 45 46 41 55 4c 54 5f 50 52
            4f 44 55 43 45 52 22 2c 22 62 22 3a 22 49 4f 43
            5f 49 4e 53 5f 50 41 52 53 45 5f 54 4f 50 49 43
            22 2c 22 63 22 3a 22 54 42 57 31 30 32 22 2c 22
            64 22 3a 22 30 22 2c 22 65 22 3a 22 36 22 2c 22
            66 22 3a 22 30 22 2c 22 67 22 3a 22 31 36 39 33
            34 35 39 38 32 32 35 37 33 22 2c 22 68 22 3a 22
            30 22 2c 22 69 22 3a 22 54 41 47 53 01 54 30 32
            30 30 02 4b 45 59 53 01 30 61 30 37 34 35 63 64
            31 36 39 33 34 35 39 38 32 32 35 36 30 62 33 36
            33 65 66 37 33 34 02 57 41 49 54 01 54 72 75 65
            02 22 2c 22 6a 22 3a 22 30 22 2c 22 6b 22 3a 22
            46 61 6c 73 65 22 7d 2c 22 6c 61 6e 67 75 61 67
            65 22 3a 22 44 4f 54 4e 45 54 22 2c 22 6f 70 61
            71 75 65 22 3a 31 35 37 36 2c 22 73 65 72 69 61
            6c 69 7a 65 54 79 70 65 43 75 72 72 65 6e 74 52
            50 43 22 3a 22 4a 53 4f 4e 22 2c 22 76 65 72 73
            69 6f 6e 22 3a 33 37 33 2c 22 72 65 6d 61 72 6b
            22 3a 22 53 45 4e 44 5f 4d 45 53 53 41 47 45 5f
            56 32 22 7d 7b 22 4b 69 6e 64 22 3a 22 54 30 32
            30 30 22 2c 22 56 65 72 73 69 6f 6e 22 3a 22 4a
            54 32 30 31 33 22 2c 22 4d 6f 62 69 6c 65 22 3a
            22 31 34 32 37 30 35 37 39 34 35 37 22 2c 22 53
            65 71 75 65 6e 63 65 22 3a 35 31 35 34 2c 22 42
            6f 64 79 22 3a 7b 22 41 6c 61 72 6d 22 3a 30 2c
            22 53 74 61 74 75 73 22 3a 37 38 36 34 33 35 2c
            22 4c 61 74 69 74 75 64 65 22 3a 33 36 38 37 31
            33 30 30 2c 22 4c 6f 6e 67 69 74 75 64 65 22 3a
            31 31 37 31 32 31 38 33 31 2c 22 41 6c 74 69 74
            75 64 65 22 3a 31 36 2c 22 53 70 65 65 64 22 3a
            30 2c 22 44 69 72 65 63 74 69 6f 6e 22 3a 31 31
            35 2c 22 47 50 53 54 69 6d 65 22 3a 22 32 30 32
            33 2d 30 38 2d 33 31 20 31 33 3a 33 30 3a 31 36
            22 2c 22 41 64 64 69 74 69 6f 6e 61 6c 73 22 3a
            7b 22 e9 87 8c e7 a8 8b 22 3a 32 36 35 30 34 30
            39 2c 22 e9 80 9f e5 ba a6 22 3a 30 2c 22 e8 a7
            86 e9 a2 91 e6 8a a5 e8 ad a6 22 3a 30 2c 22 e8
            a7 86 e9 a2 91 e4 b8 a2 e5 a4 b1 22 3a 30 2c 22
            e8 a7 86 e9 a2 91 e9 81 ae e6 8c a1 22 3a 30 2c
            22 e5 ad 98 e5 82 a8 e6 95 85 e9 9a 9c 22 3a 30
            2c 22 e5 bc 82 e5 b8 b8 e9 a9 be e9 a9 b6 22 3a
            22 41 41 41 41 22 2c 22 e8 bd a6 e8 be 86 e4 bf
            a1 e5 8f b7 22 3a 30 2c 22 e6 a8 a1 e6 8b 9f e9
            87 8f 22 3a 30 2c 22 e4 bf a1 e5 8f b7 e5 bc ba
            e5 ba a6 22 3a 34 2c 22 e5 8d ab e6 98 9f e6 95
            b0 22 3a 33 32 2c 22 31 38 33 22 3a 30 2c 22 32
            34 38 22 3a 30 2c 22 32 33 39 22 3a 30 7d 7d 7d
            """;
        var ms = new MemoryStream(data.ToHex());

        var cmd = new Command();
        var rs = cmd.Read(ms);
        Assert.True(rs);

        var header = cmd.Header;
        Assert.NotNull(header);
        Assert.Equal((Int32)RequestCode.SEND_MESSAGE_V2, header.Code);
        Assert.Equal(0, header.Flag);
        Assert.Equal(LanguageCode.DOTNET + "", header.Language);
        Assert.Equal(SerializeType.JSON + "", header.SerializeTypeCurrentRPC);
        Assert.Equal(MQVersion.V4_8_0, header.Version);
        Assert.Equal("SEND_MESSAGE_V2", header.Remark);

        var ext = header.GetExtFields();
        Assert.NotEmpty(ext);
        Assert.Equal(11, ext.Count);

        var pk = cmd.Payload;
        Assert.NotNull(pk);

        var dic = JsonParser.Decode(pk.ToStr());
        Assert.Equal(5, dic.Count);
    }

    [Fact]
    public void DecodeRocketMQ()
    {
        var data = """
            00 00 00 19 01 00 00 15 00 00 00 01 75 00 00 06
            27 00 00 00 01 00 00 00 00 00 00 00 00
            """;
        var ms = new MemoryStream(data.ToHex());

        var cmd = new Command();
        var rs = cmd.Read(ms);
        Assert.True(rs);

        var header = cmd.Header;
        Assert.NotNull(header);
        Assert.Equal((Int32)ResponseCode.SUCCESS, header.Code);
        Assert.Equal(1, header.Flag);
        Assert.Equal(LanguageCode.JAVA + "", header.Language);
        Assert.Equal(SerializeType.ROCKETMQ + "", header.SerializeTypeCurrentRPC);
        Assert.Equal(MQVersion.V4_8_0, header.Version);
        Assert.Null(header.Remark);

        var ext = header.GetExtFields();
        Assert.Empty(ext);

        var pk = cmd.Payload;
        Assert.Null(pk);
    }

    [Fact]
    public void DecodeRocketMQ2()
    {
        var data = """
            00 00 00 99 01 00 00 95 00 00 00 01 75 00 00 06
            28 00 00 00 01 00 00 00 00 00 00 00 80 00 07 71
            75 65 75 65 49 64 00 00 00 01 36 00 08 54 52 41
            43 45 5f 4f 4e 00 00 00 04 74 72 75 65 00 0a 4d
            53 47 5f 52 45 47 49 4f 4e 00 00 00 0d 44 65 66
            61 75 6c 74 52 65 67 69 6f 6e 00 05 6d 73 67 49
            64 00 00 00 20 30 41 30 39 30 46 32 38 30 30 30
            30 32 41 39 46 30 30 30 30 32 32 39 45 38 39 37
            43 46 33 38 32 00 0b 71 75 65 75 65 4f 66 66 73
            65 74 00 00 00 07 36 33 37 39 38 38 31
            """;
        var ms = new MemoryStream(data.ToHex());

        var cmd = new Command();
        var rs = cmd.Read(ms);
        Assert.True(rs);

        var header = cmd.Header;
        Assert.NotNull(header);
        Assert.Equal((Int32)ResponseCode.SUCCESS, header.Code);
        Assert.Equal(1, header.Flag);
        Assert.Equal(LanguageCode.JAVA + "", header.Language);
        Assert.Equal(SerializeType.ROCKETMQ + "", header.SerializeTypeCurrentRPC);
        Assert.Equal(MQVersion.V4_8_0, header.Version);
        Assert.Null(header.Remark);

        var ext = header.GetExtFields();
        Assert.NotEmpty(ext);
        Assert.Equal(5, ext.Count);

        var pk = cmd.Payload;
        Assert.Null(pk);
    }

    [Fact]
    public void GetRouteInfo_v520_Java()
    {
        var data = """
            00 00 00 8a 00 00 00 86 7b 22 63 6f 64 65 22 3a
            31 30 35 2c 22 65 78 74 46 69 65 6c 64 73 22 3a
            7b 22 74 6f 70 69 63 22 3a 22 54 65 5a 5f 54 65
            73 74 5f 4c 6e 67 22 7d 2c 22 66 6c 61 67 22 3a
            30 2c 22 6c 61 6e 67 75 61 67 65 22 3a 22 4a 41
            56 41 22 2c 22 6f 70 61 71 75 65 22 3a 30 2c 22
            73 65 72 69 61 6c 69 7a 65 54 79 70 65 43 75 72
            72 65 6e 74 52 50 43 22 3a 22 4a 53 4f 4e 22 2c
            22 76 65 72 73 69 6f 6e 22 3a 34 35 33 7d
            """;
        var ms = new MemoryStream(data.ToHex());

        var cmd = new Command();
        var rs = cmd.Read(ms);
        Assert.True(rs);
        Assert.False(cmd.Reply);
        Assert.Equal("""
            {"code":105,"extFields":{"topic":"TeZ_Test_Lng"},"flag":0,"language":"JAVA","opaque":0,"serializeTypeCurrentRPC":"JSON","version":453}
            """, cmd.RawJson);

        var header = cmd.Header;
        Assert.NotNull(header);
        Assert.Equal((Int32)RequestCode.GET_ROUTEINTO_BY_TOPIC, header.Code);
        Assert.Equal(0, header.Flag);
        Assert.Equal(LanguageCode.JAVA + "", header.Language);
        Assert.Equal(SerializeType.JSON + "", header.SerializeTypeCurrentRPC);
        Assert.Equal(MQVersion.V5_2_0, header.Version);
        Assert.Null(header.Remark);

        var ext = header.GetExtFields();
        Assert.Single(ext);
        Assert.Equal("TeZ_Test_Lng", ext["topic"]);

        var pk = cmd.Payload;
        Assert.Null(pk);
    }

    [Fact]
    public void DecodeRouteInfo_v520_Java()
    {
        var data = """
            00 00 01 6a 00 00 00 5f 7b 22 63 6f 64 65 22 3a
            30 2c 22 66 6c 61 67 22 3a 31 2c 22 6c 61 6e 67
            75 61 67 65 22 3a 22 4a 41 56 41 22 2c 22 6f 70
            61 71 75 65 22 3a 30 2c 22 73 65 72 69 61 6c 69
            7a 65 54 79 70 65 43 75 72 72 65 6e 74 52 50 43
            22 3a 22 4a 53 4f 4e 22 2c 22 76 65 72 73 69 6f
            6e 22 3a 34 35 33 7d 7b 22 62 72 6f 6b 65 72 44
            61 74 61 73 22 3a 5b 7b 22 62 72 6f 6b 65 72 41
            64 64 72 73 22 3a 7b 22 30 22 3a 22 31 30 2e 32
            2e 33 2e 31 31 37 3a 31 30 39 31 31 22 7d 2c 22
            62 72 6f 6b 65 72 4e 61 6d 65 22 3a 22 62 72 6f
            6b 65 72 2d 61 22 2c 22 63 6c 75 73 74 65 72 22
            3a 22 44 65 66 61 75 6c 74 43 6c 75 73 74 65 72
            22 2c 22 65 6e 61 62 6c 65 41 63 74 69 6e 67 4d
            61 73 74 65 72 22 3a 66 61 6c 73 65 7d 5d 2c 22
            66 69 6c 74 65 72 53 65 72 76 65 72 54 61 62 6c
            65 22 3a 7b 7d 2c 22 71 75 65 75 65 44 61 74 61
            73 22 3a 5b 7b 22 62 72 6f 6b 65 72 4e 61 6d 65
            22 3a 22 62 72 6f 6b 65 72 2d 61 22 2c 22 70 65
            72 6d 22 3a 36 2c 22 72 65 61 64 51 75 65 75 65
            4e 75 6d 73 22 3a 38 2c 22 74 6f 70 69 63 53 79
            73 46 6c 61 67 22 3a 30 2c 22 77 72 69 74 65 51
            75 65 75 65 4e 75 6d 73 22 3a 38 7d 5d 7d
            """;
        var ms = new MemoryStream(data.ToHex());

        var cmd = new Command();
        var rs = cmd.Read(ms);
        Assert.True(rs);
        Assert.True(cmd.Reply);
        Assert.Equal("""
            {"code":0,"flag":1,"language":"JAVA","opaque":0,"serializeTypeCurrentRPC":"JSON","version":453}
            """, cmd.RawJson);

        var header = cmd.Header;
        Assert.NotNull(header);
        Assert.Equal(0, header.Code);
        Assert.Equal(1, header.Flag);
        Assert.Equal(LanguageCode.JAVA + "", header.Language);
        Assert.Equal(SerializeType.JSON + "", header.SerializeTypeCurrentRPC);
        Assert.Equal(MQVersion.V5_2_0, header.Version);
        Assert.Null(header.Remark);

        var ext = header.GetExtFields();
        Assert.Empty(ext);

        var pk = cmd.Payload;
        Assert.NotNull(pk);

        var target = """
            {"brokerDatas":[{"brokerAddrs":{"0":"10.2.3.117:10911"},"brokerName":"broker-a","cluster":"DefaultCluster","enableActingMaster":false}],"filterServerTable":{},"queueDatas":[{"brokerName":"broker-a","perm":6,"readQueueNums":8,"topicSysFlag":0,"writeQueueNums":8}]}
            """;
        var json = pk.ToStr();
        Assert.Equal(target, json);

        var dic = cmd.ReadBodyAsJson();
        Assert.True(dic.ContainsKey("brokerDatas"));
        Assert.True(dic.ContainsKey("queueDatas"));
    }

    [Fact]
    public void GetRouteInfo_v520_Dotnet()
    {
        var data = """
            00 00 00 a5 00 00 00 a1 7b 22 63 6f 64 65 22 3a
            31 30 35 2c 22 65 78 74 46 69 65 6c 64 73 22 3a
            7b 22 74 6f 70 69 63 22 3a 22 54 65 5a 5f 54 65
            73 74 5f 4c 6e 67 22 7d 2c 22 6c 61 6e 67 75 61
            67 65 22 3a 22 44 4f 54 4e 45 54 22 2c 22 6f 70
            61 71 75 65 22 3a 31 2c 22 73 65 72 69 61 6c 69
            7a 65 54 79 70 65 43 75 72 72 65 6e 74 52 50 43
            22 3a 22 4a 53 4f 4e 22 2c 22 76 65 72 73 69 6f
            6e 22 3a 33 37 33 2c 22 72 65 6d 61 72 6b 22 3a
            22 47 45 54 5f 52 4f 55 54 45 49 4e 54 4f 5f 42
            59 5f 54 4f 50 49 43 22 7d
            """;
        var ms = new MemoryStream(data.ToHex());

        var cmd = new Command();
        var rs = cmd.Read(ms);
        Assert.True(rs);
        Assert.False(cmd.Reply);
        Assert.Equal("""
            {"code":105,"extFields":{"topic":"TeZ_Test_Lng"},"language":"DOTNET","opaque":1,"serializeTypeCurrentRPC":"JSON","version":373,"remark":"GET_ROUTEINTO_BY_TOPIC"}
            """, cmd.RawJson);

        var header = cmd.Header;
        Assert.NotNull(header);
        Assert.Equal((Int32)RequestCode.GET_ROUTEINTO_BY_TOPIC, header.Code);
        Assert.Equal(0, header.Flag);
        Assert.Equal(LanguageCode.DOTNET + "", header.Language);
        Assert.Equal(SerializeType.JSON + "", header.SerializeTypeCurrentRPC);
        Assert.Equal(MQVersion.V4_8_0, header.Version);
        //Assert.Null(header.Remark);
        Assert.Equal("GET_ROUTEINTO_BY_TOPIC", header.Remark);

        var ext = header.GetExtFields();
        Assert.Single(ext);
        Assert.Equal("TeZ_Test_Lng", ext["topic"]);

        var pk = cmd.Payload;
        Assert.Null(pk);
    }

    [Fact]
    public void DecodeRouteInfo_v520_Dotnet()
    {
        var data = """
            00 00 01 68 00 00 00 5f 7b 22 63 6f 64 65 22 3a
            30 2c 22 66 6c 61 67 22 3a 31 2c 22 6c 61 6e 67
            75 61 67 65 22 3a 22 4a 41 56 41 22 2c 22 6f 70
            61 71 75 65 22 3a 31 2c 22 73 65 72 69 61 6c 69
            7a 65 54 79 70 65 43 75 72 72 65 6e 74 52 50 43
            22 3a 22 4a 53 4f 4e 22 2c 22 76 65 72 73 69 6f
            6e 22 3a 34 35 33 7d 7b 22 62 72 6f 6b 65 72 44
            61 74 61 73 22 3a 5b 7b 22 62 72 6f 6b 65 72 41
            64 64 72 73 22 3a 7b 30 3a 22 31 30 2e 32 2e 33
            2e 31 31 37 3a 31 30 39 31 31 22 7d 2c 22 62 72
            6f 6b 65 72 4e 61 6d 65 22 3a 22 62 72 6f 6b 65
            72 2d 61 22 2c 22 63 6c 75 73 74 65 72 22 3a 22
            44 65 66 61 75 6c 74 43 6c 75 73 74 65 72 22 2c
            22 65 6e 61 62 6c 65 41 63 74 69 6e 67 4d 61 73
            74 65 72 22 3a 66 61 6c 73 65 7d 5d 2c 22 66 69
            6c 74 65 72 53 65 72 76 65 72 54 61 62 6c 65 22
            3a 7b 7d 2c 22 71 75 65 75 65 44 61 74 61 73 22
            3a 5b 7b 22 62 72 6f 6b 65 72 4e 61 6d 65 22 3a
            22 62 72 6f 6b 65 72 2d 61 22 2c 22 70 65 72 6d
            22 3a 36 2c 22 72 65 61 64 51 75 65 75 65 4e 75
            6d 73 22 3a 38 2c 22 74 6f 70 69 63 53 79 73 46
            6c 61 67 22 3a 30 2c 22 77 72 69 74 65 51 75 65
            75 65 4e 75 6d 73 22 3a 38 7d 5d 7d
            """;
        var ms = new MemoryStream(data.ToHex());

        var cmd = new Command();
        var rs = cmd.Read(ms);
        Assert.True(rs);
        Assert.True(cmd.Reply);
        Assert.Equal("""
            {"code":0,"flag":1,"language":"JAVA","opaque":1,"serializeTypeCurrentRPC":"JSON","version":453}
            """, cmd.RawJson);

        var header = cmd.Header;
        Assert.NotNull(header);
        Assert.Equal(0, header.Code);
        Assert.Equal(1, header.Flag);
        Assert.Equal(LanguageCode.JAVA + "", header.Language);
        Assert.Equal(SerializeType.JSON + "", header.SerializeTypeCurrentRPC);
        Assert.Equal(MQVersion.V5_2_0, header.Version);
        Assert.Null(header.Remark);

        var ext = header.GetExtFields();
        Assert.Empty(ext);

        var pk = cmd.Payload;
        Assert.NotNull(pk);

        var target = """
            {"brokerDatas":[{"brokerAddrs":{0:"10.2.3.117:10911"},"brokerName":"broker-a","cluster":"DefaultCluster","enableActingMaster":false}],"filterServerTable":{},"queueDatas":[{"brokerName":"broker-a","perm":6,"readQueueNums":8,"topicSysFlag":0,"writeQueueNums":8}]}
            """;
        var json = pk.ToStr();
        Assert.Equal(target, json);

        var dic = cmd.ReadBodyAsJson();
        Assert.True(dic.ContainsKey("brokerDatas"));
        Assert.True(dic.ContainsKey("queueDatas"));
    }

    [Fact]
    public void SendMessageV2_v520_Java()
    {
        var data = """
            00 00 04 09 00 00 01 81 7b 22 63 6f 64 65 22 3a
            33 31 30 2c 22 65 78 74 46 69 65 6c 64 73 22 3a
            7b 22 61 22 3a 22 52 30 31 5f 70 72 6f 64 75 63
            65 72 5f 31 32 33 22 2c 22 62 22 3a 22 54 65 5a
            5f 54 65 73 74 5f 4c 6e 67 22 2c 22 63 22 3a 22
            54 42 57 31 30 32 22 2c 22 64 22 3a 22 34 22 2c
            22 65 22 3a 22 30 22 2c 22 66 22 3a 22 30 22 2c
            22 67 22 3a 22 31 37 34 39 38 38 33 37 37 38 36
            38 36 22 2c 22 68 22 3a 22 30 22 2c 22 69 22 3a
            22 55 4e 49 51 5f 4b 45 59 5c 75 30 30 30 31 32
            34 30 30 44 44 30 32 31 30 30 38 30 30 31 35 32
            42 32 42 37 44 30 34 31 39 44 36 44 44 35 45 39
            45 46 43 31 38 42 34 41 41 43 32 34 36 32 31 32
            41 37 44 30 30 30 30 5c 75 30 30 30 32 57 41 49
            54 5c 75 30 30 30 31 74 72 75 65 5c 75 30 30 30
            32 54 41 47 53 5c 75 30 30 30 31 2a 5c 75 30 30
            30 32 22 2c 22 6a 22 3a 22 30 22 2c 22 6b 22 3a
            22 66 61 6c 73 65 22 2c 22 6d 22 3a 22 66 61 6c
            73 65 22 2c 22 6e 22 3a 22 62 72 6f 6b 65 72 2d
            61 22 7d 2c 22 66 6c 61 67 22 3a 30 2c 22 6c 61
            6e 67 75 61 67 65 22 3a 22 4a 41 56 41 22 2c 22
            6f 70 61 71 75 65 22 3a 32 2c 22 73 65 72 69 61
            6c 69 7a 65 54 79 70 65 43 75 72 72 65 6e 74 52
            50 43 22 3a 22 4a 53 4f 4e 22 2c 22 76 65 72 73
            69 6f 6e 22 3a 34 35 33 7d 5b 31 2e 30 2c 31 35
            2e 30 2c 31 2e 30 31 33 32 35 2c 30 2e 35 2c 35
            37 32 2e 35 2c 31 2e 30 31 33 32 35 2c 31 35 2e
            30 2c 31 2e 30 35 30 37 30 38 31 37 32 2c 32 39
            39 39 2e 39 34 32 37 31 35 2c 31 2e 30 32 35 31
            38 35 37 34 37 2c 32 2e 31 31 35 37 35 37 34 39
            37 2c 39 30 2e 30 2c 31 2e 30 35 30 37 30 38 31
            37 32 2c 32 2e 31 31 35 37 35 37 34 39 37 2c 34
            2e 32 36 30 33 39 33 30 34 31 2c 35 37 32 2e 35
            2c 34 2e 32 36 30 33 39 33 30 34 31 2c 34 32 33
            2e 30 2c 31 38 2e 31 39 2c 31 31 31 31 2e 30 2c
            31 2e 30 2c 31 35 2e 30 2c 31 2e 30 31 33 32 35
            2c 30 2e 35 2c 31 38 2e 36 30 38 32 35 38 31 39
            2c 31 2e 30 31 33 32 35 2c 31 35 2e 30 2c 31 2e
            30 35 30 37 30 38 31 37 32 2c 32 39 39 39 2e 39
            34 32 37 31 35 2c 31 2e 30 32 35 31 38 35 37 34
            37 2c 32 2e 31 31 35 37 35 37 34 39 37 2c 39 30
            2e 30 2c 31 2e 30 35 30 37 30 38 31 37 32 2c 32
            2e 31 31 35 37 35 37 34 39 37 2c 34 2e 32 36 30
            33 39 33 30 34 31 2c 31 38 2e 36 30 38 32 35 38
            31 39 2c 34 32 33 2e 30 2c 34 32 33 2e 30 2c 31
            38 2e 31 39 2c 31 31 31 31 2e 30 2c 31 2e 30 2c
            31 35 2e 30 2c 31 2e 30 31 33 32 35 2c 30 2e 35
            2c 31 38 2e 36 30 38 32 35 38 31 39 2c 31 2e 30
            31 33 32 35 2c 31 35 2e 30 2c 31 2e 30 35 30 37
            30 38 31 37 32 2c 32 39 39 39 2e 39 34 32 37 31
            35 2c 31 2e 30 32 35 31 38 35 37 34 37 2c 32 2e
            31 31 35 37 35 37 34 39 37 2c 39 30 2e 30 2c 31
            2e 30 35 30 37 30 38 31 37 32 2c 32 2e 31 31 35
            37 35 37 34 39 37 2c 34 2e 32 36 30 33 39 33 30
            34 31 2c 35 37 32 2e 35 2c 32 39 39 39 2e 39 34
            32 37 31 35 2c 34 32 33 2e 30 2c 31 38 2e 31 39
            2c 39 30 2e 30 2c 31 2e 30 2c 31 35 2e 30 2c 31
            2e 30 31 33 32 35 2c 30 2e 35 2c 31 38 2e 36 30
            38 32 35 38 31 39 2c 31 2e 30 31 33 32 35 2c 31
            35 2e 30 2c 31 2e 30 35 30 37 30 38 31 37 32 2c
            32 39 39 39 2e 39 34 32 37 31 35 2c 31 2e 30 32
            35 31 38 35 37 34 37 2c 32 2e 31 31 35 37 35 37
            34 39 37 2c 39 30 2e 30 2c 31 2e 30 35 30 37 30
            38 31 37 32 2c 32 2e 31 31 35 37 35 37 34 39 37
            2c 31 2e 30 32 35 31 38 35 37 34 37 5d
            """;
        var ms = new MemoryStream(data.ToHex());

        var cmd = new Command();
        var rs = cmd.Read(ms);
        Assert.True(rs);
        Assert.False(cmd.Reply);
        Assert.Equal("""
            {"code":310,"extFields":{"a":"R01_producer_123","b":"TeZ_Test_Lng","c":"TBW102","d":"4","e":"0","f":"0","g":"1749883778686","h":"0","i":"UNIQ_KEY\u00012400DD02100800152B2B7D0419D6DD5E9EFC18B4AAC246212A7D0000\u0002WAIT\u0001true\u0002TAGS\u0001*\u0002","j":"0","k":"false","m":"false","n":"broker-a"},"flag":0,"language":"JAVA","opaque":2,"serializeTypeCurrentRPC":"JSON","version":453}
            """, cmd.RawJson);

        var header = cmd.Header;
        Assert.NotNull(header);
        Assert.Equal((Int32)RequestCode.SEND_MESSAGE_V2, header.Code);
        Assert.Equal(0, header.Flag);
        Assert.Equal(LanguageCode.JAVA + "", header.Language);
        Assert.Equal(SerializeType.JSON + "", header.SerializeTypeCurrentRPC);
        Assert.Equal(MQVersion.V5_2_0, header.Version);
        Assert.Null(header.Remark);

        var ext = header.GetExtFields();
        Assert.Equal(13, ext.Count);
        Assert.Equal("R01_producer_123", ext["a"]);
        Assert.Equal("TeZ_Test_Lng", ext["b"]);
        Assert.Equal("TBW102", ext["c"]);

        var pk = cmd.Payload;
        Assert.NotNull(pk);

        var json = pk.ToStr();
        Assert.NotEmpty(json);
    }

    [Fact]
    public void DecodeSendMessageV2_v520_Java()
    {
        var data = """
            00 00 01 36 00 00 01 32 7b 22 63 6f 64 65 22 3a
            30 2c 22 65 78 74 46 69 65 6c 64 73 22 3a 7b 22
            71 75 65 75 65 49 64 22 3a 22 30 22 2c 22 74 72
            61 6e 73 61 63 74 69 6f 6e 49 64 22 3a 22 32 34
            30 30 44 44 30 32 31 30 30 38 30 30 31 35 32 42
            32 42 37 44 30 34 31 39 44 36 44 44 35 45 39 45
            46 43 31 38 42 34 41 41 43 32 34 36 32 31 32 41
            37 44 30 30 30 30 22 2c 22 6d 73 67 49 64 22 3a
            22 30 41 30 32 30 33 37 35 30 30 30 30 32 41 39
            46 30 30 30 30 30 30 30 30 30 31 45 43 31 38 46
            43 22 2c 22 54 52 41 43 45 5f 4f 4e 22 3a 22 74
            72 75 65 22 2c 22 4d 53 47 5f 52 45 47 49 4f 4e
            22 3a 22 44 65 66 61 75 6c 74 52 65 67 69 6f 6e
            22 2c 22 71 75 65 75 65 4f 66 66 73 65 74 22 3a
            22 30 22 7d 2c 22 66 6c 61 67 22 3a 31 2c 22 6c
            61 6e 67 75 61 67 65 22 3a 22 4a 41 56 41 22 2c
            22 6f 70 61 71 75 65 22 3a 32 2c 22 73 65 72 69
            61 6c 69 7a 65 54 79 70 65 43 75 72 72 65 6e 74
            52 50 43 22 3a 22 4a 53 4f 4e 22 2c 22 76 65 72
            73 69 6f 6e 22 3a 34 35 33 7d
            """;
        var ms = new MemoryStream(data.ToHex());

        var cmd = new Command();
        var rs = cmd.Read(ms);
        Assert.True(rs);
        Assert.True(cmd.Reply);
        Assert.Equal("""
            {"code":0,"extFields":{"queueId":"0","transactionId":"2400DD02100800152B2B7D0419D6DD5E9EFC18B4AAC246212A7D0000","msgId":"0A02037500002A9F0000000001EC18FC","TRACE_ON":"true","MSG_REGION":"DefaultRegion","queueOffset":"0"},"flag":1,"language":"JAVA","opaque":2,"serializeTypeCurrentRPC":"JSON","version":453}
            """, cmd.RawJson);

        var header = cmd.Header;
        Assert.NotNull(header);
        Assert.Equal(0, header.Code);
        Assert.Equal(1, header.Flag);
        Assert.Equal(LanguageCode.JAVA + "", header.Language);
        Assert.Equal(SerializeType.JSON + "", header.SerializeTypeCurrentRPC);
        Assert.Equal(MQVersion.V5_2_0, header.Version);
        Assert.Null(header.Remark);

        var ext = header.GetExtFields();
        Assert.Equal(6, ext.Count);
        Assert.Equal("0", ext["queueId"]);
        Assert.Equal("2400DD02100800152B2B7D0419D6DD5E9EFC18B4AAC246212A7D0000", ext["transactionId"]);
        Assert.Equal("0A02037500002A9F0000000001EC18FC", ext["msgId"]);
        Assert.Equal("true", ext["TRACE_ON"]);
        Assert.Equal("DefaultRegion", ext["MSG_REGION"]);
        Assert.Equal("0", ext["queueOffset"]);

        var result = new SendResult();
        result.Read(ext);
        Assert.Equal("2400DD02100800152B2B7D0419D6DD5E9EFC18B4AAC246212A7D0000", result.TransactionId);
        Assert.Equal("0A02037500002A9F0000000001EC18FC", result.MsgId);
        Assert.Equal("DefaultRegion", result.RegionId);
        Assert.Equal(0, result.QueueOffset);

        var pk = cmd.Payload;
        Assert.Null(pk);
    }

    [Fact]
    public void SendMessageV2_v520_Dotnet()
    {
        var data = """
            00 00 01 31 00 00 01 1a 7b 22 63 6f 64 65 22 3a
            33 31 30 2c 22 65 78 74 46 69 65 6c 64 73 22 3a
            7b 22 61 22 3a 22 52 30 31 5f 70 72 6f 64 75 63
            65 72 5f 31 32 33 22 2c 22 62 22 3a 22 54 65 5a
            5f 54 65 73 74 5f 4c 6e 67 22 2c 22 63 22 3a 22
            54 42 57 31 30 32 22 2c 22 64 22 3a 22 34 22 2c
            22 65 22 3a 22 30 22 2c 22 66 22 3a 22 30 22 2c
            22 67 22 3a 22 31 37 34 39 38 32 36 35 32 39 37
            31 36 22 2c 22 68 22 3a 22 30 22 2c 22 69 22 3a
            22 54 41 47 53 01 2a 02 57 41 49 54 01 54 72 75
            65 02 22 2c 22 6a 22 3a 22 30 22 2c 22 6b 22 3a
            22 46 61 6c 73 65 22 7d 2c 22 6c 61 6e 67 75 61
            67 65 22 3a 22 44 4f 54 4e 45 54 22 2c 22 6f 70
            61 71 75 65 22 3a 31 2c 22 73 65 72 69 61 6c 69
            7a 65 54 79 70 65 43 75 72 72 65 6e 74 52 50 43
            22 3a 22 4a 53 4f 4e 22 2c 22 76 65 72 73 69 6f
            6e 22 3a 33 37 33 2c 22 72 65 6d 61 72 6b 22 3a
            22 53 45 4e 44 5f 4d 45 53 53 41 47 45 5f 56 32
            22 7d 32 30 32 35 2d 30 36 2d 31 33 20 32 32 3a
            35 34 3a 31 32
            """;
        var ms = new MemoryStream(data.ToHex());

        var cmd = new Command();
        var rs = cmd.Read(ms);
        Assert.True(rs);
        Assert.False(cmd.Reply);
        Assert.Equal("""
            {"code":310,"extFields":{"a":"R01_producer_123","b":"TeZ_Test_Lng","c":"TBW102","d":"4","e":"0","f":"0","g":"1749826529716","h":"0","i":"TAGS*WAITTrue","j":"0","k":"False"},"language":"DOTNET","opaque":1,"serializeTypeCurrentRPC":"JSON","version":373,"remark":"SEND_MESSAGE_V2"}
            """, cmd.RawJson);

        var header = cmd.Header;
        Assert.NotNull(header);
        Assert.Equal((Int32)RequestCode.SEND_MESSAGE_V2, header.Code);
        Assert.Equal(0, header.Flag);
        Assert.Equal(LanguageCode.DOTNET + "", header.Language);
        Assert.Equal(SerializeType.JSON + "", header.SerializeTypeCurrentRPC);
        Assert.Equal(MQVersion.V4_8_0, header.Version);
        //Assert.Null(header.Remark);
        Assert.Equal("SEND_MESSAGE_V2", header.Remark);

        var ext = header.GetExtFields();
        //Assert.Equal(13, ext.Count);
        Assert.Equal("R01_producer_123", ext["a"]);
        Assert.Equal("TeZ_Test_Lng", ext["b"]);
        Assert.Equal("TBW102", ext["c"]);

        var pk = cmd.Payload;
        Assert.NotNull(pk);

        var json = pk.ToStr();
        Assert.Equal("2025-06-13 22:54:12", json);
    }
}
