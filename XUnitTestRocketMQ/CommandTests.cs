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
        Assert.Equal("""
            {"code":105,"extFields":{"topic":"TeZ_Test_Lng"},"language":"DOTNET","opaque":1,"serializeTypeCurrentRPC":"JSON","version":373,"remark":"GET_ROUTEINTO_BY_TOPIC"}
            """, cmd.RawJson);

        var header = cmd.Header;
        Assert.NotNull(header);
        Assert.Equal((Int32)RequestCode.GET_ROUTEINTO_BY_TOPIC, header.Code);
        Assert.Equal(0, header.Flag);
        Assert.Equal(LanguageCode.DOTNET + "", header.Language);
        Assert.Equal(SerializeType.JSON + "", header.SerializeTypeCurrentRPC);
        Assert.Equal(MQVersion.V5_2_0, header.Version);
        Assert.Null(header.Remark);

        var ext = header.GetExtFields();
        Assert.Single(ext);
        Assert.Equal("TeZ_Test_Lng", ext["topic"]);

        var pk = cmd.Payload;
        Assert.Null(pk);
    }
}
