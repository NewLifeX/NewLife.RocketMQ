using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NewLife.RocketMQ.Protocol;
using Xunit;

namespace XUnitTestRocketMQ;

public class MQVersionTests
{
    [Fact]
    public void Test1()
    {
        var ver = (MQVersion)275;
        Assert.Equal("V4_3_1", ver.ToString());

        ver = (MQVersion)373;
        Assert.Equal("V4_8_0", ver.ToString());
    }
}
