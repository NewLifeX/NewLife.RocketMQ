using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NewLife.RocketMQ.Protocol;

/// <summary>语言类型</summary>
public enum LanguageCode : Byte
{
    /// <summary></summary>
    JAVA = 0,

    /// <summary></summary>
    CPP = 1,

    /// <summary></summary>
    DOTNET = 2,

    /// <summary></summary>
    PYTHON = 3,

    /// <summary></summary>
    DELPHI = 4,

    /// <summary></summary>
    ERLANG = 5,

    /// <summary></summary>
    RUBY = 6,

    /// <summary></summary>
    OTHER = 7,

    /// <summary></summary>
    HTTP = 8,

    /// <summary></summary>
    GO = 9,

    /// <summary></summary>
    PHP = 10,

    /// <summary></summary>
    OMS = 11,

    /// <summary></summary>
    RUST = 12,
}
