namespace NewLife.RocketMQ.Protocol;

/// <summary>响应异常</summary>
public class ResponseException : Exception
{
    /// <summary>响应代码</summary>
    public ResponseCode Code { get; set; }

    /// <summary>实例化响应异常</summary>
    /// <param name="code"></param>
    /// <param name="message"></param>
    public ResponseException(ResponseCode code, String message) : base(code + ": " + message) => Code = code;
}