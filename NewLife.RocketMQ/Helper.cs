namespace NewLife.RocketMQ;

static class Helper
{
    public static TEnum ToEnum<TEnum>(this String value, TEnum defaultValue = default) where TEnum : struct => Enum.TryParse<TEnum>(value, out var v) ? v : defaultValue;
}
