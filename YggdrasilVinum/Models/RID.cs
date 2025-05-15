using System.Diagnostics.CodeAnalysis;

namespace YggdrasilVinum.Models;

public struct RID(ulong pageId, uint pageCount) : IParsable<RID>
{
    public ulong pageId { get; } = pageId;
    public uint pageCount { get; } = pageCount;

    public static RID Parse(string s, IFormatProvider? provider)
    {
        if (string.IsNullOrEmpty(s))
            throw new ArgumentNullException(nameof(s));

        if (s.Length < 2)
            throw new FormatException("Invalid RID format");

        var parts = s.Split(':');
        if (parts.Length != 2)
            throw new FormatException("Invalid RID format");

        if (!ulong.TryParse(parts[0], out var pageId))
            throw new FormatException("Invalid page ID format");

        if (!uint.TryParse(parts[1], out var pageCount))
            throw new FormatException("Invalid page count format");

        return new RID(pageId, pageCount);
    }

    public static bool TryParse([NotNullWhen(true)] string? s, IFormatProvider? provider, out RID result)
    {
        try
        {
            result = Parse(s, null);
            return true;
        }
        catch (FormatException)
        {
            result = default;
            return false;
        }
    }

    public override string ToString()
    {
        return $"{pageId}:{pageCount}";
    }
}
