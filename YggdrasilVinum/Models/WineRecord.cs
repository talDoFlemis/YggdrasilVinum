using System.Diagnostics.CodeAnalysis;

namespace YggdrasilVinum.Models;

public record struct WineRecord : IParsable<WineRecord>
{
    public WineRecord(int wineId, string label, int harvestYear, WineType type)
    {
        if (string.IsNullOrWhiteSpace(label))
            throw new ArgumentException("Label cannot be empty", nameof(label));
        if (harvestYear <= 0)
            throw new ArgumentException("Harvest year must be positive", nameof(harvestYear));

        WineId = wineId;
        Label = label;
        HarvestYear = harvestYear;
        Type = type;
    }

    public int WineId { get; set; }
    public string Label { get; set; }
    public int HarvestYear { get; set; }
    public WineType Type { get; set; }

    public static WineRecord Parse(string s, IFormatProvider? provider)
    {
        if (string.IsNullOrWhiteSpace(s))
            throw new ArgumentException("Input string cannot be null or empty", nameof(s));

        var parts = s.Split(',');
        if (parts.Length != 4)
            throw new FormatException("Input string must contain exactly 4 comma-separated values");

        return new WineRecord(
            int.Parse(parts[0]),
            parts[1],
            int.Parse(parts[2]),
            Enum.Parse<WineType>(parts[3])
        );
    }

    public static bool TryParse([NotNullWhen(true)] string? s, IFormatProvider? provider, out WineRecord result)
    {
        result = default;

        if (string.IsNullOrWhiteSpace(s))
            return false;

        var parts = s.Split(',');
        if (parts.Length != 4)
            return false;

        try
        {
            result = new WineRecord(
                int.Parse(parts[0]),
                parts[1],
                int.Parse(parts[2]),
                Enum.Parse<WineType>(parts[3])
            );
            return true;
        }
        catch
        {
            return false;
        }
    }
}

public enum WineType
{
    Red,
    White,
    Rose
}
