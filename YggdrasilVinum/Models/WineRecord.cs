namespace YggdrasilVinum.Models;

public readonly struct WineRecord
{
    public readonly int WineId;
    public readonly string Label;
    public readonly int HarvestYear;
    public readonly WineType Type;

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
}

public enum WineType
{
    Red,
    White,
    Rose
}