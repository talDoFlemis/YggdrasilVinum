namespace YggdrasilVinum.Models;

public record struct WineRecord
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
}

public enum WineType
{
    Red,
    White,
    Rose
}
