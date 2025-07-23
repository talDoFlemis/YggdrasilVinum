namespace YggdrasilVinum.Services.Configuration;

/// <summary>
///     Configuration class for application services
/// </summary>
public class ApplicationConfiguration
{
    public string StoragePath { get; init; } = "./storage";
    public ulong HeapSizeInBytes { get; init; } = 40 * 1024 * 1024; // 40 MB
    public ulong PageSizeInBytes { get; init; } = 4096;
    public ulong AmountOfPageFrames { get; init; } = 1;
    public ulong AmountOfIndexFrames { get; init; } = 1;
    public string IndexPath { get; init; } = "./storage/index.txt";
    public int MaxNumberOfKeysPerNode { get; init; } = 4;
    public string ProcessedWinesPath { get; init; } = "./storage/processed_wines.txt";
}
