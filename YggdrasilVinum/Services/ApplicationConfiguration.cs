namespace YggdrasilVinum.Services;

/// <summary>
/// Configuration class for application services
/// </summary>
public class ApplicationConfiguration
{
    public string StoragePath { get; set; } = "./storage";
    public ulong HeapSizeInBytes { get; set; } = 40 * 1024 * 1024; // 40 MB
    public ulong PageSizeInBytes { get; set; } = 4096;
    public ulong AmountOfPageFrames { get; set; } = 1;
    public ulong AmountOfIndexFrames { get; set; } = 1;
    public string IndexPath { get; set; } = "./storage/index.txt";
    public int MaxNumberOfKeysPerNode { get; set; } = 4;
    public string ProcessedWinesPath { get; set; } = "./storage/processed_wines.txt";
}
