using Microsoft.Extensions.Configuration;
using Serilog;
using YggdrasilVinum.Buffer;
using YggdrasilVinum.Index;
using YggdrasilVinum.Models;
using YggdrasilVinum.Parsers;
using YggdrasilVinum.Storage;

namespace YggdrasilVinum.Services;

/// <summary>
///     Factory class that creates application components and configures the application
/// </summary>
public static class ApplicationFactory
{
    /// <summary>
    ///     Configures Serilog based on application settings
    /// </summary>
    public static void ConfigureSerilog()
    {
        var configuration = new ConfigurationBuilder()
            .SetBasePath(AppDomain.CurrentDomain.BaseDirectory)
            .AddJsonFile("appsettings.json")
            .Build();

        Log.Logger = new LoggerConfiguration()
            .ReadFrom.Configuration(configuration)
            .CreateLogger();

        Log.Information("Configured logging for YggdrasilVinum B+ Tree Database Application");
    }

    /// <summary>
    ///     Parses and loads wine data
    /// </summary>
    public static Result<List<WineRecord>, WineDataParser.ParseError> LoadWineData(string wineDataPath)
    {
        Log.Information("Loading wine data from: {WineDataPath}", wineDataPath);

        var wineResult = WineDataParser.ParseCsvFile(wineDataPath);

        if (wineResult.IsError)
        {
            var error = wineResult.GetErrorOrThrow();
            Log.Error("Failed to parse wine data: {ErrorMessage}", error.Message);
        }
        else
        {
            var wines = wineResult.GetValueOrThrow();
            Log.Information("Successfully parsed {Count} wine records", wines.Count);
        }

        return wineResult;
    }

    /// <summary>
    ///     Creates and initializes a B+ tree
    /// </summary>
    public static IBPlusTreeIndex<TKey, TValue> CreateBPlusTree<TKey, TValue>(string indexPath, int degree)
        where TKey : IComparable<TKey>
        where TValue : IParsable<TValue>
    {
        return new BPlusTreeIndex<TKey, TValue>(indexPath, degree);
    }


    public static IFileManager CreateFileManager(string storagePath, ulong heapSizeInBytes, ulong pageSizeInBytes)
    {
        var sequentialFileManager = new SequentialHeapFileManager(storagePath, heapSizeInBytes, pageSizeInBytes);

        return sequentialFileManager;
    }

    public static IBufferManager CreateBufferManager(
        IFileManager fileManager,
        ulong amountOfPageFrames,
        ulong amountOfIndexFrames
    )
    {
        var bufferManager = new LruBufferManager(fileManager, amountOfPageFrames, amountOfIndexFrames);
        return bufferManager;
    }

    /// <summary>
    ///     Creates and configures a WineProcessor for binary search operations.
    /// </summary>
    public static IWineProcessor CreateWineProcessor(string processedFilePath)
    {
        Log.Information("Creating wine processor with file path: {ProcessedFilePath}", processedFilePath);
        return new WineProcessor(processedFilePath);
    }

    /// <summary>
    ///     Creates a HarvestYearSearchProcessor that uses the WineProcessor for binary search.
    /// </summary>
    public static HarvestYearSearchProcessor CreateHarvestYearSearchProcessor(string processedFilePath)
    {
        var wineProcessor = CreateWineProcessor(processedFilePath);
        Log.Information("Creating harvest year search processor");
        return new HarvestYearSearchProcessor(wineProcessor);
    }

    /// <summary>
    ///     Creates a HarvestYearSearchProcessor with a custom wine processor (useful for testing).
    /// </summary>
    public static HarvestYearSearchProcessor CreateHarvestYearSearchProcessor(IWineProcessor wineProcessor)
    {
        Log.Information("Creating harvest year search processor with custom wine processor");
        return new HarvestYearSearchProcessor(wineProcessor);
    }

    /// <summary>
    ///     Preprocesses wine data for binary search operations.
    ///     This should be called during application setup/initialization.
    /// </summary>
    public static async Task<Result<Unit, WineProcessorError>> PreprocessWineDataAsync(
        string csvFilePath,
        string processedFilePath)
    {
        Log.Information("Preprocessing wine data from {CsvFilePath} to {ProcessedFilePath}",
            csvFilePath, processedFilePath);

        var wineProcessor = CreateWineProcessor(processedFilePath);

        try
        {
            var result = await wineProcessor.ProcessCsvFileAsync(csvFilePath);
            if (result.IsError)
                Log.Error("Failed to preprocess wine data: {Error}", result.GetErrorOrThrow().Message);
            else
                Log.Information("Successfully preprocessed {RecordCount} wine records", wineProcessor.RecordCount);

            return result;
        }
        finally
        {
            wineProcessor.Dispose();
        }
    }
}
