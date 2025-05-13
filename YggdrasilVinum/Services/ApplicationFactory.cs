using Microsoft.Extensions.Configuration;
using Serilog;
using YggdrasilVinum.Models;
using YggdrasilVinum.Parsers;

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
    ///     Creates and initializes a B+ tree with wine data
    /// </summary>
    public static BPlusTree<int, WineRecord> CreateBPlusTree(List<WineRecord> wines, int pageSize)
    {
        var bPlusTree = new BPlusTree<int, WineRecord>(pageSize);

        // Insert all wines into the B+ tree
        foreach (var wine in wines)
        {
            bPlusTree.Insert(wine.WineId, wine);
            Log.Debug("Inserted wine: {WineId} - {Label}", wine.WineId, wine.Label);
        }

        return bPlusTree;
    }
}
