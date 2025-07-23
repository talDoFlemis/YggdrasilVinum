using Serilog;
using YggdrasilVinum.Models;
using YggdrasilVinum.Services.Facades;
using YggdrasilVinum.Services.Processing;

namespace YggdrasilVinum.Services.CommandProcessing;

/// <summary>
///     Simplified insert processor that uses StorageFacade.
///     Demonstrates the benefits of the Facade pattern by reducing dependencies
///     and simplifying the insertion logic.
/// </summary>
public class SimplifiedInsertProcessor
{
    private readonly ILogger _logger = Log.ForContext<SimplifiedInsertProcessor>();
    private readonly StorageFacade _storageFacade;

    public SimplifiedInsertProcessor(StorageFacade storageFacade)
    {
        _storageFacade = storageFacade ?? throw new ArgumentNullException(nameof(storageFacade));
    }

    /// <summary>
    ///     Executes the wine record insertion using the storage facade.
    ///     Much simpler than the original InsertProcessor that needed to coordinate
    ///     multiple storage components directly.
    /// </summary>
    /// <param name="record">The wine record to insert</param>
    /// <returns>Result indicating success or failure</returns>
    public async Task<Result<RID, InsertError>> ExecuteAsync(WineRecord record)
    {
        _logger.Debug("Inserting wine record using storage facade: {@Record}", record);

        var result = await _storageFacade.InsertWineRecordAsync(record);

        if (result.IsError)
        {
            var error = result.GetErrorOrThrow();
            _logger.Error("Failed to insert wine record: {@Error}", error);
            return Result<RID, InsertError>.Error(new InsertError(error.Message));
        }

        var rid = result.GetValueOrThrow();
        _logger.Information("Successfully inserted wine record with RID: {@RID}", rid);

        return Result<RID, InsertError>.Success(rid);
    }
}

/// <summary>
///     Simplified search processor that uses StorageFacade.
///     Demonstrates how the Facade pattern simplifies search operations
///     by encapsulating the coordination between index and storage.
/// </summary>
public class SimplifiedSearchProcessor
{
    private readonly ILogger _logger = Log.ForContext<SimplifiedSearchProcessor>();
    private readonly StorageFacade _storageFacade;

    public SimplifiedSearchProcessor(StorageFacade storageFacade)
    {
        _storageFacade = storageFacade ?? throw new ArgumentNullException(nameof(storageFacade));
    }

    /// <summary>
    ///     Executes the search for wine records by harvest year using the storage facade.
    ///     Much simpler than the original EqualitySearchProcessor that needed to coordinate
    ///     B+ tree searches and buffer manager operations directly.
    /// </summary>
    /// <param name="harvestYear">The harvest year to search for</param>
    /// <returns>Result containing the found wine records</returns>
    public async Task<Result<WineRecord[], SearchError>> ExecuteAsync(int harvestYear)
    {
        _logger.Debug("Searching for wine records by harvest year using storage facade: {HarvestYear}", harvestYear);

        var result = await _storageFacade.FindRecordsByYearAsync(harvestYear);

        if (result.IsError)
        {
            var error = result.GetErrorOrThrow();
            _logger.Error("Failed to search for wine records: {@Error}", error);
            return Result<WineRecord[], SearchError>.Error(new SearchError(error.Message));
        }

        var records = result.GetValueOrThrow().ToArray();
        _logger.Information("Successfully found {Count} wine records for harvest year {HarvestYear}",
            records.Length, harvestYear);

        return Result<WineRecord[], SearchError>.Success(records);
    }
}
