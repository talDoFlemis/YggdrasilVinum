using Serilog;
using YggdrasilVinum.Models;

namespace YggdrasilVinum.Services;

public class Database(InsertProcessor insertProcessor, EqualitySearchProcessor equalitySearchProcessor)
{
    private readonly ILogger _logger = Log.ForContext<Database>();
    private ulong _recordsInserted;

    public async Task<Result<Unit, InsertError>> InsertAsync(
        WineRecord record
    )
    {
        _logger.Debug("Inserting record: {@Record}", record);
        var result = await insertProcessor.ExecuteAsync(record);

        if (result.IsError)
        {
            var error = result.GetErrorOrThrow();
            _logger.Error("Failed to insert record: {@Error}", error);
            return await Task.FromResult(Result<Unit, InsertError>.Error(error));
        }

        var value = result.GetValueOrThrow();
        _logger.Information("Successfully inserted record: {@Record}", record);
        _recordsInserted++;

        return await Task.FromResult(Result<Unit, InsertError>.Success(value));
    }

    public async Task<Result<WineRecord[], SearchError>> SearchAsync(
        int harvestYear
    )
    {
        _logger.Debug("Searching for records with harvest year: {@HarvestYear}", harvestYear);
        var result = await equalitySearchProcessor.ExecuteAsync(harvestYear);

        if (result.IsError)
        {
            var error = result.GetErrorOrThrow();
            _logger.Error("Failed to search for records: {@Error}", error);
            return await Task.FromResult(Result<WineRecord[], SearchError>.Error(error));
        }

        var value = result.GetValueOrThrow();
        _logger.Information("Successfully found records: {@Records}", value);

        return await Task.FromResult(Result<WineRecord[], SearchError>.Success(value));
    }

    public ulong GetRecordsInsertedCount()
    {
        return _recordsInserted;
    }
}
