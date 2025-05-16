using Serilog;
using YggdrasilVinum.Buffer;
using YggdrasilVinum.Index;
using YggdrasilVinum.Models;

namespace YggdrasilVinum.Services;

public class EqualitySearchProcessor(
    IBufferManager bufferManager,
    IBPlusTreeIndex<int, RID> bPlusTree
)
{
    private readonly ILogger _logger = Log.ForContext<EqualitySearchProcessor>();

    public async Task<Result<WineRecord[], SearchError>> ExecuteAsync(
        int harvestYear
    )
    {
        _logger.Debug("Searching for records with harvest year: {@HarvestYear}", harvestYear);

        var searchResult = await bPlusTree.SearchAsync(harvestYear);
        if (searchResult.IsError)
        {
            var error = searchResult.GetErrorOrThrow();
            _logger.Error("Failed to search for harvest year: {@Error}", error);

            return await Task.FromResult(Result<WineRecord[], SearchError>.Error(
                new SearchError("Failed to search for harvest year")));
        }

        var rids = searchResult.GetValueOrThrow();

        _logger.Information("Found {@size} records with harvest year: {@HarvestYear}", rids.Count, harvestYear);

        List<WineRecord> allWineRecords = [];
        foreach (var rid in rids)
        {
            _logger.Debug("Processing page ID: {@PageId}", rid);
            var recordResult = await ProcessPageAsync(rid);

            if (recordResult.IsError)
            {
                var error = recordResult.GetErrorOrThrow();
                _logger.Error("Failed to process page: {@Error}", error);

                return await Task.FromResult(Result<WineRecord[], SearchError>.Error(
                    new SearchError("Failed to process page")));
            }

            allWineRecords.Add(recordResult.GetValueOrThrow());
        }

        return await Task.FromResult(Result<WineRecord[], SearchError>.Success(allWineRecords.ToArray()));
    }

    private async Task<Result<WineRecord, SearchError>> ProcessPageAsync(RID rid)
    {
        var result = await bufferManager.LoadPageAsync(rid.pageId);
        if (result.IsError)
        {
            var error = result.GetErrorOrThrow();
            _logger.Error("Failed to load page: {@Error}", error);

            return await Task.FromResult(
                Result<WineRecord, SearchError>.Error(new SearchError("Failed to load page")));
        }

        var page = result.GetValueOrThrow();
        var foundRecords = page.Content[rid.pageCount];

        return await Task.FromResult(Result<WineRecord, SearchError>.Success(foundRecords));
    }
}

public readonly struct SearchError(string message)
{
    public string Message { get; } = message;
}
