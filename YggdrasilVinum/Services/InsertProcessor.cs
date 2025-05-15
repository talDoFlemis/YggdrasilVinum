using Serilog;
using YggdrasilVinum.Buffer;
using YggdrasilVinum.Index;
using YggdrasilVinum.Models;
using YggdrasilVinum.Storage;

namespace YggdrasilVinum.Services;

public class InsertProcessor(
    IBufferManager bufferManager,
    IFileManager fileManager,
    IBPlusTreeIndex<int> bPlusTree
)
{
    private readonly ILogger _logger = Log.ForContext<InsertProcessor>();

    public async Task<Result<Unit, InsertError>> ExecuteAsync(
        WineRecord record
    )
    {
        _logger.Debug("Inserting record: {@Record}", record);

        var randomPageResult = await bufferManager.GetRandomPageAsync();

        if (randomPageResult.IsError)
        {
            var error = randomPageResult.GetErrorOrThrow();
            _logger.Error("Failed to get random page: {@Error}", error);

            return await Task.FromResult(Result<Unit, InsertError>.Error(
                new InsertError("Failed to get random page")));
        }

        var page = randomPageResult.GetValueOrThrow();

        var hasSpaceResult = await PageHasEnoughSpaceToInsertRecord(page, record);
        if (hasSpaceResult.IsError)
            return await Task.FromResult(Result<Unit, InsertError>.Error(hasSpaceResult.GetErrorOrThrow()));

        var hasSpace = hasSpaceResult.GetValueOrThrow();

        if (!hasSpace)
        {
            var allocateNewPageResult = await AllocateNewPage();
            if (allocateNewPageResult.IsError)
                return await Task.FromResult(Result<Unit, InsertError>.Error(allocateNewPageResult.GetErrorOrThrow()));

            page = allocateNewPageResult.GetValueOrThrow();
        }

        page.Content = page.Content.Append(record).ToArray();

        var putPageResult = await bufferManager.PutPageAsync(page);
        if (putPageResult.IsError)
        {
            var error = putPageResult.GetErrorOrThrow();
            _logger.Error("Failed to put page: {@Error}", error);

            return await Task.FromResult(Result<Unit, InsertError>.Error(
                new InsertError("Failed to put page")));
        }

        _logger.Information("Inserted record with success: {@Record}", record);

        var insertInTreeResult = await bPlusTree.InsertAsync(record.HarvestYear, page.PageId);
        if (insertInTreeResult.IsError)
        {
            var error = insertInTreeResult.GetErrorOrThrow();
            _logger.Error("Failed to insert record into B+ tree: {@Error}", error);
            return await Task.FromResult(Result<Unit, InsertError>.Error(
                new InsertError("Failed to insert record into B+ tree")));
        }

        _logger.Information("Inserted page into B+ tree with success: {@page}", page);
        return await Task.FromResult(Result<Unit, InsertError>.Success(Unit.Value));
    }

    private async Task<Result<bool, InsertError>> PageHasEnoughSpaceToInsertRecord(Page page, WineRecord record)
    {
        _logger.Debug("Checking if page has enough space: {@Page}", page);
        var hasSpaceResult = await fileManager.PageHasEnoughSpaceToInsertRecord(page, record);
        if (hasSpaceResult.IsError)
        {
            var error = hasSpaceResult.GetErrorOrThrow();
            _logger.Error("Failed to check if page has enough space: {@Error}", error);

            return await Task.FromResult(Result<bool, InsertError>.Error(
                new InsertError($"Failed to check if page has enough space: {error}")));
        }

        var hasSpace = hasSpaceResult.GetValueOrThrow();

        return await Task.FromResult(Result<bool, InsertError>.Success(hasSpace));
    }

    private async Task<Result<Page, InsertError>> AllocateNewPage()
    {
        _logger.Debug("Allocating new page");

        var newPageResult = await fileManager.AllocateNewPageAsync();
        if (newPageResult.IsError)
        {
            var error = newPageResult.GetErrorOrThrow();
            _logger.Error("Failed to allocate new page: {@Error}", error);

            return await Task.FromResult(
                Result<Page, InsertError>.Error(new InsertError($"Failed to allocate new page {error}")));
        }

        var newPage = newPageResult.GetValueOrThrow();

        _logger.Information("Allocated new page with ID: {@PageId}", newPage.PageId);
        return await Task.FromResult(Result<Page, InsertError>.Success(newPage));
    }
}

public readonly struct InsertError
{
    public string Message { get; }

    public InsertError(string message)
    {
        Message = message;
    }
}
