using Serilog;
using YggdrasilVinum.Index;
using YggdrasilVinum.Models;
using YggdrasilVinum.Storage;

namespace YggdrasilVinum.Services;

/// <summary>
///     Facade pattern implementation that provides a unified interface for storage operations.
///     Simplifies interaction with the storage subsystem by encapsulating the complexity
///     of coordinating IBPlusTreeIndex and IFileManager operations.
/// </summary>
public class StorageFacade
{
    private readonly IFileManager _heapFile;
    private readonly IBPlusTreeIndex<int, RID> _index;
    private readonly ILogger _logger = Log.ForContext<StorageFacade>();

    public StorageFacade(IBPlusTreeIndex<int, RID> index, IFileManager heapFile)
    {
        _index = index ?? throw new ArgumentNullException(nameof(index));
        _heapFile = heapFile ?? throw new ArgumentNullException(nameof(heapFile));
    }

    /// <summary>
    ///     Inserts a wine record into the storage system.
    ///     Handles both heap file insertion and B+ tree indexing in a single operation.
    /// </summary>
    /// <param name="record">The wine record to insert</param>
    /// <returns>Result indicating success or failure with error details</returns>
    public virtual async Task<Result<RID, StorageFacadeError>> InsertWineRecordAsync(WineRecord record)
    {
        _logger.Debug("Inserting wine record: {@Record}", record);

        try
        {
            // 1. Allocate a new page in the heap file
            var pageResult = await _heapFile.AllocateNewPageAsync();
            if (pageResult.IsError)
            {
                var error = pageResult.GetErrorOrThrow();
                _logger.Error("Failed to allocate page for record: {@Error}", error);
                return Result<RID, StorageFacadeError>.Error(
                    new StorageFacadeError($"Failed to allocate page: {error.Message}"));
            }

            var page = pageResult.GetValueOrThrow();

            // 2. Add the record to the page
            page.Content = page.Content.Append(record).ToArray();

            // 3. Write the page back to the heap file
            var writeResult = await _heapFile.WritePageAsync(page);
            if (writeResult.IsError)
            {
                var error = writeResult.GetErrorOrThrow();
                _logger.Error("Failed to write page: {@Error}", error);
                return Result<RID, StorageFacadeError>.Error(
                    new StorageFacadeError($"Failed to write page: {error.Message}"));
            }

            // 4. Create RID for the inserted record
            var rid = new RID(page.PageId, (uint)(page.Content.Length - 1));

            // 5. Insert the key (HarvestYear) and RID into the B+ tree
            var indexResult = await _index.InsertAsync(record.HarvestYear, rid);
            if (indexResult.IsError)
            {
                var error = indexResult.GetErrorOrThrow();
                _logger.Error("Failed to insert into B+ tree: {@Error}", error);
                return Result<RID, StorageFacadeError>.Error(
                    new StorageFacadeError($"Failed to insert into index: {error.Message}"));
            }

            _logger.Information("Successfully inserted wine record with RID: {@RID}", rid);
            return Result<RID, StorageFacadeError>.Success(rid);
        }
        catch (Exception ex)
        {
            _logger.Error(ex, "Unexpected error during wine record insertion");
            return Result<RID, StorageFacadeError>.Error(
                new StorageFacadeError($"Unexpected error: {ex.Message}"));
        }
    }

    /// <summary>
    ///     Finds all wine records with the specified harvest year.
    ///     Coordinates B+ tree search and heap file record retrieval.
    /// </summary>
    /// <param name="year">The harvest year to search for</param>
    /// <returns>Result containing an enumerable of matching wine records</returns>
    public virtual async Task<Result<IEnumerable<WineRecord>, StorageFacadeError>> FindRecordsByYearAsync(int year)
    {
        _logger.Debug("Searching for records with harvest year: {Year}", year);

        try
        {
            // 1. Search for RIDs in the B+ tree
            var searchResult = await _index.SearchAsync(year);
            if (searchResult.IsError)
            {
                var error = searchResult.GetErrorOrThrow();
                _logger.Error("Failed to search B+ tree: {@Error}", error);
                return Result<IEnumerable<WineRecord>, StorageFacadeError>.Error(
                    new StorageFacadeError($"Failed to search index: {error.Message}"));
            }

            var rids = searchResult.GetValueOrThrow();
            _logger.Information("Found {Count} RIDs for harvest year {Year}", rids.Count, year);

            var records = new List<WineRecord>();

            // 2. For each RID, retrieve the corresponding record from the heap file
            foreach (var rid in rids)
            {
                var pageResult = await _heapFile.ReadPageAsync(rid.pageId);
                if (pageResult.IsError)
                {
                    var error = pageResult.GetErrorOrThrow();
                    _logger.Warning("Failed to read page {PageId}: {@Error}", rid.pageId, error);
                    continue; // Skip this record but continue with others
                }

                var page = pageResult.GetValueOrThrow();

                // Validate that the record index exists in the page
                if (rid.pageCount < page.Content.Length)
                {
                    var record = page.Content[rid.pageCount];
                    records.Add(record);
                    _logger.Debug("Retrieved record: {@Record}", record);
                }
                else
                {
                    _logger.Warning("Invalid record index {Index} for page {PageId} with {ContentLength} records",
                        rid.pageCount, rid.pageId, page.Content.Length);
                }
            }

            _logger.Information("Successfully retrieved {Count} records for harvest year {Year}", records.Count, year);
            return Result<IEnumerable<WineRecord>, StorageFacadeError>.Success(records);
        }
        catch (Exception ex)
        {
            _logger.Error(ex, "Unexpected error during record search");
            return Result<IEnumerable<WineRecord>, StorageFacadeError>.Error(
                new StorageFacadeError($"Unexpected error: {ex.Message}"));
        }
    }

    /// <summary>
    ///     Initializes the storage facade by ensuring both components are ready.
    /// </summary>
    /// <returns>Result indicating successful initialization</returns>
    public virtual async Task<Result<Unit, StorageFacadeError>> InitializeAsync()
    {
        _logger.Debug("Initializing storage facade");

        try
        {
            // Initialize the heap file manager
            var fileResult = await _heapFile.InitializeAsync();
            if (fileResult.IsError)
            {
                var error = fileResult.GetErrorOrThrow();
                _logger.Error("Failed to initialize file manager: {@Error}", error);
                return Result<Unit, StorageFacadeError>.Error(
                    new StorageFacadeError($"Failed to initialize file manager: {error.Message}"));
            }

            // Initialize the B+ tree index
            var indexResult = await _index.InitializeAsync();
            if (indexResult.IsError)
            {
                var error = indexResult.GetErrorOrThrow();
                _logger.Error("Failed to initialize B+ tree: {@Error}", error);
                return Result<Unit, StorageFacadeError>.Error(
                    new StorageFacadeError($"Failed to initialize index: {error.Message}"));
            }

            _logger.Information("Storage facade initialized successfully");
            return Result<Unit, StorageFacadeError>.Success(Unit.Value);
        }
        catch (Exception ex)
        {
            _logger.Error(ex, "Unexpected error during storage facade initialization");
            return Result<Unit, StorageFacadeError>.Error(
                new StorageFacadeError($"Unexpected error: {ex.Message}"));
        }
    }
}

/// <summary>
///     Error type for storage facade operations
/// </summary>
public readonly struct StorageFacadeError(string message)
{
    public string Message { get; } = message;

    public override string ToString()
    {
        return Message;
    }
}
