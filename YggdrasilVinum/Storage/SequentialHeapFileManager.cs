using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Serilog;
using YggdrasilVinum.Models;

namespace YggdrasilVinum.Storage;

public sealed class SequentialHeapFileManager(
    string storagePath,
    ulong heapSizeInBytes = 100000000,
    ulong pageSizeInBytes = 4096)
    : IFileManager, IDisposable, IAsyncDisposable
{
    private static readonly string _metadataFile = "heap_metadata.ygg";
    private static readonly string _heapFile = "heap.ygg";
    private readonly ILogger _logger = Log.ForContext<SequentialHeapFileManager>();
    private FileStream? _fileStream;
    private HeapFileMetadata _heapFileMetadata = new();

    public async ValueTask DisposeAsync()
    {
        if (_fileStream != null) await _fileStream.DisposeAsync();
    }

    public void Dispose()
    {
        _fileStream?.Dispose();
    }

    public async Task<Result<Unit, StoreError>> InitializeAsync()
    {
        _logger.Debug("Initializing file storage manager with path: {StoragePath}", storagePath);

        if (!Directory.Exists(storagePath))
        {
            var result = CreateDirectory();
            if (result.IsError) return await Task.FromResult(result);
        }

        var heapMetadataFilePath = Path.Combine(storagePath, _metadataFile);
        var heapFilePath = Path.Combine(storagePath, _heapFile);

        _logger.Debug("Checking for heap metadata file: {HeapFilePath}", heapMetadataFilePath);

        if (!File.Exists(heapMetadataFilePath))
        {
            var result = CreateHeapMetadataFile(heapMetadataFilePath);
            if (result.IsError) return await Task.FromResult(result);

            result = await CreateHeapFile(heapFilePath);
            if (result.IsError) return await Task.FromResult(result);
        }

        _logger.Debug("Heap file exists or was created, unmarshalling it...");

        var unmarshalResult = UnmarshalHeapMetadataFile(heapMetadataFilePath);
        if (unmarshalResult.IsError) return await Task.FromResult(unmarshalResult);

        var openResult = OpenHeapFile(heapFilePath);
        if (openResult.IsError) return await Task.FromResult(openResult);


        _logger.Information("File storage manager initialized successfully");
        return await Task.FromResult(Result<Unit, StoreError>.Success(Unit.Value));
    }

    public async Task<Result<Page, StoreError>> ReadPageAsync(ulong pageId)
    {
        Debug.Assert(_fileStream != null);
        Debug.Assert(_heapFileMetadata != null);
        if (pageId <= 0)
            return Result<Page, StoreError>.Error(new StoreError($"Page ID {pageId} is invalid"));
        if (pageId > _heapFileMetadata.LastPageId)
            return Result<Page, StoreError>.Error(new StoreError($"Page ID {pageId} does not exist"));

        _logger.Debug("Reading page with ID: {PageId}", pageId);
        try
        {
            _fileStream.Seek((long)(pageId * pageSizeInBytes), SeekOrigin.Begin);

            var buffer = new byte[pageSizeInBytes];

            // Read the page data into the buffer
            var bytesRead = await _fileStream.ReadAsync(buffer, 0, (int)pageSizeInBytes);

            if (bytesRead == 0)
            {
                _logger.Warning("No data was read for page {PageId}", pageId);
                return Result<Page, StoreError>.Error(new StoreError($"No data found for page {pageId}"));
            }

            // Deserialize the buffer into a Page object
            var jsonData = Encoding.UTF8.GetString(buffer).TrimEnd('\0');

            // Skip empty pages
            if (string.IsNullOrWhiteSpace(jsonData))
            {
                _logger.Warning("Page {PageId} is empty", pageId);
                return Result<Page, StoreError>.Error(new StoreError($"Page {pageId} is empty"));
            }

            var page = JsonSerializer.Deserialize<Page>(jsonData);
            if (page == null)
            {
                _logger.Warning("Failed to deserialize page {PageId}", pageId);
                return Result<Page, StoreError>.Error(new StoreError($"Failed to deserialize page {pageId}"));
            }

            page.LastAccessed = DateTime.UtcNow;

            return await Task.FromResult(Result<Page, StoreError>.Success(page));
        }
        catch (Exception e)
        {
            _logger.Error("Failed to read page: {Error}", e.Message);
            return await Task.FromResult(Result<Page, StoreError>.Error(
                new StoreError($"Failed to read page: {e.Message}")));
        }
    }

    public async Task<Result<Unit, StoreError>> WritePageAsync(Page page)
    {
        Debug.Assert(_fileStream != null);
        Debug.Assert(_heapFileMetadata != null);
        if (page.PageId <= 0)
            return Result<Unit, StoreError>.Error(new StoreError($"Page ID {page.PageId} is invalid"));
        if (page.PageId > _heapFileMetadata.LastPageId)
            return Result<Unit, StoreError>.Error(new StoreError($"Page ID {page.PageId} does not exist"));

        _logger.Debug("Writing page with ID: {PageId}", page.PageId);

        if (page.PageId > _heapFileMetadata.LastPageId)
        {
            _logger.Error("Page ID {PageId} is greater than the last page ID {LastPageId}",
                page.PageId, _heapFileMetadata.LastPageId);
            return await Task.FromResult(Result<Unit, StoreError>.Error(
                new StoreError(
                    $"Page ID {page.PageId} is greater than the last page ID {_heapFileMetadata.LastPageId}")));
        }

        try
        {
            var jsonData = JsonSerializer.Serialize(page);
            var buffer = Encoding.UTF8.GetBytes(jsonData);

            if ((ulong)buffer.Length > pageSizeInBytes)
                return await Task.FromResult(Result<Unit, StoreError>.Error(
                    new StoreError($"Page size exceeds the maximum allowed size of {pageSizeInBytes} bytes.")));

            var paddedBuffer = new byte[pageSizeInBytes];
            Array.Copy(buffer, paddedBuffer, buffer.Length);

            _fileStream.Seek((long)(page.PageId * pageSizeInBytes), SeekOrigin.Begin);
            await _fileStream.WriteAsync(paddedBuffer, 0, paddedBuffer.Length);
        }
        catch (Exception e)
        {
            _logger.Error("Failed to write page: {Error}", e.Message);
            return await Task.FromResult(Result<Unit, StoreError>.Error(
                new StoreError($"Failed to write page: {e.Message}")));
        }

        var updateResult = UpdateHeapMetadataFile(Path.Combine(storagePath, _metadataFile), _heapFileMetadata);
        if (updateResult.IsError)
            return await Task.FromResult(Result<Unit, StoreError>.Error(
                new StoreError($"Failed to update heap metadata file: {updateResult}")));

        _logger.Information("Page with ID: {PageId} written successfully", page.PageId);

        return await Task.FromResult(Result<Unit, StoreError>.Success(Unit.Value));
    }

    public Task<Result<bool, StoreError>> PageExistsAsync(ulong pageId)
    {
        Debug.Assert(_fileStream != null);
        Debug.Assert(_heapFileMetadata != null);

        _logger.Debug("Checking if page with ID: {PageId} exists", pageId);

        if (pageId > _heapFileMetadata.LastPageId || pageId <= 0)
        {
            _logger.Warning("Page with ID: {PageId} does not exist", pageId);
            return Task.FromResult(Result<bool, StoreError>.Success(false));
        }

        return Task.FromResult(Result<bool, StoreError>.Success(true));
    }

    public async Task<Result<Page, StoreError>> AllocateNewPageAsync()
    {
        Debug.Assert(_fileStream != null);
        Debug.Assert(_heapFileMetadata != null);

        _logger.Debug("Allocating new page");

        var newPage = new Page(_heapFileMetadata.LastPageId + 1, []);

        _heapFileMetadata.LastPageId++;
        _heapFileMetadata.PageCount++;

        var result = await WritePageAsync(newPage);
        if (result.IsError)
        {
            _logger.Error("Failed to allocate new page: {Error}", result);
            return Result<Page, StoreError>.Error(result.GetErrorOrThrow());
        }

        _logger.Information("Allocated new page with ID: {PageId}", newPage.PageId);

        return await Task.FromResult(Result<Page, StoreError>.Success(newPage));
    }

    public async Task<Result<Unit, StoreError>> FlushAsync()
    {
        Debug.Assert(_fileStream != null);
        Debug.Assert(_heapFileMetadata != null);
        _logger.Debug("Flushing file stream to disk");

        try
        {
            await _fileStream.FlushAsync();
        }
        catch (Exception e)
        {
            _logger.Error("Failed to flush file stream: {Error}", e.Message);
            return await Task.FromResult(Result<Unit, StoreError>.Error(
                new StoreError($"Failed to flush file stream: {e.Message}")));
        }

        _logger.Information("Flushed file stream to disk with success");
        return await Task.FromResult(Result<Unit, StoreError>.Success(Unit.Value));
    }

    private Result<Unit, StoreError> CreateDirectory()
    {
        _logger.Debug("Creating storage directory: {StoragePath}", storagePath);

        try
        {
            Directory.CreateDirectory(storagePath);
            _logger.Information("Created storage directory: {StoragePath}", storagePath);
        }
        catch (Exception e)
        {
            _logger.Error("Failed to create storage directory: {StoragePath}. Error: {Error}", storagePath,
                e.Message);
            return Result<Unit, StoreError>.Error(
                new StoreError($"Failed to create storage directory: {e.Message}"));
        }

        return Result<Unit, StoreError>.Success(Unit.Value);
    }

    private Result<Unit, StoreError> CreateHeapMetadataFile(string heapFilePath)
    {
        _logger.Debug("Creating heap file in path: {HeapFilePath}", heapFilePath);

        try
        {
            var metadata = new HeapFileMetadata
            {
                LastPageId = 0,
                PageCount = 0,
                HeapSizeInBytes = (long)heapSizeInBytes,
                CreatedAt = DateTime.UtcNow,
                LastModifiedAt = DateTime.UtcNow
            };
            _heapFileMetadata = metadata;

            var jsonMetadata =
                JsonSerializer.Serialize(metadata, new JsonSerializerOptions { WriteIndented = true });
            File.WriteAllText(heapFilePath, jsonMetadata);

            _logger.Information("Heap Metadata file created successfully: {HeapFilePath}", heapFilePath);
        }
        catch (Exception e)
        {
            _logger.Error("Failed to create heap file: {HeapFilePath}. Error: {Error}", heapFilePath, e.Message);
            return Result<Unit, StoreError>.Error(
                new StoreError($"Failed to create heap file: {e.Message}"));
        }

        _logger.Debug("Heap file created successfully: {HeapFilePath}", heapFilePath);
        return Result<Unit, StoreError>.Success(Unit.Value);
    }

    private Result<Unit, StoreError> UnmarshalHeapMetadataFile(string heapFilePath)
    {
        _logger.Debug("Unmarshalling heap file metadata at {HeapFilePath}", heapFilePath);

        try
        {
            var jsonMetadata = File.ReadAllText(heapFilePath);
            var metadata = JsonSerializer.Deserialize<HeapFileMetadata>(jsonMetadata);

            if (metadata == null)
                return Result<Unit, StoreError>.Error(
                    new StoreError("Failed to deserialize metadata: result was null"));

            _logger.Debug("Heap metadata loaded: LastPageId={LastPageId}, PageCount={PageCount}, " +
                          "HeapSize={HeapSize}MB, Created={Created}, Modified={Modified}",
                metadata.LastPageId,
                metadata.PageCount,
                metadata.HeapSizeInBytes / (1024 * 1024),
                metadata.CreatedAt,
                metadata.LastModifiedAt);

            _heapFileMetadata = metadata;

            return Result<Unit, StoreError>.Success(Unit.Value);
        }
        catch (Exception e)
        {
            _logger.Error("Failed to unmarshal heap file: {HeapFilePath}. Error: {Error}", heapFilePath, e.Message);
            return Result<Unit, StoreError>.Error(
                new StoreError($"Failed to unmarshal heap file: {e.Message}"));
        }
    }

    private async Task<Result<Unit, StoreError>> CreateHeapFile(string heapFilePath)
    {
        _logger.Debug("Creating heap file in path: {HeapFilePath}", heapFilePath);

        try
        {
            await using var fs = new FileStream(heapFilePath, FileMode.Create, FileAccess.Write);
            fs.SetLength((long)heapSizeInBytes);
            _fileStream = fs;

            var allocateResult = await AllocateNewPageAsync();
            if (allocateResult.IsError)
            {
                return Result<Unit, StoreError>.Error(allocateResult.GetErrorOrThrow());
            }
        }
        catch (Exception e)
        {
            _logger.Error("Failed to create heap file: {HeapFilePath}. Error: {Error}", heapFilePath, e.Message);
            return Result<Unit, StoreError>.Error(
                new StoreError($"Failed to create heap file: {e.Message}"));
        }

        _logger.Information("Heap file created successfully: {HeapFilePath}", heapFilePath);
        return Result<Unit, StoreError>.Success(Unit.Value);
    }

    private Result<Unit, StoreError> OpenHeapFile(string heapFilePath)
    {
        _logger.Debug("Opening heap file in path: {HeapFilePath}", heapFilePath);

        try
        {
            var fs = new FileStream(heapFilePath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
            _fileStream = fs;
        }
        catch (Exception e)
        {
            _logger.Error("Failed to open heap file: {HeapFilePath}. Error: {Error}", heapFilePath, e.Message);
            return Result<Unit, StoreError>.Error(
                new StoreError($"Failed to open heap file: {e.Message}"));
        }

        _logger.Debug("Heap file opened successfully: {HeapFilePath}", heapFilePath);
        return Result<Unit, StoreError>.Success(Unit.Value);
    }

    private Result<Unit, StoreError> UpdateHeapMetadataFile(string metadataFilePath, HeapFileMetadata metadata)
    {
        try
        {
            metadata.LastModifiedAt = DateTime.UtcNow;
            var jsonMetadata =
                JsonSerializer.Serialize(metadata, new JsonSerializerOptions { WriteIndented = true });
            File.WriteAllText(metadataFilePath, jsonMetadata);

            return Result<Unit, StoreError>.Success(Unit.Value);
        }
        catch (Exception e)
        {
            _logger.Error("Failed to update metadata file: {MetadataFilePath}. Error: {Error}",
                metadataFilePath, e.Message);
            return Result<Unit, StoreError>.Error(
                new StoreError($"Failed to update metadata file: {e.Message}"));
        }
    }
}

internal class HeapFileMetadata
{
    public ulong LastPageId { get; set; }
    public ulong PageCount { get; set; }
    public long HeapSizeInBytes { get; set; }
    public DateTime CreatedAt { get; set; }
    public DateTime LastModifiedAt { get; set; }
}
