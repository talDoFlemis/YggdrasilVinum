using System.Buffers.Binary;
using System.Text;
using Serilog;
using YggdrasilVinum.Models;
using YggdrasilVinum.Parsers;

namespace YggdrasilVinum.Services;

/// <summary>
///     Processes wine CSV data into a fixed-size binary format sorted by harvest year
///     for efficient binary search operations.
/// </summary>
public class WineProcessor : IWineProcessor
{
    private const int WINE_ID_SIZE = 4;
    private const int LABEL_SIZE = 100; // Fixed size for label, null-padded
    private const int HARVEST_YEAR_SIZE = 4;
    private const int WINE_TYPE_SIZE = 1;
    private const int RECORD_SIZE = WINE_ID_SIZE + LABEL_SIZE + HARVEST_YEAR_SIZE + WINE_TYPE_SIZE; // 109 bytes

    private const int MAX_RECORDS_IN_MEMORY = 10000; // Adjust based on available memory

    private readonly ILogger _logger = Log.ForContext<WineProcessor>();
    private readonly string _processedFilePath;
    private readonly string _tempDirectory;
    private FileStream? _processedFileStream;

    public WineProcessor(string processedFilePath)
    {
        _processedFilePath = processedFilePath ?? throw new ArgumentNullException(nameof(processedFilePath));
        _tempDirectory = Path.Combine(Path.GetDirectoryName(_processedFilePath)!, "wine_temp");
    }

    /// <summary>
    ///     Processes the CSV file and creates a sorted binary file.
    /// </summary>
    public async Task<Result<Unit, WineProcessorError>> ProcessCsvFileAsync(string csvFilePath)
    {
        _logger.Information("Starting wine CSV processing from {CsvFilePath}", csvFilePath);

        try
        {
            // Clean up any existing temp directory
            await CleanupTempDirectoryAsync();
            Directory.CreateDirectory(_tempDirectory);

            // Step 1: Parse CSV and create sorted chunks
            var chunkFilesResult = await CreateSortedChunksAsync(csvFilePath);
            if (chunkFilesResult.IsError)
                return Result<Unit, WineProcessorError>.Error(chunkFilesResult.GetErrorOrThrow());

            var chunkFiles = chunkFilesResult.GetValueOrThrow();

            // Step 2: Merge sorted chunks into final file
            var mergeResult = await MergeSortedChunksAsync(chunkFiles);
            if (mergeResult.IsError)
                return Result<Unit, WineProcessorError>.Error(mergeResult.GetErrorOrThrow());

            // Step 3: Open the processed file for reading
            var openResult = await OpenProcessedFileAsync();
            if (openResult.IsError)
                return Result<Unit, WineProcessorError>.Error(openResult.GetErrorOrThrow());

            // Cleanup temp files
            await CleanupTempDirectoryAsync();

            _logger.Information("Successfully processed {RecordCount} wine records", RecordCount);
            return Result<Unit, WineProcessorError>.Success(Unit.Value);
        }
        catch (Exception ex)
        {
            _logger.Error(ex, "Error processing wine CSV file");
            return Result<Unit, WineProcessorError>.Error(
                new WineProcessorError($"Failed to process CSV file: {ex.Message}"));
        }
    }

    /// <summary>
    ///     Opens an existing processed file for binary search operations.
    /// </summary>
    public async Task<Result<Unit, WineProcessorError>> OpenProcessedFileAsync()
    {
        _logger.Debug("Opening processed wine file {ProcessedFilePath}", _processedFilePath);

        try
        {
            if (!File.Exists(_processedFilePath))
                return Result<Unit, WineProcessorError>.Error(
                    new WineProcessorError($"Processed file not found: {_processedFilePath}"));

            _processedFileStream = new FileStream(_processedFilePath, FileMode.Open, FileAccess.Read, FileShare.Read);
            RecordCount = _processedFileStream.Length / RECORD_SIZE;

            _logger.Information("Opened processed file with {RecordCount} records", RecordCount);
            return Result<Unit, WineProcessorError>.Success(Unit.Value);
        }
        catch (Exception ex)
        {
            _logger.Error(ex, "Error opening processed file");
            return Result<Unit, WineProcessorError>.Error(
                new WineProcessorError($"Failed to open processed file: {ex.Message}"));
        }
    }

    /// <summary>
    ///     Searches for wines with the specified harvest year using binary search.
    /// </summary>
    public async Task<Result<List<WineRecord>, WineProcessorError>> SearchByHarvestYearAsync(int harvestYear)
    {
        if (_processedFileStream == null)
            return Result<List<WineRecord>, WineProcessorError>.Error(
                new WineProcessorError("Processed file is not open. Call OpenProcessedFileAsync first."));

        _logger.Debug("Searching for wines with harvest year {HarvestYear}", harvestYear);

        try
        {
            var results = new List<WineRecord>();

            // Binary search for the first occurrence
            var firstIndex = await BinarySearchFirstOccurrenceAsync(harvestYear);
            if (firstIndex == -1)
            {
                _logger.Debug("No wines found with harvest year {HarvestYear}", harvestYear);
                return Result<List<WineRecord>, WineProcessorError>.Success(results);
            }

            // Read all consecutive records with the same harvest year
            var currentIndex = firstIndex;
            while (currentIndex < RecordCount)
            {
                var recordResult = await ReadRecordAtIndexAsync(currentIndex);
                if (recordResult.IsError)
                    return Result<List<WineRecord>, WineProcessorError>.Error(recordResult.GetErrorOrThrow());

                var record = recordResult.GetValueOrThrow();
                if (record.HarvestYear != harvestYear)
                    break;

                results.Add(record);
                currentIndex++;
            }

            _logger.Information("Found {Count} wines with harvest year {HarvestYear}", results.Count, harvestYear);
            return Result<List<WineRecord>, WineProcessorError>.Success(results);
        }
        catch (Exception ex)
        {
            _logger.Error(ex, "Error searching for harvest year {HarvestYear}", harvestYear);
            return Result<List<WineRecord>, WineProcessorError>.Error(
                new WineProcessorError($"Failed to search by harvest year: {ex.Message}"));
        }
    }

    /// <summary>
    ///     Gets the total number of records in the processed file.
    /// </summary>
    public long RecordCount { get; private set; }

    public void Dispose()
    {
        _processedFileStream?.Dispose();
        CleanupTempDirectoryAsync().GetAwaiter().GetResult();
    }

    #region Helper Classes

    private class ChunkReader : IDisposable
    {
        private readonly FileStream _stream;

        public ChunkReader(string filePath)
        {
            _stream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
        }

        public WineRecord? CurrentRecord { get; private set; }

        public void Dispose()
        {
            _stream?.Dispose();
        }

        public async Task<WineRecord?> ReadNextRecordAsync()
        {
            var buffer = new byte[RECORD_SIZE];
            var bytesRead = await _stream.ReadAsync(buffer);

            if (bytesRead == 0)
            {
                CurrentRecord = null;
                return null;
            }

            if (bytesRead != RECORD_SIZE)
                throw new InvalidOperationException($"Expected {RECORD_SIZE} bytes, got {bytesRead}");

            var span = buffer.AsSpan();

            // Read WineId
            var wineId = BinaryPrimitives.ReadInt32LittleEndian(span[..4]);

            // Read Label
            var labelSpan = span.Slice(4, LABEL_SIZE);
            var labelBytes = labelSpan.ToArray();
            var nullIndex = Array.IndexOf(labelBytes, (byte)0);
            var labelLength = nullIndex >= 0 ? nullIndex : LABEL_SIZE;
            var label = Encoding.UTF8.GetString(labelBytes, 0, labelLength);

            // Read HarvestYear
            var harvestYear = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(4 + LABEL_SIZE, 4));

            // Read WineType
            var wineType = (WineType)buffer[4 + LABEL_SIZE + 4];

            CurrentRecord = new WineRecord(wineId, label, harvestYear, wineType);
            return CurrentRecord;
        }
    }

    #endregion

    #region Private Methods

    private async Task<Result<List<string>, WineProcessorError>> CreateSortedChunksAsync(string csvFilePath)
    {
        _logger.Debug("Creating sorted chunks from CSV file");

        var chunkFiles = new List<string>();
        var currentChunk = new List<WineRecord>();
        var chunkNumber = 0;

        var parseResult = WineDataParser.ParseCsvFile(csvFilePath);
        if (parseResult.IsError)
        {
            var error = parseResult.GetErrorOrThrow();
            return Result<List<string>, WineProcessorError>.Error(
                new WineProcessorError($"Failed to parse CSV: {error.Message}"));
        }

        var wines = parseResult.GetValueOrThrow();
        RecordCount = wines.Count;

        // Process in chunks to avoid memory issues
        foreach (var wine in wines)
        {
            currentChunk.Add(wine);

            if (currentChunk.Count >= MAX_RECORDS_IN_MEMORY)
            {
                var chunkResult = await WriteSortedChunkAsync(currentChunk, chunkNumber++);
                if (chunkResult.IsError)
                    return Result<List<string>, WineProcessorError>.Error(chunkResult.GetErrorOrThrow());

                chunkFiles.Add(chunkResult.GetValueOrThrow());
                currentChunk.Clear();
            }
        }

        // Write the remaining records
        if (currentChunk.Count > 0)
        {
            var chunkResult = await WriteSortedChunkAsync(currentChunk, chunkNumber);
            if (chunkResult.IsError)
                return Result<List<string>, WineProcessorError>.Error(chunkResult.GetErrorOrThrow());

            chunkFiles.Add(chunkResult.GetValueOrThrow());
        }

        _logger.Information("Created {ChunkCount} sorted chunks", chunkFiles.Count);
        return Result<List<string>, WineProcessorError>.Success(chunkFiles);
    }

    private async Task<Result<string, WineProcessorError>> WriteSortedChunkAsync(
        List<WineRecord> records, int chunkNumber)
    {
        var chunkFilePath = Path.Combine(_tempDirectory, $"chunk_{chunkNumber:D4}.bin");

        _logger.Debug("Writing sorted chunk {ChunkNumber} with {RecordCount} records",
            chunkNumber, records.Count);

        try
        {
            // Sort by harvest year
            records.Sort((a, b) => a.HarvestYear.CompareTo(b.HarvestYear));

            await using var writer = new FileStream(chunkFilePath, FileMode.Create, FileAccess.Write);
            foreach (var record in records)
            {
                var buffer = SerializeRecord(record);
                await writer.WriteAsync(buffer);
            }

            await writer.FlushAsync();
            return Result<string, WineProcessorError>.Success(chunkFilePath);
        }
        catch (Exception ex)
        {
            _logger.Error(ex, "Error writing sorted chunk {ChunkNumber}", chunkNumber);
            return Result<string, WineProcessorError>.Error(
                new WineProcessorError($"Failed to write sorted chunk: {ex.Message}"));
        }
    }

    private async Task<Result<Unit, WineProcessorError>> MergeSortedChunksAsync(List<string> chunkFiles)
    {
        _logger.Debug("Merging {ChunkCount} sorted chunks", chunkFiles.Count);

        try
        {
            var readers = new List<ChunkReader>();

            // Open all chunk files
            foreach (var chunkFile in chunkFiles)
            {
                var reader = new ChunkReader(chunkFile);
                var firstRecord = await reader.ReadNextRecordAsync();
                if (firstRecord.HasValue)
                    readers.Add(reader);
                else
                    reader.Dispose();
            }

            // Create output file
            await using var writer = new FileStream(_processedFilePath, FileMode.Create, FileAccess.Write);

            // Merge using a priority queue approach
            var heap = new SortedDictionary<int, Queue<ChunkReader>>();

            // Initialize heap with first record from each chunk
            foreach (var reader in readers)
            {
                var record = reader.CurrentRecord!.Value;
                if (!heap.ContainsKey(record.HarvestYear))
                    heap[record.HarvestYear] = new Queue<ChunkReader>();
                heap[record.HarvestYear].Enqueue(reader);
            }

            // Merge records in sorted order
            while (heap.Count > 0)
            {
                // Get the chunk with the smallest harvest year
                var (minYear, queue) = heap.First();
                var reader = queue.Dequeue();

                if (queue.Count == 0)
                    heap.Remove(minYear);

                // Write the current record
                var buffer = SerializeRecord(reader.CurrentRecord!.Value);
                await writer.WriteAsync(buffer);

                // Read next record from this chunk
                var nextRecord = await reader.ReadNextRecordAsync();
                if (nextRecord.HasValue)
                {
                    var year = nextRecord.Value.HarvestYear;
                    if (!heap.ContainsKey(year))
                        heap[year] = new Queue<ChunkReader>();
                    heap[year].Enqueue(reader);
                }
                else
                {
                    reader.Dispose();
                }
            }

            // Cleanup any remaining readers
            foreach (var reader in readers) reader.Dispose();

            await writer.FlushAsync();
            _logger.Information("Successfully merged all chunks into {ProcessedFilePath}", _processedFilePath);
            return Result<Unit, WineProcessorError>.Success(Unit.Value);
        }
        catch (Exception ex)
        {
            _logger.Error(ex, "Error merging sorted chunks");
            return Result<Unit, WineProcessorError>.Error(
                new WineProcessorError($"Failed to merge sorted chunks: {ex.Message}"));
        }
    }

    private byte[] SerializeRecord(WineRecord record)
    {
        var buffer = new byte[RECORD_SIZE];
        var span = buffer.AsSpan();

        // Write WineId (4 bytes)
        BinaryPrimitives.WriteInt32LittleEndian(span[..4], record.WineId);

        // Write Label (100 bytes, null-padded)
        var labelBytes = Encoding.UTF8.GetBytes(record.Label);
        var labelSpan = span.Slice(4, LABEL_SIZE);
        labelBytes.AsSpan()[..Math.Min(labelBytes.Length, LABEL_SIZE)].CopyTo(labelSpan);

        // Write HarvestYear (4 bytes)
        BinaryPrimitives.WriteInt32LittleEndian(span.Slice(4 + LABEL_SIZE, 4), record.HarvestYear);

        // Write WineType (1 byte)
        buffer[4 + LABEL_SIZE + 4] = (byte)record.Type;

        return buffer;
    }

    private WineRecord DeserializeRecord(byte[] buffer)
    {
        var span = buffer.AsSpan();

        // Read WineId
        var wineId = BinaryPrimitives.ReadInt32LittleEndian(span[..4]);

        // Read Label
        var labelSpan = span.Slice(4, LABEL_SIZE);
        var labelBytes = labelSpan.ToArray();
        var nullIndex = Array.IndexOf(labelBytes, (byte)0);
        var labelLength = nullIndex >= 0 ? nullIndex : LABEL_SIZE;
        var label = Encoding.UTF8.GetString(labelBytes, 0, labelLength);

        // Read HarvestYear
        var harvestYear = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(4 + LABEL_SIZE, 4));

        // Read WineType
        var wineType = (WineType)buffer[4 + LABEL_SIZE + 4];

        return new WineRecord(wineId, label, harvestYear, wineType);
    }

    private async Task<long> BinarySearchFirstOccurrenceAsync(int harvestYear)
    {
        long left = 0;
        var right = RecordCount - 1;
        long result = -1;

        while (left <= right)
        {
            var mid = left + (right - left) / 2;

            var recordResult = await ReadRecordAtIndexAsync(mid);
            if (recordResult.IsError)
                throw new InvalidOperationException("Failed to read record during binary search");

            var record = recordResult.GetValueOrThrow();

            if (record.HarvestYear == harvestYear)
            {
                result = mid;
                right = mid - 1; // Continue searching for first occurrence
            }
            else if (record.HarvestYear < harvestYear)
            {
                left = mid + 1;
            }
            else
            {
                right = mid - 1;
            }
        }

        return result;
    }

    private async Task<Result<WineRecord, WineProcessorError>> ReadRecordAtIndexAsync(long index)
    {
        if (_processedFileStream == null)
            return Result<WineRecord, WineProcessorError>.Error(
                new WineProcessorError("Processed file is not open"));

        if (index < 0 || index >= RecordCount)
            return Result<WineRecord, WineProcessorError>.Error(
                new WineProcessorError($"Index {index} is out of range"));

        try
        {
            var offset = index * RECORD_SIZE;
            _processedFileStream.Seek(offset, SeekOrigin.Begin);

            var buffer = new byte[RECORD_SIZE];
            var bytesRead = await _processedFileStream.ReadAsync(buffer);

            if (bytesRead != RECORD_SIZE)
                return Result<WineRecord, WineProcessorError>.Error(
                    new WineProcessorError($"Expected to read {RECORD_SIZE} bytes, got {bytesRead}"));

            var record = DeserializeRecord(buffer);
            return Result<WineRecord, WineProcessorError>.Success(record);
        }
        catch (Exception ex)
        {
            _logger.Error(ex, "Error reading record at index {Index}", index);
            return Result<WineRecord, WineProcessorError>.Error(
                new WineProcessorError($"Failed to read record: {ex.Message}"));
        }
    }

    private async Task CleanupTempDirectoryAsync()
    {
        if (Directory.Exists(_tempDirectory))
            try
            {
                await Task.Run(() => Directory.Delete(_tempDirectory, true));
            }
            catch (Exception ex)
            {
                _logger.Warning(ex, "Failed to cleanup temp directory {TempDirectory}", _tempDirectory);
            }
    }

    #endregion
}

public readonly struct WineProcessorError
{
    public string Message { get; }

    public WineProcessorError(string message)
    {
        Message = message;
    }
}
