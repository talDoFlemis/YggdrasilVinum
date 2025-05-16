using FluentAssertions;
using Serilog;
using YggdrasilVinum.Buffer;
using YggdrasilVinum.Index;
using YggdrasilVinum.Models;
using YggdrasilVinum.Services;
using YggdrasilVinum.Storage;

namespace YggdrasilVinum.Tests.Integration.Services;

public sealed class EqualitySearchProcessorIntegrationTests : IDisposable
{
    private readonly string _tempBasePath;

    public EqualitySearchProcessorIntegrationTests()
    {
        // Setup logging for tests
        Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Debug()
            .WriteTo.Console()
            .CreateLogger();

        // Create base temporary directory for all tests
        _tempBasePath = Path.Combine(Path.GetTempPath(), "YggdrasilSearchTests_" + Guid.NewGuid());
        Directory.CreateDirectory(_tempBasePath);
    }

    public void Dispose()
    {
        // Clean up temp directories after tests
        if (Directory.Exists(_tempBasePath))
            try
            {
                Directory.Delete(_tempBasePath, true);
            }
            catch (Exception ex)
            {
                Log.Error($"Warning: Failed to clean up temp directory: {ex.Message}");
            }
            finally
            {
                Log.CloseAndFlush();
            }
    }

    private string CreateTempDirectory()
    {
        var tempDir = Path.Combine(_tempBasePath, Guid.NewGuid().ToString());
        Directory.CreateDirectory(tempDir);
        return tempDir;
    }

    [Fact]
    public async Task ExecuteAsync_FindsMatchingRecords_WhenHarvestYearExists()
    {
        // Arrange
        var tempDir = CreateTempDirectory();
        var fileManager = new SequentialHeapFileManager(tempDir, 1024 * 10);
        await fileManager.InitializeAsync();

        var bufferManager = new LruBufferManager(fileManager, 3);
        await bufferManager.InitializeAsync();

        // Make sure we're using the correct BPlusTreeIndex with RID
        var bPlusTree = new BPlusTreeIndex<int, RID>(Path.Combine(tempDir, "index.txt"), 4);
        await bPlusTree.InitializeAsync();

        var insertProcessor = new InsertProcessor(bufferManager, fileManager, bPlusTree);
        var searchProcessor = new EqualitySearchProcessor(bufferManager, bPlusTree);

        // Insert records with a mix of harvest years
        var records = new List<WineRecord>
        {
            new(1, "Cabernet Sauvignon", 2018, WineType.Red),
            new(2, "Chardonnay", 2019, WineType.White),
            new(3, "Merlot", 2018, WineType.Red),
            new(4, "Pinot Noir", 2020, WineType.Red),
            new(5, "Sauvignon Blanc", 2019, WineType.White)
        };

        // Insert all records
        foreach (var record in records)
        {
            var result = await insertProcessor.ExecuteAsync(record);
            result.IsSuccess.Should().BeTrue();
        }

        await bufferManager.FlushAllFramesAsync();

        // Act
        var searchResult = await searchProcessor.ExecuteAsync(2018);

        // Assert
        searchResult.IsSuccess.Should().BeTrue();
        var foundRecords = searchResult.GetValueOrThrow();
        foundRecords.Should().HaveCount(2);
        foundRecords.Should().Contain(r => r.WineId == 1 && r.Label == "Cabernet Sauvignon" && r.HarvestYear == 2018);
        foundRecords.Should().Contain(r => r.WineId == 3 && r.Label == "Merlot" && r.HarvestYear == 2018);
    }

    [Fact]
    public async Task ExecuteAsync_ReturnsEmptyArray_WhenNoRecordsWithHarvestYear()
    {
        // Arrange
        var tempDir = CreateTempDirectory();
        var fileManager = new SequentialHeapFileManager(tempDir, 1024 * 10);
        await fileManager.InitializeAsync();

        var bufferManager = new LruBufferManager(fileManager, 3);
        await bufferManager.InitializeAsync();

        var bPlusTree = new BPlusTreeIndex<int, RID>(Path.Combine(tempDir, "index.txt"), 4);
        await bPlusTree.InitializeAsync();

        var insertProcessor = new InsertProcessor(bufferManager, fileManager, bPlusTree);
        var searchProcessor = new EqualitySearchProcessor(bufferManager, bPlusTree);

        var records = new List<WineRecord>
        {
            new(1, "Cabernet Sauvignon", 2018, WineType.Red),
            new(2, "Chardonnay", 2019, WineType.White),
            new(3, "Merlot", 2018, WineType.Red)
        };

        foreach (var record in records)
        {
            var result = await insertProcessor.ExecuteAsync(record);
            result.IsSuccess.Should().BeTrue();
        }

        await bufferManager.FlushAllFramesAsync();

        // Act - Search for a year that doesn't exist
        var searchResult = await searchProcessor.ExecuteAsync(2022);

        // Assert
        searchResult.IsSuccess.Should().BeTrue();
        var foundRecords = searchResult.GetValueOrThrow();
        foundRecords.Should().BeEmpty();
    }

    [Fact]
    public async Task ExecuteAsync_FindsRecordsAcrossMultiplePages()
    {
        // Arrange
        var tempDir = CreateTempDirectory();
        // Use a small page size to force multiple pages
        var fileManager = new SequentialHeapFileManager(tempDir, 1024 * 10, 256);
        await fileManager.InitializeAsync();

        var bufferManager = new LruBufferManager(fileManager, 5);
        await bufferManager.InitializeAsync();

        // Use RID value type for the B+ tree to store both page ID and record position
        var bPlusTree = new BPlusTreeIndex<int, RID>(Path.Combine(tempDir, "index.txt"), 4);
        await bPlusTree.InitializeAsync();

        var insertProcessor = new InsertProcessor(bufferManager, fileManager, bPlusTree);
        var searchProcessor = new EqualitySearchProcessor(bufferManager, bPlusTree);

        // Insert many records with long descriptions to force creation of multiple pages
        var targetYear = 2020;
        var totalRecords = 15;
        var expectedMatches = 0;

        for (var i = 1; i <= totalRecords; i++)
        {
            // Every 3rd record will have our target year
            var year = i % 3 == 0 ? targetYear : 2019;
            if (year == targetYear) expectedMatches++;

            var record = new WineRecord(
                i,
                $"Wine with a very long description {i} to force pagination across multiple pages in the storage system",
                year,
                i % 2 == 0 ? WineType.Red : WineType.White
            );

            var result = await insertProcessor.ExecuteAsync(record);
            result.IsSuccess.Should().BeTrue();
        }

        await bufferManager.FlushAllFramesAsync();

        // Act
        var searchResult = await searchProcessor.ExecuteAsync(targetYear);

        // Assert
        searchResult.IsSuccess.Should().BeTrue();
        var foundRecords = searchResult.GetValueOrThrow();
        foundRecords.Should().HaveCount(expectedMatches);
        foundRecords.All(r => r.HarvestYear == targetYear).Should().BeTrue();
    }

    [Fact]
    public async Task ExecuteAsync_WorksWithReloadedData()
    {
        // Arrange
        var tempDir = CreateTempDirectory();
        var fileManager = new SequentialHeapFileManager(tempDir, 1024 * 10);
        await fileManager.InitializeAsync();

        var bufferManager = new LruBufferManager(fileManager, 3);
        await bufferManager.InitializeAsync();

        var bPlusTree = new BPlusTreeIndex<int, RID>(Path.Combine(tempDir, "index.txt"), 4);
        await bPlusTree.InitializeAsync();

        var insertProcessor = new InsertProcessor(bufferManager, fileManager, bPlusTree);

        // Insert some records
        var records = new List<WineRecord>
        {
            new(1, "Cabernet Sauvignon", 2018, WineType.Red),
            new(2, "Chardonnay", 2019, WineType.White),
            new(3, "Merlot", 2018, WineType.Red),
            new(4, "Pinot Noir", 2020, WineType.Red)
        };

        foreach (var record in records)
        {
            var result = await insertProcessor.ExecuteAsync(record);
            result.IsSuccess.Should().BeTrue();
        }

        await bufferManager.FlushAllFramesAsync();
        await fileManager.DisposeAsync();
        // No need to dispose B+ tree, it doesn't implement IAsyncDisposable

        // Create new instances to simulate application restart
        var newFileManager = new SequentialHeapFileManager(tempDir, 1024 * 10);
        await newFileManager.InitializeAsync();

        var newBufferManager = new LruBufferManager(newFileManager, 3);
        await newBufferManager.InitializeAsync();

        // Create new B+ tree instance with RID that loads existing index data
        var newBPlusTree = new BPlusTreeIndex<int, RID>(Path.Combine(tempDir, "index.txt"), 4);
        await newBPlusTree.InitializeAsync();

        var searchProcessor = new EqualitySearchProcessor(newBufferManager, newBPlusTree);

        // Act
        var searchResult = await searchProcessor.ExecuteAsync(2018);

        // Assert
        searchResult.IsSuccess.Should().BeTrue();
        var foundRecords = searchResult.GetValueOrThrow();
        foundRecords.Should().HaveCount(2);
        foundRecords.Should().Contain(r => r.WineId == 1 && r.HarvestYear == 2018);
        foundRecords.Should().Contain(r => r.WineId == 3 && r.HarvestYear == 2018);
    }

    [Fact]
    public async Task ExecuteAsync_HandlesMultipleSearches_WithDifferentYears()
    {
        // Arrange
        var tempDir = CreateTempDirectory();
        var fileManager = new SequentialHeapFileManager(tempDir, 1024 * 10);
        await fileManager.InitializeAsync();

        var bufferManager = new LruBufferManager(fileManager, 3);
        await bufferManager.InitializeAsync();

        var bPlusTree = new BPlusTreeIndex<int, RID>(Path.Combine(tempDir, "index.txt"), 4);
        await bPlusTree.InitializeAsync();

        var insertProcessor = new InsertProcessor(bufferManager, fileManager, bPlusTree);
        var searchProcessor = new EqualitySearchProcessor(bufferManager, bPlusTree);

        // Insert records with different harvest years
        var recordsByYear = new Dictionary<int, List<WineRecord>>
        {
            [2018] = new()
            {
                new WineRecord(1, "Cabernet Sauvignon", 2018, WineType.Red),
                new WineRecord(2, "Merlot Reserve", 2018, WineType.Red)
            },
            [2019] = new() { new WineRecord(3, "Chardonnay", 2019, WineType.White) },
            [2020] = new()
            {
                new WineRecord(4, "Pinot Noir", 2020, WineType.Red),
                new WineRecord(5, "Syrah", 2020, WineType.Red),
                new WineRecord(6, "Malbec", 2020, WineType.Red)
            }
        };

        // Insert all records
        foreach (var year in recordsByYear.Keys)
        foreach (var record in recordsByYear[year])
        {
            var result = await insertProcessor.ExecuteAsync(record);
            result.IsSuccess.Should().BeTrue();
        }

        await bufferManager.FlushAllFramesAsync();

        // Act & Assert - Search for each year and verify results
        foreach (var year in recordsByYear.Keys)
        {
            var searchResult = await searchProcessor.ExecuteAsync(year);
            searchResult.IsSuccess.Should().BeTrue();

            var foundRecords = searchResult.GetValueOrThrow();
            foundRecords.Length.Should().Be(recordsByYear[year].Count);

            foreach (var expectedRecord in recordsByYear[year])
                foundRecords.Should().Contain(r =>
                    r.WineId == expectedRecord.WineId &&
                    r.Label == expectedRecord.Label &&
                    r.HarvestYear == year);
        }
    }
}
