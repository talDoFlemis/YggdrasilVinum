using FluentAssertions;
using Serilog;
using YggdrasilVinum.Buffer;
using YggdrasilVinum.Models;
using YggdrasilVinum.Services;
using YggdrasilVinum.Storage;

namespace YggdrasilVinum.Tests.Integration.Services;

public sealed class InsertProcessorIntegrationTests : IDisposable
{
    private readonly string _tempBasePath;

    public InsertProcessorIntegrationTests()
    {
        // Setup logging for tests
        Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Debug()
            .WriteTo.Console()
            .CreateLogger();

        // Create base temporary directory for all tests
        _tempBasePath = Path.Combine(Path.GetTempPath(), "YggdrasilInsertTests_" + Guid.NewGuid());
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
                Console.WriteLine($"Warning: Failed to clean up temp directory: {ex.Message}");
            }
    }

    private string CreateTempDirectory()
    {
        var tempDir = Path.Combine(_tempBasePath, Guid.NewGuid().ToString());
        Directory.CreateDirectory(tempDir);
        return tempDir;
    }

    [Fact]
    public async Task ExecuteAsync_InsertsRecordSuccessfully()
    {
        // Arrange
        var tempDir = CreateTempDirectory();
        var fileManager = new SequentialHeapFileManager(tempDir, 1024 * 10);
        await fileManager.InitializeAsync();

        var bufferManager = new LruBufferManager(fileManager, 3);
        await bufferManager.InitializeAsync();

        var insertProcessor = new InsertProcessor(bufferManager, fileManager);
        var wineRecord = new WineRecord(1, "Cabernet Sauvignon", 2018, WineType.Red);

        // Act
        var result = await insertProcessor.ExecuteAsync(wineRecord);
        var flushResult = await bufferManager.FlushAllFramesAsync();

        // Assert
        flushResult.IsSuccess.Should().BeTrue();
        result.IsSuccess.Should().BeTrue();

        // Verify the record was actually inserted by reading the page
        var pageResult = await fileManager.ReadPageAsync(1);
        pageResult.IsSuccess.Should().BeTrue();

        var page = pageResult.GetValueOrThrow();
        page.Content.Should().ContainSingle(r => r.WineId == 1 && r.Label == "Cabernet Sauvignon");
    }

    [Fact]
    public async Task ExecuteAsync_InsertsMultipleRecordsSuccessfully()
    {
        // Arrange
        var tempDir = CreateTempDirectory();
        var fileManager = new SequentialHeapFileManager(tempDir, 1024 * 10);
        await fileManager.InitializeAsync();

        var bufferManager = new LruBufferManager(fileManager, 3);
        await bufferManager.InitializeAsync();

        var insertProcessor = new InsertProcessor(bufferManager, fileManager);

        var records = new List<WineRecord>
        {
            new(1, "Cabernet Sauvignon", 2018, WineType.Red),
            new(2, "Chardonnay", 2019, WineType.White),
            new(3, "Merlot", 2017, WineType.Red),
            new(4, "Pinot Noir", 2020, WineType.Red),
            new(5, "Sauvignon Blanc", 2021, WineType.White)
        };

        // Act
        foreach (var record in records)
        {
            var result = await insertProcessor.ExecuteAsync(record);
            result.IsSuccess.Should().BeTrue();
        }

        var flushResult = await bufferManager.FlushAllFramesAsync();

        // Assert
        flushResult.IsSuccess.Should().BeTrue();
        var pageResult = await bufferManager.LoadPageAsync(1);
        pageResult.IsSuccess.Should().BeTrue();

        var page = pageResult.GetValueOrThrow();
        page.Content.Length.Should().Be(5);
        page.Content.Should().Contain(r => r.WineId == 1 && r.Label == "Cabernet Sauvignon");
        page.Content.Should().Contain(r => r.WineId == 3 && r.Label == "Merlot");
        page.Content.Should().Contain(r => r.WineId == 5 && r.Label == "Sauvignon Blanc");
    }

    [Fact]
    public async Task ExecuteAsync_AllocatesNewPageWhenFirstPageIsFull()
    {
        // Arrange
        var tempDir = CreateTempDirectory();
        // Use a small page size to force allocation of new page
        var fileManager = new SequentialHeapFileManager(tempDir, 1024 * 10, 512);
        await fileManager.InitializeAsync();

        var bufferManager = new LruBufferManager(fileManager, 3);
        await bufferManager.InitializeAsync();

        var insertProcessor = new InsertProcessor(bufferManager, fileManager);

        // Insert many records with long names to fill the page
        for (var i = 1; i <= 9; i++)
        {
            var record = new WineRecord(
                i,
                $"Wine with a very long name to consume space in the page {i}",
                2000 + i,
                WineType.Red
            );
            await insertProcessor.ExecuteAsync(record);
        }

        // This record should cause a new page allocation
        var newRecord = new WineRecord(
            100,
            "This should be on a new page with a long long long long long name",
            2022,
            WineType.White
        );

        // Act
        var result = await insertProcessor.ExecuteAsync(newRecord);
        var flushResult = await bufferManager.FlushAllFramesAsync();

        // Assert
        flushResult.IsSuccess.Should().BeTrue();
        result.IsSuccess.Should().BeTrue();

        // Check if a second page exists and contains our record
        var page2Exists = await fileManager.PageExistsAsync(4);
        page2Exists.IsSuccess.Should().BeTrue();
        page2Exists.GetValueOrThrow().Should().BeTrue();

        var page2Result = await fileManager.ReadPageAsync(4);
        page2Result.IsSuccess.Should().BeTrue();

        var page2 = page2Result.GetValueOrThrow();
        page2.Content.Should().ContainSingle(r => r.WineId == newRecord.WineId && r.Label == newRecord.Label);
    }

    [Fact]
    public async Task ExecuteAsync_DisposesResourcesCorrectly()
    {
        // Arrange
        var tempDir = CreateTempDirectory();
        var fileManager = new SequentialHeapFileManager(tempDir, 1024 * 10);
        await fileManager.InitializeAsync();

        var bufferManager = new LruBufferManager(fileManager, 3);
        await bufferManager.InitializeAsync();

        var insertProcessor = new InsertProcessor(bufferManager, fileManager);

        // Insert some records
        for (var i = 1; i <= 5; i++)
        {
            var record = new WineRecord(i, $"Wine {i}", 2020 + i, WineType.Red);
            var result = await insertProcessor.ExecuteAsync(record);
            result.IsSuccess.Should().BeTrue();
        }

        // Act - Dispose the Buffer Manager
        await bufferManager.FlushAllFramesAsync();
        await fileManager.DisposeAsync();

        // Create a new file manager and buffer manager to verify data was persisted
        var newFileManager = new SequentialHeapFileManager(tempDir, 1024 * 10);
        await newFileManager.InitializeAsync();

        // Assert
        var pageResult = await newFileManager.ReadPageAsync(1);
        pageResult.IsSuccess.Should().BeTrue();
        pageResult.GetValueOrThrow().Content.Length.Should().Be(5);
    }
}
