using FluentAssertions;
using Moq;
using Serilog;
using YggdrasilVinum.Buffer;
using YggdrasilVinum.Index;
using YggdrasilVinum.Models;
using YggdrasilVinum.Services;

namespace YggdrasilVinum.Tests.Unit.Services;

public class EqualitySearchProcessorTests
{
    private readonly EqualitySearchProcessor _equalitySearchProcessor;
    private readonly Mock<IBPlusTreeIndex<int, RID>> _mockBPlusTree;
    private readonly Mock<IBufferManager> _mockBufferManager;
    private readonly int _testHarvestYear = 2020;

    public EqualitySearchProcessorTests()
    {
        // Setup
        _mockBufferManager = new Mock<IBufferManager>();
        _mockBPlusTree = new Mock<IBPlusTreeIndex<int, RID>>();
        _equalitySearchProcessor = new EqualitySearchProcessor(_mockBufferManager.Object, _mockBPlusTree.Object);

        // Configure logger to avoid errors during tests
        Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Debug()
            .WriteTo.Console()
            .CreateLogger();
    }

    [Fact]
    public async Task ExecuteAsync_ReturnsMatchingRecords_WhenBPlusTreeAndBufferManagerSucceed()
    {
        // Arrange
        var rid1 = new RID(1UL, 0);
        var rid2 = new RID(2UL, 2);
        var rids = new List<RID> { rid1, rid2 };

        var records1 = new WineRecord[]
        {
            new(1, "Wine 1", _testHarvestYear, WineType.Red),
            new(2, "Wine 2", _testHarvestYear - 1, WineType.White), new(3, "Wine 3", _testHarvestYear, WineType.Red)
        };

        var records2 = new WineRecord[]
        {
            new(4, "Wine 4", _testHarvestYear - 1, WineType.White),
            new(5, "Wine 5", _testHarvestYear + 1, WineType.Rose), new(6, "Wine 6", _testHarvestYear, WineType.Red)
        };

        var page1 = new Page(1UL, records1);
        var page2 = new Page(2UL, records2);

        // Setup B+ tree search
        _mockBPlusTree.Setup(m => m.SearchAsync(_testHarvestYear))
            .ReturnsAsync(Result<List<RID>, BPlusTreeError>.Success(rids));

        // Setup buffer manager to return pages
        _mockBufferManager.Setup(m => m.LoadPageAsync(rid1.pageId))
            .ReturnsAsync(Result<Page, BufferError>.Success(page1));
        _mockBufferManager.Setup(m => m.LoadPageAsync(rid2.pageId))
            .ReturnsAsync(Result<Page, BufferError>.Success(page2));

        // Act
        var result = await _equalitySearchProcessor.ExecuteAsync(_testHarvestYear);

        // Assert
        result.IsSuccess.Should().BeTrue();
        var records = result.GetValueOrThrow();
        records.Should().HaveCount(2);
        records.Should().Contain(r => r.WineId == 1); // Record at page 1, index 0
        records.Should().Contain(r => r.WineId == 6); // Record at page 2, index 2
    }

    [Fact]
    public async Task ExecuteAsync_ReturnsEmptyArray_WhenNoMatchingRIDs()
    {
        // Arrange
        // Empty list of RIDs means no matches were found in the B+ tree
        var rids = new List<RID>();

        // Setup B+ tree search to return empty list (no matching records)
        _mockBPlusTree.Setup(m => m.SearchAsync(_testHarvestYear))
            .ReturnsAsync(Result<List<RID>, BPlusTreeError>.Success(rids));

        // Act
        var result = await _equalitySearchProcessor.ExecuteAsync(_testHarvestYear);

        // Assert
        result.IsSuccess.Should().BeTrue();
        var retrievedRecords = result.GetValueOrThrow();
        retrievedRecords.Should().BeEmpty();
    }

    [Fact]
    public async Task ExecuteAsync_ReturnsError_WhenBPlusTreeSearchFails()
    {
        // Arrange
        var errorMessage = "Failed to search B+ tree";
        _mockBPlusTree.Setup(m => m.SearchAsync(_testHarvestYear))
            .ReturnsAsync(Result<List<RID>, BPlusTreeError>.Error(new BPlusTreeError(errorMessage)));

        // Act
        var result = await _equalitySearchProcessor.ExecuteAsync(_testHarvestYear);

        // Assert
        result.IsError.Should().BeTrue();
        result.GetErrorOrThrow().Message.Should().Contain("Failed to search for harvest year");
    }

    [Fact]
    public async Task ExecuteAsync_ReturnsError_WhenLoadPageFails()
    {
        // Arrange
        var rid = new RID(1UL, 0);
        var rids = new List<RID> { rid };
        var errorMessage = "Failed to load page";

        // Setup B+ tree search
        _mockBPlusTree.Setup(m => m.SearchAsync(_testHarvestYear))
            .ReturnsAsync(Result<List<RID>, BPlusTreeError>.Success(rids));

        // Setup buffer manager to fail
        _mockBufferManager.Setup(m => m.LoadPageAsync(rid.pageId))
            .ReturnsAsync(Result<Page, BufferError>.Error(new BufferError(errorMessage)));

        // Act
        var result = await _equalitySearchProcessor.ExecuteAsync(_testHarvestYear);

        // Assert
        result.IsError.Should().BeTrue();
        result.GetErrorOrThrow().Message.Should().Contain("Failed to process page");
    }

    [Fact]
    public async Task ExecuteAsync_ReturnsEmptyArray_WhenBPlusTreeReturnsEmptyRIDList()
    {
        // Arrange
        _mockBPlusTree.Setup(m => m.SearchAsync(_testHarvestYear))
            .ReturnsAsync(Result<List<RID>, BPlusTreeError>.Success(new List<RID>()));

        // Act
        var result = await _equalitySearchProcessor.ExecuteAsync(_testHarvestYear);

        // Assert
        result.IsSuccess.Should().BeTrue();
        var foundRecords = result.GetValueOrThrow();
        foundRecords.Should().BeEmpty();
    }

    [Fact]
    public async Task ExecuteAsync_ProcessesMultipleRIDs_FromDifferentPages()
    {
        // Arrange
        // RIDs pointing to specific records in different pages
        var rids = new List<RID>
        {
            new(1, 0), // First record in page 1 
            new(2, 1), // Second record in page 2
            new(3, 0) // First record in page 3
        };

        var records1 = new WineRecord[]
        {
            new(1, "Wine 1", _testHarvestYear, WineType.Red), new(2, "Wine 2", _testHarvestYear - 2, WineType.White)
        };

        var records2 = new WineRecord[]
        {
            new(3, "Wine 3", _testHarvestYear - 1, WineType.Red),
            new(4, "Wine 4", _testHarvestYear, WineType.White),
            new(5, "Wine 5", _testHarvestYear + 1, WineType.Rose)
        };

        var records3 = new WineRecord[]
        {
            new(6, "Wine 6", _testHarvestYear, WineType.Red), new(7, "Wine 7", _testHarvestYear - 1, WineType.White)
        };

        var page1 = new Page(1, records1);
        var page2 = new Page(2, records2);
        var page3 = new Page(3, records3);

        // Setup B+ tree search
        _mockBPlusTree.Setup(m => m.SearchAsync(_testHarvestYear))
            .ReturnsAsync(Result<List<RID>, BPlusTreeError>.Success(rids));

        // Setup buffer manager to return pages
        _mockBufferManager.Setup(m => m.LoadPageAsync(1UL))
            .ReturnsAsync(Result<Page, BufferError>.Success(page1));
        _mockBufferManager.Setup(m => m.LoadPageAsync(2UL))
            .ReturnsAsync(Result<Page, BufferError>.Success(page2));
        _mockBufferManager.Setup(m => m.LoadPageAsync(3UL))
            .ReturnsAsync(Result<Page, BufferError>.Success(page3));

        // Act
        var result = await _equalitySearchProcessor.ExecuteAsync(_testHarvestYear);

        // Assert
        result.IsSuccess.Should().BeTrue();
        var foundRecords = result.GetValueOrThrow();
        foundRecords.Should().HaveCount(3);
        foundRecords.Should().Contain(r => r.WineId == 1); // From page 1, index 0
        foundRecords.Should().Contain(r => r.WineId == 4); // From page 2, index 1
        foundRecords.Should().Contain(r => r.WineId == 6); // From page 3, index 0
    }
}
