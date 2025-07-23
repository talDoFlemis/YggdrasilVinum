using FluentAssertions;
using Moq;
using YggdrasilVinum.Index;
using YggdrasilVinum.Models;
using YggdrasilVinum.Services;
using YggdrasilVinum.Storage;

namespace YggdrasilVinum.Tests.Unit.Services;

public class StorageFacadeTests
{
    private readonly Mock<IFileManager> _mockFileManager;
    private readonly Mock<IBPlusTreeIndex<int, RID>> _mockIndex;
    private readonly StorageFacade _storageFacade;
    private readonly WineRecord _testRecord = new(1, "Test Wine", 2020, WineType.Red);

    public StorageFacadeTests()
    {
        _mockIndex = new Mock<IBPlusTreeIndex<int, RID>>();
        _mockFileManager = new Mock<IFileManager>();
        _storageFacade = new StorageFacade(_mockIndex.Object, _mockFileManager.Object);
    }

    [Fact]
    public async Task InsertWineRecordAsync_WhenSuccessful_ReturnsRID()
    {
        // Arrange
        var expectedPage = new Page(1, []);
        var expectedRID = new RID(1, 0);

        _mockFileManager.Setup(x => x.AllocateNewPageAsync())
            .ReturnsAsync(Result<Page, StoreError>.Success(expectedPage));

        _mockFileManager.Setup(x => x.WritePageAsync(It.IsAny<Page>()))
            .ReturnsAsync(Result<YggdrasilVinum.Models.Unit, StoreError>.Success(YggdrasilVinum.Models.Unit.Value));

        _mockIndex.Setup(x => x.InsertAsync(2020, expectedRID))
            .ReturnsAsync(Result<YggdrasilVinum.Models.Unit, BPlusTreeError>.Success(YggdrasilVinum.Models.Unit.Value));

        // Act
        var result = await _storageFacade.InsertWineRecordAsync(_testRecord);

        // Assert
        result.IsSuccess.Should().BeTrue();
        var rid = result.GetValueOrThrow();
        rid.pageId.Should().Be(1);
        rid.pageCount.Should().Be(0);

        _mockFileManager.Verify(x => x.AllocateNewPageAsync(), Times.Once);
        _mockFileManager.Verify(x => x.WritePageAsync(It.IsAny<Page>()), Times.Once);
        _mockIndex.Verify(x => x.InsertAsync(2020, expectedRID), Times.Once);
    }

    [Fact]
    public async Task InsertWineRecordAsync_WhenFileManagerFails_ReturnsError()
    {
        // Arrange
        var storeError = new StoreError("File manager error");
        _mockFileManager.Setup(x => x.AllocateNewPageAsync())
            .ReturnsAsync(Result<Page, StoreError>.Error(storeError));

        // Act
        var result = await _storageFacade.InsertWineRecordAsync(_testRecord);

        // Assert
        result.IsError.Should().BeTrue();
        var error = result.GetErrorOrThrow();
        error.Message.Should().Contain("Failed to allocate page");
    }

    [Fact]
    public async Task FindRecordsByYearAsync_WhenSuccessful_ReturnsRecords()
    {
        // Arrange
        var rid1 = new RID(1, 0);
        var rid2 = new RID(2, 0); // Different page to avoid multiple calls
        var rids = new List<RID> { rid1, rid2 };

        var wine1 = new WineRecord(1, "Wine 1", 2020, WineType.Red);
        var wine2 = new WineRecord(2, "Wine 2", 2020, WineType.White);
        var page1 = new Page(1, [wine1]);
        var page2 = new Page(2, [wine2]);

        _mockIndex.Setup(x => x.SearchAsync(2020))
            .ReturnsAsync(Result<List<RID>, BPlusTreeError>.Success(rids));

        _mockFileManager.Setup(x => x.ReadPageAsync(1))
            .ReturnsAsync(Result<Page, StoreError>.Success(page1));

        _mockFileManager.Setup(x => x.ReadPageAsync(2))
            .ReturnsAsync(Result<Page, StoreError>.Success(page2));

        // Act
        var result = await _storageFacade.FindRecordsByYearAsync(2020);

        // Assert
        result.IsSuccess.Should().BeTrue();
        var records = result.GetValueOrThrow().ToArray();
        records.Should().HaveCount(2);
        records[0].Should().Be(wine1);
        records[1].Should().Be(wine2);

        _mockIndex.Verify(x => x.SearchAsync(2020), Times.Once);
        _mockFileManager.Verify(x => x.ReadPageAsync(1), Times.Once);
        _mockFileManager.Verify(x => x.ReadPageAsync(2), Times.Once);
    }

    [Fact]
    public async Task FindRecordsByYearAsync_WhenIndexSearchFails_ReturnsError()
    {
        // Arrange
        var indexError = new BPlusTreeError("Index search failed");
        _mockIndex.Setup(x => x.SearchAsync(2020))
            .ReturnsAsync(Result<List<RID>, BPlusTreeError>.Error(indexError));

        // Act
        var result = await _storageFacade.FindRecordsByYearAsync(2020);

        // Assert
        result.IsError.Should().BeTrue();
        var error = result.GetErrorOrThrow();
        error.Message.Should().Contain("Failed to search index");
    }

    [Fact]
    public async Task InitializeAsync_WhenSuccessful_ReturnsSuccess()
    {
        // Arrange
        _mockFileManager.Setup(x => x.InitializeAsync())
            .ReturnsAsync(Result<YggdrasilVinum.Models.Unit, StoreError>.Success(YggdrasilVinum.Models.Unit.Value));

        _mockIndex.Setup(x => x.InitializeAsync())
            .ReturnsAsync(
                Result<YggdrasilVinum.Models.Unit, BPlusTreeError>.Success(YggdrasilVinum.Models.Unit.Value)); // Act
        var result = await _storageFacade.InitializeAsync();

        // Assert
        result.IsSuccess.Should().BeTrue();
        _mockFileManager.Verify(x => x.InitializeAsync(), Times.Once);
        _mockIndex.Verify(x => x.InitializeAsync(), Times.Once);
    }

    [Fact]
    public async Task InitializeAsync_WhenFileManagerInitializationFails_ReturnsError()
    {
        // Arrange
        var storeError = new StoreError("File manager initialization failed");
        _mockFileManager.Setup(x => x.InitializeAsync())
            .ReturnsAsync(Result<YggdrasilVinum.Models.Unit, StoreError>.Error(storeError));

        // Act
        var result = await _storageFacade.InitializeAsync();

        // Assert
        result.IsError.Should().BeTrue();
        var error = result.GetErrorOrThrow();
        error.Message.Should().Contain("Failed to initialize file manager");
    }
}
