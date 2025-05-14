using Moq;
using Serilog;
using FluentAssertions;
using YggdrasilVinum.Buffer;
using YggdrasilVinum.Models;
using YggdrasilVinum.Services;
using YggdrasilVinum.Storage;

namespace YggdrasilVinum.Tests.Unit.Services;

public class InsertProcessorTests
{
    private readonly Mock<IBufferManager> _mockBufferManager;
    private readonly Mock<IFileManager> _mockFileManager;
    private readonly InsertProcessor _insertProcessor;
    private readonly WineRecord _testRecord;

    public InsertProcessorTests()
    {
        // Setup
        _mockBufferManager = new Mock<IBufferManager>();
        _mockFileManager = new Mock<IFileManager>();
        _insertProcessor = new InsertProcessor(_mockBufferManager.Object, _mockFileManager.Object);
        _testRecord = new WineRecord(1, "Test Wine", 2020, WineType.Red);

        // Configure logger to avoid errors during tests
        Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Debug()
            .WriteTo.Console()
            .CreateLogger();
    }

    [Fact]
    public async Task ExecuteAsync_WhenRandomPageHasEnoughSpace_InsertsRecordSuccessfully()
    {
        // Arrange
        var pageId = (ulong)1;
        var page = new Page(pageId, Array.Empty<WineRecord>());

        _mockBufferManager.Setup(m => m.GetRandomPageAsync())
            .ReturnsAsync(Result<Page, BufferError>.Success(page));

        _mockFileManager.Setup(m => m.PageHasEnoughSpaceToInsertRecord(page, _testRecord))
            .ReturnsAsync(Result<bool, StoreError>.Success(true));

        _mockBufferManager.Setup(m => m.PutPageAsync(It.Is<Page>(p =>
            p.PageId == pageId && p.Content.Contains(_testRecord))))
            .ReturnsAsync(Result<YggdrasilVinum.Models.Unit, BufferError>.Success(YggdrasilVinum.Models.Unit.Value));

        // Act
        var result = await _insertProcessor.ExecuteAsync(_testRecord);

        // Assert
        result.IsSuccess.Should().BeTrue();
        _mockBufferManager.Verify(m => m.PutPageAsync(It.Is<Page>(p =>
            p.PageId == pageId && p.Content.Contains(_testRecord))), Times.Once);
        _mockFileManager.Verify(m => m.AllocateNewPageAsync(), Times.Never);
    }

    [Fact]
    public async Task ExecuteAsync_WhenRandomPageDoesNotHaveEnoughSpace_AllocatesNewPage()
    {
        // Arrange
        var existingPageId = 1UL;
        var existingPage = new Page(existingPageId, Array.Empty<WineRecord>());
        var newPageId = 2UL;
        var newPage = new Page(newPageId, Array.Empty<WineRecord>());

        _mockBufferManager.Setup(m => m.GetRandomPageAsync())
            .ReturnsAsync(Result<Page, BufferError>.Success(existingPage));

        _mockFileManager.Setup(m => m.PageHasEnoughSpaceToInsertRecord(existingPage, _testRecord))
            .ReturnsAsync(Result<bool, StoreError>.Success(false));

        _mockFileManager.Setup(m => m.AllocateNewPageAsync())
            .ReturnsAsync(Result<Page, StoreError>.Success(newPage));

        _mockBufferManager.Setup(m => m.PutPageAsync(It.Is<Page>(p =>
            p.PageId == newPageId && p.Content.Contains(_testRecord))))
            .ReturnsAsync(Result<YggdrasilVinum.Models.Unit, BufferError>.Success(YggdrasilVinum.Models.Unit.Value));

        // Act
        var result = await _insertProcessor.ExecuteAsync(_testRecord);

        // Assert
        result.IsSuccess.Should().BeTrue();
        _mockFileManager.Verify(m => m.AllocateNewPageAsync(), Times.Once);
        _mockBufferManager.Verify(m => m.PutPageAsync(It.Is<Page>(p =>
            p.PageId == newPageId && p.Content.Contains(_testRecord))), Times.Once);
    }

    [Fact]
    public async Task ExecuteAsync_WhenGetRandomPageFails_ReturnsError()
    {
        // Arrange
        var errorMessage = "Failed to get random page";
        _mockBufferManager.Setup(m => m.GetRandomPageAsync())
            .ReturnsAsync(Result<Page, BufferError>.Error(new BufferError(errorMessage)));

        // Act
        var result = await _insertProcessor.ExecuteAsync(_testRecord);

        // Assert
        result.IsError.Should().BeTrue();
        result.GetErrorOrThrow().Message.Should().Contain("Failed to get random page");
        _mockFileManager.Verify(m => m.PageHasEnoughSpaceToInsertRecord(It.IsAny<Page>(), It.IsAny<WineRecord>()), Times.Never);
    }

    [Fact]
    public async Task ExecuteAsync_WhenCheckingSpaceFailsWithError_ReturnsError()
    {
        // Arrange
        var page = new Page(1, Array.Empty<WineRecord>());
        var errorMessage = "Failed to check space";

        _mockBufferManager.Setup(m => m.GetRandomPageAsync())
            .ReturnsAsync(Result<Page, BufferError>.Success(page));

        _mockFileManager.Setup(m => m.PageHasEnoughSpaceToInsertRecord(page, _testRecord))
            .ReturnsAsync(Result<bool, StoreError>.Error(new StoreError(errorMessage)));

        // Act
        var result = await _insertProcessor.ExecuteAsync(_testRecord);

        // Assert
        result.IsError.Should().BeTrue();
        result.GetErrorOrThrow().Message.Should().Contain("Failed to check if page has enough space");
    }

    [Fact]
    public async Task ExecuteAsync_WhenAllocatingNewPageFails_ReturnsError()
    {
        // Arrange
        var page = new Page(1, Array.Empty<WineRecord>());
        var errorMessage = "Failed to allocate new page";

        _mockBufferManager.Setup(m => m.GetRandomPageAsync())
            .ReturnsAsync(Result<Page, BufferError>.Success(page));

        _mockFileManager.Setup(m => m.PageHasEnoughSpaceToInsertRecord(page, _testRecord))
            .ReturnsAsync(Result<bool, StoreError>.Success(false));

        _mockFileManager.Setup(m => m.AllocateNewPageAsync())
            .ReturnsAsync(Result<Page, StoreError>.Error(new StoreError(errorMessage)));

        // Act
        var result = await _insertProcessor.ExecuteAsync(_testRecord);

        // Assert
        result.IsError.Should().BeTrue();
        result.GetErrorOrThrow().Message.Should().Contain("Failed to allocate new page");
    }

    [Fact]
    public async Task ExecuteAsync_WhenPutPageFails_ReturnsError()
    {
        // Arrange
        var page = new Page(1, Array.Empty<WineRecord>());
        var errorMessage = "Failed to put page";

        _mockBufferManager.Setup(m => m.GetRandomPageAsync())
            .ReturnsAsync(Result<Page, BufferError>.Success(page));

        _mockFileManager.Setup(m => m.PageHasEnoughSpaceToInsertRecord(page, _testRecord))
            .ReturnsAsync(Result<bool, StoreError>.Success(true));

        _mockBufferManager.Setup(m => m.PutPageAsync(It.IsAny<Page>()))
            .ReturnsAsync(Result<YggdrasilVinum.Models.Unit, BufferError>.Error(new BufferError(errorMessage)));

        // Act
        var result = await _insertProcessor.ExecuteAsync(_testRecord);

        // Assert
        result.IsError.Should().BeTrue();
        result.GetErrorOrThrow().Message.Should().Contain("Failed to put page");
    }
}
