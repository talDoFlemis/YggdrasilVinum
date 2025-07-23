using FluentAssertions;
using Moq;
using YggdrasilVinum.Index;
using YggdrasilVinum.Models;
using YggdrasilVinum.Services.CommandProcessing;
using YggdrasilVinum.Services.Facades;
using YggdrasilVinum.Storage;

namespace YggdrasilVinum.Tests.Unit.Services;

public class SimplifiedProcessorsTests
{
    private readonly SimplifiedInsertProcessor _insertProcessor;
    private readonly Mock<StorageFacade> _mockStorageFacade;
    private readonly SimplifiedSearchProcessor _searchProcessor;
    private readonly WineRecord _testRecord = new(1, "Test Wine", 2020, WineType.Red);

    public SimplifiedProcessorsTests()
    {
        _mockStorageFacade = new Mock<StorageFacade>(Mock.Of<IBPlusTreeIndex<int, RID>>(), Mock.Of<IFileManager>());
        _insertProcessor = new SimplifiedInsertProcessor(_mockStorageFacade.Object);
        _searchProcessor = new SimplifiedSearchProcessor(_mockStorageFacade.Object);
    }

    [Fact]
    public async Task SimplifiedInsertProcessor_ExecuteAsync_WhenSuccessful_ReturnsRID()
    {
        // Arrange
        var expectedRID = new RID(1, 0);
        _mockStorageFacade.Setup(x => x.InsertWineRecordAsync(_testRecord))
            .ReturnsAsync(Result<RID, StorageFacadeError>.Success(expectedRID));

        // Act
        var result = await _insertProcessor.ExecuteAsync(_testRecord);

        // Assert
        result.IsSuccess.Should().BeTrue();
        var rid = result.GetValueOrThrow();
        rid.Should().Be(expectedRID);

        _mockStorageFacade.Verify(x => x.InsertWineRecordAsync(_testRecord), Times.Once);
    }

    [Fact]
    public async Task SimplifiedInsertProcessor_ExecuteAsync_WhenFails_ReturnsError()
    {
        // Arrange
        var facadeError = new StorageFacadeError("Storage error");
        _mockStorageFacade.Setup(x => x.InsertWineRecordAsync(_testRecord))
            .ReturnsAsync(Result<RID, StorageFacadeError>.Error(facadeError));

        // Act
        var result = await _insertProcessor.ExecuteAsync(_testRecord);

        // Assert
        result.IsError.Should().BeTrue();
        var error = result.GetErrorOrThrow();
        error.ToString().Should().Be("Storage error");
    }

    [Fact]
    public async Task SimplifiedSearchProcessor_ExecuteAsync_WhenSuccessful_ReturnsRecords()
    {
        // Arrange
        var expectedRecords = new[]
        {
            new WineRecord(1, "Wine 1", 2020, WineType.Red), new WineRecord(2, "Wine 2", 2020, WineType.White)
        };

        _mockStorageFacade.Setup(x => x.FindRecordsByYearAsync(2020))
            .ReturnsAsync(Result<IEnumerable<WineRecord>, StorageFacadeError>.Success(expectedRecords));

        // Act
        var result = await _searchProcessor.ExecuteAsync(2020);

        // Assert
        result.IsSuccess.Should().BeTrue();
        var records = result.GetValueOrThrow();
        records.Should().HaveCount(2);
        records.Should().BeEquivalentTo(expectedRecords);

        _mockStorageFacade.Verify(x => x.FindRecordsByYearAsync(2020), Times.Once);
    }

    [Fact]
    public async Task SimplifiedSearchProcessor_ExecuteAsync_WhenFails_ReturnsError()
    {
        // Arrange
        var facadeError = new StorageFacadeError("Search error");
        _mockStorageFacade.Setup(x => x.FindRecordsByYearAsync(2020))
            .ReturnsAsync(Result<IEnumerable<WineRecord>, StorageFacadeError>.Error(facadeError));

        // Act
        var result = await _searchProcessor.ExecuteAsync(2020);

        // Assert
        result.IsError.Should().BeTrue();
        var error = result.GetErrorOrThrow();
        error.ToString().Should().Be("Search error");
    }

    [Fact]
    public void SimplifiedInsertProcessor_Constructor_WithNullFacade_ThrowsArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new SimplifiedInsertProcessor(null!));
    }

    [Fact]
    public void SimplifiedSearchProcessor_Constructor_WithNullFacade_ThrowsArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new SimplifiedSearchProcessor(null!));
    }
}
