using FluentAssertions;
using Moq;
using Serilog;
using YggdrasilVinum.Models;
using YggdrasilVinum.Services;

namespace YggdrasilVinum.Tests.Unit.Services;

public class HarvestYearSearchProcessorTests : IDisposable
{
    private readonly Mock<IWineProcessor> _mockWineProcessor;
    private readonly HarvestYearSearchProcessor _processor;

    public HarvestYearSearchProcessorTests()
    {
        // Setup Serilog for testing
        Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Debug()
            .WriteTo.Console()
            .CreateLogger();

        _mockWineProcessor = new Mock<IWineProcessor>();
        _processor = new HarvestYearSearchProcessor(_mockWineProcessor.Object);
    }

    public void Dispose()
    {
        Log.CloseAndFlush();
    }

    [Fact]
    public async Task SearchByHarvestYearAsync_Success_ReturnsWines()
    {
        // Arrange
        var harvestYear = 2018;
        var expectedWines = new List<WineRecord>
        {
            new(1, "Cabernet Sauvignon", 2018, WineType.Red), new(2, "Chardonnay", 2018, WineType.White)
        };

        _mockWineProcessor
            .Setup(x => x.SearchByHarvestYearAsync(harvestYear))
            .ReturnsAsync(Result<List<WineRecord>, WineProcessorError>.Success(expectedWines));

        // Act
        var result = await _processor.SearchByHarvestYearAsync(harvestYear);

        // Assert
        result.IsSuccess.Should().BeTrue();
        var wines = result.GetValueOrThrow();
        wines.Should().HaveCount(2);
        wines.Should().Contain(w => w.WineId == 1 && w.Label == "Cabernet Sauvignon");
        wines.Should().Contain(w => w.WineId == 2 && w.Label == "Chardonnay");
    }

    [Fact]
    public async Task SearchByHarvestYearAsync_WineProcessorError_ReturnsError()
    {
        // Arrange
        var harvestYear = 2018;
        var processorError = new WineProcessorError("Wine processor failed");

        _mockWineProcessor
            .Setup(x => x.SearchByHarvestYearAsync(harvestYear))
            .ReturnsAsync(Result<List<WineRecord>, WineProcessorError>.Error(processorError));

        // Act
        var result = await _processor.SearchByHarvestYearAsync(harvestYear);

        // Assert
        result.IsError.Should().BeTrue();
        var error = result.GetErrorOrThrow();
        error.Message.Should().Contain("Wine processor failed");
    }

    [Fact]
    public async Task SearchByHarvestYearAsync_NoWinesFound_ReturnsEmptyArray()
    {
        // Arrange
        var harvestYear = 2018;
        var emptyList = new List<WineRecord>();

        _mockWineProcessor
            .Setup(x => x.SearchByHarvestYearAsync(harvestYear))
            .ReturnsAsync(Result<List<WineRecord>, WineProcessorError>.Success(emptyList));

        // Act
        var result = await _processor.SearchByHarvestYearAsync(harvestYear);

        // Assert
        result.IsSuccess.Should().BeTrue();
        var wines = result.GetValueOrThrow();
        wines.Should().BeEmpty();
    }

    [Fact]
    public async Task SearchByHarvestYearRangeAsync_ValidRange_ReturnsAllWines()
    {
        // Arrange
        var fromYear = 2018;
        var toYear = 2020;

        _mockWineProcessor
            .Setup(x => x.SearchByHarvestYearAsync(2018))
            .ReturnsAsync(Result<List<WineRecord>, WineProcessorError>.Success(new List<WineRecord>
            {
                new(1, "Wine 2018", 2018, WineType.Red)
            }));

        _mockWineProcessor
            .Setup(x => x.SearchByHarvestYearAsync(2019))
            .ReturnsAsync(Result<List<WineRecord>, WineProcessorError>.Success(new List<WineRecord>
            {
                new(2, "Wine 2019 A", 2019, WineType.White), new(3, "Wine 2019 B", 2019, WineType.Red)
            }));

        _mockWineProcessor
            .Setup(x => x.SearchByHarvestYearAsync(2020))
            .ReturnsAsync(Result<List<WineRecord>, WineProcessorError>.Success(new List<WineRecord>
            {
                new(4, "Wine 2020", 2020, WineType.Rose)
            }));

        // Act
        var result = await _processor.SearchByHarvestYearRangeAsync(fromYear, toYear);

        // Assert
        result.IsSuccess.Should().BeTrue();
        var wines = result.GetValueOrThrow();
        wines.Should().HaveCount(4);
        wines.Should().Contain(w => w.HarvestYear == 2018);
        wines.Should().Contain(w => w.HarvestYear == 2019);
        wines.Should().Contain(w => w.HarvestYear == 2020);
    }

    [Fact]
    public async Task SearchByHarvestYearRangeAsync_InvalidRange_ReturnsError()
    {
        // Arrange
        var fromYear = 2020;
        var toYear = 2018; // Invalid: fromYear > toYear

        // Act
        var result = await _processor.SearchByHarvestYearRangeAsync(fromYear, toYear);

        // Assert
        result.IsError.Should().BeTrue();
        var error = result.GetErrorOrThrow();
        error.Message.Should().Contain("fromYear cannot be greater than toYear");
    }

    [Fact]
    public async Task SearchByHarvestYearRangeAsync_PartialError_ReturnsError()
    {
        // Arrange
        var fromYear = 2018;
        var toYear = 2020;

        _mockWineProcessor
            .Setup(x => x.SearchByHarvestYearAsync(2018))
            .ReturnsAsync(Result<List<WineRecord>, WineProcessorError>.Success(new List<WineRecord>
            {
                new(1, "Wine 2018", 2018, WineType.Red)
            }));

        _mockWineProcessor
            .Setup(x => x.SearchByHarvestYearAsync(2019))
            .ReturnsAsync(Result<List<WineRecord>, WineProcessorError>.Error(
                new WineProcessorError("Error searching 2019")));

        // Act
        var result = await _processor.SearchByHarvestYearRangeAsync(fromYear, toYear);

        // Assert
        result.IsError.Should().BeTrue();
        var error = result.GetErrorOrThrow();
        error.Message.Should().Contain("Error searching 2019");
    }

    [Fact]
    public async Task GetWineCountByYearAsync_ValidRange_ReturnsCorrectCounts()
    {
        // Arrange
        var fromYear = 2018;
        var toYear = 2020;

        _mockWineProcessor
            .Setup(x => x.SearchByHarvestYearAsync(2018))
            .ReturnsAsync(Result<List<WineRecord>, WineProcessorError>.Success(new List<WineRecord>
            {
                new(1, "Wine 2018", 2018, WineType.Red)
            }));

        _mockWineProcessor
            .Setup(x => x.SearchByHarvestYearAsync(2019))
            .ReturnsAsync(Result<List<WineRecord>, WineProcessorError>.Success(new List<WineRecord>
            {
                new(2, "Wine 2019 A", 2019, WineType.White), new(3, "Wine 2019 B", 2019, WineType.Red)
            }));

        _mockWineProcessor
            .Setup(x => x.SearchByHarvestYearAsync(2020))
            .ReturnsAsync(Result<List<WineRecord>, WineProcessorError>.Success(new List<WineRecord>()));

        // Act
        var result = await _processor.GetWineCountByYearAsync(fromYear, toYear);

        // Assert
        result.IsSuccess.Should().BeTrue();
        var statistics = result.GetValueOrThrow();
        statistics.Should().HaveCount(2); // Only years with wines
        statistics[2018].Should().Be(1);
        statistics[2019].Should().Be(2);
        statistics.Should().NotContainKey(2020); // No wines for 2020
    }

    [Fact]
    public async Task GetWineCountByYearAsync_InvalidRange_ReturnsError()
    {
        // Arrange
        var fromYear = 2020;
        var toYear = 2018; // Invalid: fromYear > toYear

        // Act
        var result = await _processor.GetWineCountByYearAsync(fromYear, toYear);

        // Assert
        result.IsError.Should().BeTrue();
        var error = result.GetErrorOrThrow();
        error.Message.Should().Contain("fromYear cannot be greater than toYear");
    }

    [Fact]
    public async Task GetWineCountByYearAsync_SearchError_ReturnsError()
    {
        // Arrange
        var fromYear = 2018;
        var toYear = 2018;

        _mockWineProcessor
            .Setup(x => x.SearchByHarvestYearAsync(2018))
            .ReturnsAsync(Result<List<WineRecord>, WineProcessorError>.Error(
                new WineProcessorError("Search failed")));

        // Act
        var result = await _processor.GetWineCountByYearAsync(fromYear, toYear);

        // Assert
        result.IsError.Should().BeTrue();
        var error = result.GetErrorOrThrow();
        error.Message.Should().Contain("Search failed");
    }

    [Fact]
    public void Constructor_WithNullWineProcessor_ThrowsArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new HarvestYearSearchProcessor(null!));
    }

    [Fact]
    public async Task SearchByHarvestYearAsync_ExceptionInWineProcessor_ReturnsError()
    {
        // Arrange
        var harvestYear = 2018;

        _mockWineProcessor
            .Setup(x => x.SearchByHarvestYearAsync(harvestYear))
            .ThrowsAsync(new InvalidOperationException("Unexpected error"));

        // Act
        var result = await _processor.SearchByHarvestYearAsync(harvestYear);

        // Assert
        result.IsError.Should().BeTrue();
        var error = result.GetErrorOrThrow();
        error.Message.Should().Contain("Unexpected error during search");
    }

    [Fact]
    public async Task SearchByHarvestYearRangeAsync_ExceptionInWineProcessor_ReturnsError()
    {
        // Arrange
        var fromYear = 2018;
        var toYear = 2018;

        _mockWineProcessor
            .Setup(x => x.SearchByHarvestYearAsync(2018))
            .ThrowsAsync(new InvalidOperationException("Unexpected error"));

        // Act
        var result = await _processor.SearchByHarvestYearRangeAsync(fromYear, toYear);

        // Assert
        result.IsError.Should().BeTrue();
        var error = result.GetErrorOrThrow();
        error.Message.Should().Contain("Unexpected error during range search");
    }

    [Fact]
    public async Task GetWineCountByYearAsync_ExceptionInWineProcessor_ReturnsError()
    {
        // Arrange
        var fromYear = 2018;
        var toYear = 2018;

        _mockWineProcessor
            .Setup(x => x.SearchByHarvestYearAsync(2018))
            .ThrowsAsync(new InvalidOperationException("Unexpected error"));

        // Act
        var result = await _processor.GetWineCountByYearAsync(fromYear, toYear);

        // Assert
        result.IsError.Should().BeTrue();
        var error = result.GetErrorOrThrow();
        error.Message.Should().Contain("Unexpected error during statistics generation");
    }
}
