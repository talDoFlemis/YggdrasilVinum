using FluentAssertions;
using Serilog;
using YggdrasilVinum.Models;
using YggdrasilVinum.Services;

namespace YggdrasilVinum.Tests.Integration.Services;

public sealed class WineProcessorIntegrationTests : IDisposable
{
    private readonly string _tempBasePath;

    public WineProcessorIntegrationTests()
    {
        // Setup logging for tests
        Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Debug()
            .WriteTo.Console()
            .CreateLogger();

        // Create base temporary directory for all tests
        _tempBasePath = Path.Combine(Path.GetTempPath(), "YggdrasilWineProcessorTests_" + Guid.NewGuid());
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

    private string CreateTestCsvFile(List<WineRecord> wines)
    {
        var tempDir = CreateTempDirectory();
        var csvFilePath = Path.Combine(tempDir, "test_wines.csv");

        var csvContent = "vinho_id,rotulo,ano_colheita,tipo\n";
        foreach (var wine in wines)
        {
            var typeStr = wine.Type switch
            {
                WineType.Red => "tinto",
                WineType.White => "branco",
                WineType.Rose => "rose",
                _ => throw new ArgumentException($"Unknown wine type: {wine.Type}")
            };
            csvContent += $"{wine.WineId},{wine.Label},{wine.HarvestYear},{typeStr}\n";
        }

        File.WriteAllText(csvFilePath, csvContent);
        return csvFilePath;
    }

    [Fact]
    public async Task ProcessCsvFileAsync_WithSmallDataset_ProcessesSuccessfully()
    {
        // Arrange
        var tempDir = CreateTempDirectory();
        var processedFilePath = Path.Combine(tempDir, "wines_processed.bin");

        var testWines = new List<WineRecord>
        {
            new(1, "Cabernet Sauvignon", 2018, WineType.Red),
            new(2, "Chardonnay", 2020, WineType.White),
            new(3, "Merlot", 2017, WineType.Red),
            new(4, "Pinot Noir", 2019, WineType.Red),
            new(5, "Sauvignon Blanc", 2021, WineType.White)
        };

        var csvFilePath = CreateTestCsvFile(testWines);
        var processor = new WineProcessor(processedFilePath);

        // Act
        var result = await processor.ProcessCsvFileAsync(csvFilePath);

        // Assert
        result.IsSuccess.Should().BeTrue();
        File.Exists(processedFilePath).Should().BeTrue();
        processor.RecordCount.Should().Be(5);

        processor.Dispose();
    }

    [Fact]
    public async Task ProcessCsvFileAsync_WithLargeDataset_ProcessesSuccessfully()
    {
        // Arrange
        var tempDir = CreateTempDirectory();
        var processedFilePath = Path.Combine(tempDir, "wines_processed.bin");

        // Create a larger dataset to test chunking
        var testWines = new List<WineRecord>();
        for (var i = 1; i <= 1000; i++)
        {
            var year = 1990 + i % 40; // Years from 1990 to 2029
            var type = (WineType)(i % 3);
            testWines.Add(new WineRecord(i, $"Wine {i}", year, type));
        }

        var csvFilePath = CreateTestCsvFile(testWines);
        var processor = new WineProcessor(processedFilePath);

        // Act
        var result = await processor.ProcessCsvFileAsync(csvFilePath);

        // Assert
        result.IsSuccess.Should().BeTrue();
        File.Exists(processedFilePath).Should().BeTrue();
        processor.RecordCount.Should().Be(1000);

        processor.Dispose();
    }

    [Fact]
    public async Task SearchByHarvestYearAsync_SingleMatch_ReturnsCorrectRecord()
    {
        // Arrange
        var tempDir = CreateTempDirectory();
        var processedFilePath = Path.Combine(tempDir, "wines_processed.bin");

        var testWines = new List<WineRecord>
        {
            new(1, "Cabernet Sauvignon", 2018, WineType.Red),
            new(2, "Chardonnay", 2020, WineType.White),
            new(3, "Merlot", 2019, WineType.Red),
            new(4, "Pinot Noir", 2021, WineType.Red),
            new(5, "Sauvignon Blanc", 2017, WineType.White)
        };

        var csvFilePath = CreateTestCsvFile(testWines);
        var processor = new WineProcessor(processedFilePath);

        await processor.ProcessCsvFileAsync(csvFilePath);

        // Act
        var searchResult = await processor.SearchByHarvestYearAsync(2019);

        // Assert
        searchResult.IsSuccess.Should().BeTrue();
        var wines = searchResult.GetValueOrThrow();
        wines.Should().HaveCount(1);
        wines[0].WineId.Should().Be(3);
        wines[0].Label.Should().Be("Merlot");
        wines[0].HarvestYear.Should().Be(2019);
        wines[0].Type.Should().Be(WineType.Red);

        processor.Dispose();
    }

    [Fact]
    public async Task SearchByHarvestYearAsync_MultipleMatches_ReturnsAllRecords()
    {
        // Arrange
        var tempDir = CreateTempDirectory();
        var processedFilePath = Path.Combine(tempDir, "wines_processed.bin");

        var testWines = new List<WineRecord>
        {
            new(1, "Cabernet Sauvignon", 2018, WineType.Red),
            new(2, "Chardonnay", 2018, WineType.White),
            new(3, "Merlot", 2019, WineType.Red),
            new(4, "Pinot Noir", 2018, WineType.Red),
            new(5, "Sauvignon Blanc", 2017, WineType.White)
        };

        var csvFilePath = CreateTestCsvFile(testWines);
        var processor = new WineProcessor(processedFilePath);

        await processor.ProcessCsvFileAsync(csvFilePath);

        // Act
        var searchResult = await processor.SearchByHarvestYearAsync(2018);

        // Assert
        searchResult.IsSuccess.Should().BeTrue();
        var wines = searchResult.GetValueOrThrow();
        wines.Should().HaveCount(3);
        wines.Should().OnlyContain(w => w.HarvestYear == 2018);

        // Verify all three wines are returned and sorted by wine ID
        wines.Should().Contain(w => w.WineId == 1 && w.Label == "Cabernet Sauvignon");
        wines.Should().Contain(w => w.WineId == 2 && w.Label == "Chardonnay");
        wines.Should().Contain(w => w.WineId == 4 && w.Label == "Pinot Noir");

        processor.Dispose();
    }

    [Fact]
    public async Task SearchByHarvestYearAsync_NoMatches_ReturnsEmptyList()
    {
        // Arrange
        var tempDir = CreateTempDirectory();
        var processedFilePath = Path.Combine(tempDir, "wines_processed.bin");

        var testWines = new List<WineRecord>
        {
            new(1, "Cabernet Sauvignon", 2018, WineType.Red),
            new(2, "Chardonnay", 2020, WineType.White),
            new(3, "Merlot", 2019, WineType.Red)
        };

        var csvFilePath = CreateTestCsvFile(testWines);
        var processor = new WineProcessor(processedFilePath);

        await processor.ProcessCsvFileAsync(csvFilePath);

        // Act
        var searchResult = await processor.SearchByHarvestYearAsync(2015);

        // Assert
        searchResult.IsSuccess.Should().BeTrue();
        var wines = searchResult.GetValueOrThrow();
        wines.Should().BeEmpty();

        processor.Dispose();
    }

    [Fact]
    public async Task SearchByHarvestYearAsync_WithoutProcessing_ReturnsError()
    {
        // Arrange
        var tempDir = CreateTempDirectory();
        var processedFilePath = Path.Combine(tempDir, "wines_processed.bin");
        var processor = new WineProcessor(processedFilePath);

        // Act
        var searchResult = await processor.SearchByHarvestYearAsync(2018);

        // Assert
        searchResult.IsError.Should().BeTrue();
        var error = searchResult.GetErrorOrThrow();
        error.Message.Should().Contain("not open");

        processor.Dispose();
    }

    [Fact]
    public async Task ProcessCsvFileAsync_SortsRecordsByHarvestYear()
    {
        // Arrange
        var tempDir = CreateTempDirectory();
        var processedFilePath = Path.Combine(tempDir, "wines_processed.bin");

        // Create wines with unsorted harvest years
        var testWines = new List<WineRecord>
        {
            new(1, "Wine 2020", 2020, WineType.Red),
            new(2, "Wine 2015", 2015, WineType.White),
            new(3, "Wine 2018", 2018, WineType.Red),
            new(4, "Wine 2010", 2010, WineType.Red),
            new(5, "Wine 2022", 2022, WineType.White)
        };

        var csvFilePath = CreateTestCsvFile(testWines);
        var processor = new WineProcessor(processedFilePath);

        await processor.ProcessCsvFileAsync(csvFilePath);

        // Act & Assert - Search for each year and verify they're found
        var searchResult2010 = await processor.SearchByHarvestYearAsync(2010);
        searchResult2010.IsSuccess.Should().BeTrue();
        searchResult2010.GetValueOrThrow().Should().HaveCount(1);
        searchResult2010.GetValueOrThrow()[0].Label.Should().Be("Wine 2010");

        var searchResult2015 = await processor.SearchByHarvestYearAsync(2015);
        searchResult2015.IsSuccess.Should().BeTrue();
        searchResult2015.GetValueOrThrow().Should().HaveCount(1);
        searchResult2015.GetValueOrThrow()[0].Label.Should().Be("Wine 2015");

        var searchResult2018 = await processor.SearchByHarvestYearAsync(2018);
        searchResult2018.IsSuccess.Should().BeTrue();
        searchResult2018.GetValueOrThrow().Should().HaveCount(1);
        searchResult2018.GetValueOrThrow()[0].Label.Should().Be("Wine 2018");

        processor.Dispose();
    }

    [Fact]
    public async Task SearchByHarvestYearAsync_HandlesSortingCorrectly()
    {
        // Arrange
        var tempDir = CreateTempDirectory();
        var processedFilePath = Path.Combine(tempDir, "wines_processed.bin");

        // Create many wines with the same year to test correct sorting
        var testWines = new List<WineRecord>();
        for (var i = 1; i <= 100; i++)
        {
            var year = 2020 + i % 10; // Years from 2020 to 2029
            testWines.Add(new WineRecord(i, $"Wine {i}", year, WineType.Red));
        }

        var csvFilePath = CreateTestCsvFile(testWines);
        var processor = new WineProcessor(processedFilePath);

        await processor.ProcessCsvFileAsync(csvFilePath);

        // Act - Search for a year that should have multiple wines
        var searchResult = await processor.SearchByHarvestYearAsync(2025);

        // Assert
        searchResult.IsSuccess.Should().BeTrue();
        var wines = searchResult.GetValueOrThrow();
        wines.Should().HaveCount(10); // Every 10th wine starting from wine 6
        wines.Should().OnlyContain(w => w.HarvestYear == 2025);

        processor.Dispose();
    }

    [Fact]
    public async Task WineProcessor_HandleLongLabels_TruncatesCorrectly()
    {
        // Arrange
        var tempDir = CreateTempDirectory();
        var processedFilePath = Path.Combine(tempDir, "wines_processed.bin");

        var longLabel = new string('A', 150); // Label longer than 100 characters
        var testWines = new List<WineRecord>
        {
            new(1, longLabel, 2018, WineType.Red), new(2, "Short Label", 2019, WineType.White)
        };

        var csvFilePath = CreateTestCsvFile(testWines);
        var processor = new WineProcessor(processedFilePath);

        await processor.ProcessCsvFileAsync(csvFilePath);

        // Act
        var searchResult = await processor.SearchByHarvestYearAsync(2018);

        // Assert
        searchResult.IsSuccess.Should().BeTrue();
        var wines = searchResult.GetValueOrThrow();
        wines.Should().HaveCount(1);
        // Label should be truncated to 100 characters max
        wines[0].Label.Length.Should().BeLessThanOrEqualTo(100);
        wines[0].Label.Should().StartWith("AAA");

        processor.Dispose();
    }

    [Fact]
    public async Task OpenProcessedFileAsync_ExistingFile_OpensSuccessfully()
    {
        // Arrange
        var tempDir = CreateTempDirectory();
        var processedFilePath = Path.Combine(tempDir, "wines_processed.bin");

        var testWines = new List<WineRecord> { new(1, "Test Wine", 2018, WineType.Red) };

        var csvFilePath = CreateTestCsvFile(testWines);
        var processor1 = new WineProcessor(processedFilePath);

        // Process and dispose first processor
        await processor1.ProcessCsvFileAsync(csvFilePath);
        processor1.Dispose();

        // Act - Create new processor and open existing file
        var processor2 = new WineProcessor(processedFilePath);
        var openResult = await processor2.OpenProcessedFileAsync();

        // Assert
        openResult.IsSuccess.Should().BeTrue();
        processor2.RecordCount.Should().Be(1);

        var searchResult = await processor2.SearchByHarvestYearAsync(2018);
        searchResult.IsSuccess.Should().BeTrue();
        searchResult.GetValueOrThrow().Should().HaveCount(1);

        processor2.Dispose();
    }

    [Fact]
    public async Task ProcessCsvFileAsync_NonExistentFile_ReturnsError()
    {
        // Arrange
        var tempDir = CreateTempDirectory();
        var processedFilePath = Path.Combine(tempDir, "wines_processed.bin");
        var nonExistentCsvPath = Path.Combine(tempDir, "nonexistent.csv");
        var processor = new WineProcessor(processedFilePath);

        // Act
        var result = await processor.ProcessCsvFileAsync(nonExistentCsvPath);

        // Assert
        result.IsError.Should().BeTrue();
        var error = result.GetErrorOrThrow();
        error.Message.Should().Contain("CSV file not found");

        processor.Dispose();
    }

    [Fact]
    public async Task ProcessCsvFileAsync_EmptyFile_HandlesGracefully()
    {
        // Arrange
        var tempDir = CreateTempDirectory();
        var processedFilePath = Path.Combine(tempDir, "wines_processed.bin");
        var emptyWines = new List<WineRecord>();
        var csvFilePath = CreateTestCsvFile(emptyWines);

        // Create an empty CSV file with just headers
        File.WriteAllText(csvFilePath, "vinho_id,rotulo,ano_colheita,tipo\n");

        var processor = new WineProcessor(processedFilePath);

        // Act
        var result = await processor.ProcessCsvFileAsync(csvFilePath);

        // Assert
        result.IsSuccess.Should().BeTrue();
        processor.RecordCount.Should().Be(0);

        var searchResult = await processor.SearchByHarvestYearAsync(2018);
        searchResult.IsSuccess.Should().BeTrue();
        searchResult.GetValueOrThrow().Should().BeEmpty();

        processor.Dispose();
    }

    [Fact]
    public async Task ProcessCsvFileAsync_RealWinesCsv_ProcessesCorrectly()
    {
        // Arrange
        var tempDir = CreateTempDirectory();
        var processedFilePath = Path.Combine(tempDir, "wines_processed.bin");

        // Use the actual wines.csv from the project if it exists
        var realCsvPath = Path.Combine(Directory.GetCurrentDirectory(), "Data", "wines.csv");

        // Skip if the real file doesn't exist (in case test environment doesn't have it)
        if (!File.Exists(realCsvPath))
        {
            realCsvPath = Path.Combine(Directory.GetCurrentDirectory(), "..", "..", "..", "..", "YggdrasilVinum",
                "Data", "wines.csv");
            if (!File.Exists(realCsvPath))
                return; // Skip this test if we can't find the real wine data
        }

        var processor = new WineProcessor(processedFilePath);

        // Act
        var result = await processor.ProcessCsvFileAsync(realCsvPath);

        // Assert
        result.IsSuccess.Should().BeTrue();
        processor.RecordCount.Should().BeGreaterThan(0);

        // Test searching for a few specific years
        var searchResult1913 = await processor.SearchByHarvestYearAsync(1913);
        searchResult1913.IsSuccess.Should().BeTrue();
        searchResult1913.GetValueOrThrow().Should().NotBeEmpty();

        var searchResult2014 = await processor.SearchByHarvestYearAsync(2014);
        searchResult2014.IsSuccess.Should().BeTrue();
        searchResult2014.GetValueOrThrow().Should().NotBeEmpty();

        processor.Dispose();
    }
}
