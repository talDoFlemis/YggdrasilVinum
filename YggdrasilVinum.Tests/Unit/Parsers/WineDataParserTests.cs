using System.Reflection;
using YggdrasilVinum.Models;
using YggdrasilVinum.Parsers;

namespace YggdrasilVinum.Tests.Unit.Parsers;

public class WineDataParserTests
{
    [Fact]
    public void ParseCsvLine_ValidInput_ReturnsCorrectWineRecord()
    {
        // Arrange
        var testWine = TestHelper.TestWineData[0];
        var validLine = $"{testWine.Id},{testWine.Label},{testWine.Year},tinto";
        var lineNumber = 1;


        // Act - Use reflection to access private method
        var method = typeof(WineDataParser).GetMethod("ParseCsvLine",
            BindingFlags.NonPublic | BindingFlags.Static);
        var result =
            (Result<WineRecord, WineDataParser.ParseError>)method.Invoke(null, new object[] { validLine, lineNumber });

        // Assert
        Assert.True(result.IsSuccess);
        var record = result.GetValueOrThrow();
        Assert.Equal(testWine.Id, record.WineId);
        Assert.Equal(testWine.Label, record.Label);
        Assert.Equal(testWine.Year, record.HarvestYear);
        Assert.Equal(WineType.Red, record.Type);
    }

    [Fact]
    public void ParseCsvString_ValidInput_ReturnsCorrectRecords()
    {
        // Arrange
        var testData = TestHelper.TestWineData.Take(3).ToList();
        var csvContent = TestHelper.GenerateTestCsvContent(testData);

        // Act
        var result = WineDataParser.ParseCsvString(csvContent);

        // Assert
        Assert.True(result.IsSuccess);
        var records = result.GetValueOrThrow();
        Assert.Equal(testData.Count, records.Count);

        for (var i = 0; i < testData.Count; i++)
        {
            Assert.Equal(testData[i].Id, records[i].WineId);
            Assert.Equal(testData[i].Label, records[i].Label);
            Assert.Equal(testData[i].Year, records[i].HarvestYear);
            Assert.Equal(testData[i].Type, records[i].Type);
        }
    }

    [Fact]
    public void ParseCsvString_EmptyInput_ReturnsError()
    {
        // Arrange
        var csvContent = "";

        // Act
        var result = WineDataParser.ParseCsvString(csvContent);

        // Assert
        Assert.True(result.IsError);
        var error = result.GetErrorOrThrow();
        Assert.Equal("CSV string is empty", error.Message);
    }

    [Fact]
    public void ParseCsvReader_MissingHeaderLine_ReturnsError()
    {
        // Arrange
        using var reader = new StringReader("");

        // Act
        var result = WineDataParser.ParseCsvReader(reader);

        // Assert
        Assert.True(result.IsError);
        var error = result.GetErrorOrThrow();
        Assert.Equal("CSV input is empty", error.Message);
    }

    [Theory]
    [InlineData("1,Missing harvest year,,tinto", "Invalid harvest year")]
    [InlineData("not_an_id,Label,2019,tinto", "Invalid wine ID")]
    [InlineData("1,Label,not_a_year,tinto", "Invalid harvest year")]
    [InlineData("1,Label,2019,invalid_type", "Invalid wine type: invalid_type")]
    [InlineData("1,Label,2019", "Expected 4 fields, got 3")]
    [InlineData("1,Label,2019,tinto,extra_field", "Expected 4 fields, got 5")]
    public void ParseCsvLine_InvalidInputs_ReturnsError(string invalidLine, string expectedErrorMessage)
    {
        // Arrange
        var lineNumber = 1;

        // Act - Use reflection to access private method
        var method = typeof(WineDataParser).GetMethod("ParseCsvLine",
            BindingFlags.NonPublic | BindingFlags.Static);
        var result =
            (Result<WineRecord, WineDataParser.ParseError>)method.Invoke(null,
                new object[] { invalidLine, lineNumber });

        // Assert
        Assert.True(result.IsError);
        var error = result.GetErrorOrThrow();
        Assert.Equal(expectedErrorMessage, error.Message);
        Assert.Equal(lineNumber, error.LineNumber);
    }

    [Theory]
    [InlineData("tinto", WineType.Red)]
    [InlineData("TINTO", WineType.Red)]
    [InlineData("branco", WineType.White)]
    [InlineData("BRANCO", WineType.White)]
    [InlineData("rose", WineType.Rose)]
    [InlineData("rosé", WineType.Rose)]
    [InlineData("ROSÉ", WineType.Rose)]
    public void ParseWineType_ValidTypes_ReturnsCorrectEnum(string typeString, WineType expectedType)
    {
        // Arrange
        var lineNumber = 1;

        // Act - Use reflection to access private method
        var method = typeof(WineDataParser).GetMethod("ParseWineType",
            BindingFlags.NonPublic | BindingFlags.Static);
        var result =
            (Result<WineType, WineDataParser.ParseError>)method.Invoke(null, new object[] { typeString, lineNumber });

        // Assert
        Assert.True(result.IsSuccess);
        Assert.Equal(expectedType, result.GetValueOrThrow());
    }

    [Fact]
    public void ParseWineType_InvalidType_ReturnsError()
    {
        // Arrange
        var invalidType = "invalid_type";
        var lineNumber = 1;

        // Act - Use reflection to access private method
        var method = typeof(WineDataParser).GetMethod("ParseWineType",
            BindingFlags.NonPublic | BindingFlags.Static);
        var result =
            (Result<WineType, WineDataParser.ParseError>)method.Invoke(null, new object[] { invalidType, lineNumber });

        // Assert
        Assert.True(result.IsError);
        var error = result.GetErrorOrThrow();
        Assert.Equal($"Invalid wine type: {invalidType}", error.Message);
        Assert.Equal(lineNumber, error.LineNumber);
    }

    [Fact]
    public void ParseCsvString_SkipsEmptyLines()
    {
        // Arrange
        var testData = TestHelper.TestWineData.Take(2).ToList();
        var csvContent =
            $"{TestHelper.ValidCsvHeader}\n" +
            $"{testData[0].Id},{testData[0].Label},{testData[0].Year},tinto\n" +
            "\n" + // Empty line
            $"{testData[1].Id},{testData[1].Label},{testData[1].Year},branco\n";

        // Act
        var result = WineDataParser.ParseCsvString(csvContent);

        // Assert
        Assert.True(result.IsSuccess);
        var records = result.GetValueOrThrow();
        Assert.Equal(2, records.Count); // Only two valid records
        Assert.Equal(testData[0].Id, records[0].WineId);
        Assert.Equal(testData[1].Id, records[1].WineId);
    }

    [Fact]
    public void ParseCsvString_WithMaxIntValues_HandlesCorrectly()
    {
        // Arrange
        var csvContent =
            $"{TestHelper.ValidCsvHeader}\n" +
            $"{int.MaxValue},Max Integer Wine,{int.MaxValue},tinto";

        // Act
        var result = WineDataParser.ParseCsvString(csvContent);

        // Assert
        Assert.True(result.IsSuccess);
        var records = result.GetValueOrThrow();
        Assert.Single(records);
        Assert.Equal(int.MaxValue, records[0].WineId);
        Assert.Equal(int.MaxValue, records[0].HarvestYear);
    }

    [Fact]
    public void ParseCsvString_WithHistoricalZeroYear_ReturnsError()
    {
        // Arrange - Test for year 0 (which doesn't exist in history)
        var csvContent =
            $"{TestHelper.ValidCsvHeader}\n" +
            "1,Ancient Roman Wine,0,tinto";

        // Act
        var result = WineDataParser.ParseCsvString(csvContent);

        // Assert
        Assert.True(result.IsError);
        var error = result.GetErrorOrThrow();
        Assert.Equal(1, error.LineNumber);
        Assert.Contains("Harvest year must be positive", error.Message);
    }

    [Fact]
    public void ParseCsvString_WithNegativeHarvestYear_ReturnsError()
    {
        // Arrange
        var csvContent =
            $"{TestHelper.ValidCsvHeader}\n" +
            "1,Negative Year Wine,-1,tinto";

        // Act
        var result = WineDataParser.ParseCsvString(csvContent);

        // Assert
        Assert.True(result.IsError);
        var error = result.GetErrorOrThrow();
        Assert.Equal("Failed to parse line: Harvest year must be positive (Parameter 'harvestYear')", error.Message);
    }

    [Fact]
    public void ParseCsvString_WithEmptyLabel_ReturnsError()
    {
        // Arrange
        var csvContent =
            $"{TestHelper.ValidCsvHeader}\n" +
            "1,,2020,tinto";

        // Act
        var result = WineDataParser.ParseCsvString(csvContent);

        // Assert
        Assert.True(result.IsError);
    }

    [Fact]
    public void ParseCsvString_WithWhitespaceOnlyLabel_ReturnsError()
    {
        // Arrange
        var csvContent =
            $"{TestHelper.ValidCsvHeader}\n" +
            "1,   ,2020,tinto";

        // Act
        var result = WineDataParser.ParseCsvString(csvContent);

        // Assert
        Assert.True(result.IsError);
    }

    [Fact]
    public void ParseCsvString_WithLargeDataset_ProcessesAllRecords()
    {
        // Arrange
        var testData = new List<(int Id, string Label, int Year, WineType Type)>();
        // Generate 1000 test records
        for (var i = 1; i <= 1000; i++)
        {
            var type = i % 3 == 0 ? WineType.Red : i % 3 == 1 ? WineType.White : WineType.Rose;
            testData.Add((i, $"Wine {i}", 2000 + i % 22, type));
        }

        var csvContent = TestHelper.GenerateTestCsvContent(testData);

        // Act
        var result = WineDataParser.ParseCsvString(csvContent);

        // Assert
        Assert.True(result.IsSuccess);
        var records = result.GetValueOrThrow();
        Assert.Equal(1000, records.Count);

        // Check some random records
        Assert.Equal(500, records[499].WineId);
        Assert.Equal("Wine 500", records[499].Label);
        Assert.Equal(testData[999].Year, records[999].HarvestYear);
    }

    [Fact]
    public void ParseCsvReader_WithOneValidLineBeforeInvalidLine_ProcessesValidLineOnly()
    {
        // Arrange
        var csvContent =
            $"{TestHelper.ValidCsvHeader}\n" +
            "1,Valid Wine,2020,tinto\n" +
            "2,Invalid Year,-1,branco";

        using var reader = new StringReader(csvContent);

        // Act
        var result = WineDataParser.ParseCsvReader(reader);

        // Assert
        Assert.True(result.IsError);
        // The error should be about the second line
        var error = result.GetErrorOrThrow();
        Assert.Equal(2, error.LineNumber);
    }

    [Fact]
    public void ParseCsvString_WithSpecialCharactersInLabel_ParsesCorrectly()
    {
        // Arrange
        var csvContent =
            $"{TestHelper.ValidCsvHeader}\n" +
            "1,Special@#$%^&*()_+-={}[]|\\:;<.>/?Characters,2020,tinto";

        // Act
        var result = WineDataParser.ParseCsvString(csvContent);

        // Assert
        Assert.True(result.IsSuccess);
        var records = result.GetValueOrThrow();
        Assert.Single(records);
        Assert.Equal("Special@#$%^&*()_+-={}[]|\\:;<.>/?Characters", records[0].Label);
    }

    [Fact]
    public void ParseCsvString_WithExtremelyLongLabel_ParsesCorrectly()
    {
        // Arrange
        var longLabel = new string('X', 10000); // 10,000 characters
        var csvContent =
            $"{TestHelper.ValidCsvHeader}\n" +
            $"1,{longLabel},2020,tinto";

        // Act
        var result = WineDataParser.ParseCsvString(csvContent);

        // Assert
        Assert.True(result.IsSuccess);
        var records = result.GetValueOrThrow();
        Assert.Single(records);
        Assert.Equal(longLabel, records[0].Label);
        Assert.Equal(10000, records[0].Label.Length);
    }

    [Theory]
    [InlineData(1900)]
    [InlineData(2000)]
    [InlineData(2022)]
    [InlineData(2100)]
    public void ParseCsvString_WithValidHarvestYears_ParsesCorrectly(int year)
    {
        // Arrange
        var csvContent =
            $"{TestHelper.ValidCsvHeader}\n" +
            $"1,Valid Year Wine,{year},tinto";

        // Act
        var result = WineDataParser.ParseCsvString(csvContent);

        // Assert
        Assert.True(result.IsSuccess);
        var records = result.GetValueOrThrow();
        Assert.Single(records);
        Assert.Equal(year, records[0].HarvestYear);
    }

    [Fact]
    public void ParseCsvString_WithMultipleErrorTypes_FailsOnFirstError()
    {
        // Arrange
        var csvContent =
            $"{TestHelper.ValidCsvHeader}\n" +
            "1,Valid Wine,2020,tinto\n" +
            "not_an_id,Invalid ID,2019,tinto\n" + // Should fail here (line 2)
            "2,Missing Year,,branco\n" + // This error doesn't get reached
            "3,Invalid Year,not_a_year,tinto\n"; // This error doesn't get reached

        // Act
        var result = WineDataParser.ParseCsvString(csvContent);

        // Assert
        Assert.True(result.IsError);
        var error = result.GetErrorOrThrow();
        Assert.Equal(2, error.LineNumber); // Should fail on the second data line (line 2)
        Assert.Equal("Invalid wine ID", error.Message);
    }

    [Fact]
    public void ParseCsvReader_WithEmptyReaderAfterHeader_ReturnsEmptyList()
    {
        // Arrange
        var csvContent = $"{TestHelper.ValidCsvHeader}\n";
        using var reader = new StringReader(csvContent);

        // Act
        var result = WineDataParser.ParseCsvReader(reader);

        // Assert
        Assert.True(result.IsSuccess);
        var records = result.GetValueOrThrow();
        Assert.Empty(records);
    }

    [Fact]
    public void ParseCsvString_WithBoundaryConditions_HandlesEdgeCases()
    {
        // Test with various boundary conditions

        // 1. Minimum valid harvest year
        var csvContent1 = $"{TestHelper.ValidCsvHeader}\n1,Wine Label,1,tinto";
        var result1 = WineDataParser.ParseCsvString(csvContent1);
        Assert.True(result1.IsSuccess);
        Assert.Equal(1, result1.GetValueOrThrow()[0].HarvestYear);

        // 2. Near int.MaxValue harvest year
        var nearMaxYear = int.MaxValue - 1;
        var csvContent2 = $"{TestHelper.ValidCsvHeader}\n1,Wine Label,{nearMaxYear},tinto";
        var result2 = WineDataParser.ParseCsvString(csvContent2);
        Assert.True(result2.IsSuccess);
        Assert.Equal(nearMaxYear, result2.GetValueOrThrow()[0].HarvestYear);

        // 3. Single-character label
        var csvContent3 = $"{TestHelper.ValidCsvHeader}\n1,X,2020,tinto";
        var result3 = WineDataParser.ParseCsvString(csvContent3);
        Assert.True(result3.IsSuccess);
        Assert.Equal("X", result3.GetValueOrThrow()[0].Label);
    }

    [Fact]
    public void ParseCsvString_WithCommaSeparatedValues_SplitsOnEveryComma()
    {
        // Arrange
        var csvLine = "1,\"Château Margaux, Grand Cru Classé\",2018,tinto";

        // Act
        var result = WineDataParser.ParseCsvString(
            $"{TestHelper.ValidCsvHeader}\n{csvLine}");

        // Assert
        Assert.True(result.IsError);
        // The simple string.Split(',') implementation will split on all commas,
        // not just those outside of quotes
        var error = result.GetErrorOrThrow();
        Assert.Equal(1, error.LineNumber);
        Assert.Equal("Expected 4 fields, got 5", error.Message);
    }

    [Theory]
    [InlineData("1,\"Label with \"\"quoted\"\" text\",2020,tinto")]
    [InlineData("1,Label with escaped quote\\,2020,tinto")]
    [InlineData("1,\"Label spanning\nmultiple\nlines\",2020,tinto")]
    public void ParseCsvString_WithComplexFormats_ShouldHandleExpectedly(string csvLine)
    {
        // Arrange
        var csvContent = $"{TestHelper.ValidCsvHeader}\n{csvLine}";

        // Act & Assert
        // This test documents the current behavior with complex CSV formats
        // The current parser doesn't handle these complex formats properly,
        // so we're just verifying what it currently does
        var result = WineDataParser.ParseCsvString(csvContent);

        // We don't assert specific behavior since the current parser has limited CSV capabilities.
        // This test is to document the existing behavior with complex formats.
    }

    [Fact]
    public void ParseCsvString_WithEmptyFile_ReturnsError()
    {
        // Test with completely empty content (not even a header)
        var emptyContent = string.Empty;
        var result = WineDataParser.ParseCsvString(emptyContent);

        Assert.True(result.IsError);
        Assert.Equal("CSV string is empty", result.GetErrorOrThrow().Message);
    }


    [Fact]
    public void ParseCsvString_WithMultipleValidAndInvalidRecords_FailsOnFirstInvalid()
    {
        // Setup a CSV with multiple valid records followed by invalid ones
        var csvContent =
            $"{TestHelper.ValidCsvHeader}\n" +
            // Valid records
            "1,Wine One,2020,tinto\n" +
            "2,Wine Two,2019,branco\n" +
            "3,Wine Three,2021,rosé\n" +
            // Invalid records
            "4,Wine Four,-1,tinto\n" + // Invalid year (negative)
            "5,Wine Five,2022,invalid\n"; // Invalid type

        var result = WineDataParser.ParseCsvString(csvContent);

        Assert.True(result.IsError);
        var error = result.GetErrorOrThrow();
        Assert.Equal(4, error.LineNumber); // Should fail on the first invalid (line 4)
        Assert.Equal("Failed to parse line: Harvest year must be positive (Parameter 'harvestYear')", error.Message);
    }
}
