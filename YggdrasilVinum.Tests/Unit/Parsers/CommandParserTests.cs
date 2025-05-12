using System.Reflection;
using YggdrasilVinum.Models;
using YggdrasilVinum.Parsers;

namespace YggdrasilVinum.Tests.Unit.Parsers;

public class CommandParserTests
{
    [Fact]
    public void ParseCommand_ValidInsertCommand_ReturnsCorrectCommand()
    {
        // Arrange
        var validLine = "INC:123";
        var lineNumber = 1;

        // Act - Use reflection to access private method
        var method = typeof(CommandParser).GetMethod("ParseCommand",
            BindingFlags.NonPublic | BindingFlags.Static);
        var result = (Result<CommandParser.Command, CommandParser.ParseError>)method.Invoke(
            null, new object[] { validLine, lineNumber });

        // Assert
        Assert.True(result.IsSuccess);
        var command = result.GetValueOrThrow();
        Assert.Equal(CommandParser.CommandType.Insert, command.Type);
        Assert.Equal(123, command.Key);
    }

    [Fact]
    public void ParseCommand_ValidSearchCommand_ReturnsCorrectCommand()
    {
        // Arrange
        var validLine = "BUS=:456";
        var lineNumber = 1;

        // Act - Use reflection to access private method
        var method = typeof(CommandParser).GetMethod("ParseCommand",
            BindingFlags.NonPublic | BindingFlags.Static);
        var result = (Result<CommandParser.Command, CommandParser.ParseError>)method.Invoke(
            null, new object[] { validLine, lineNumber });

        // Assert
        Assert.True(result.IsSuccess);
        var command = result.GetValueOrThrow();
        Assert.Equal(CommandParser.CommandType.Search, command.Type);
        Assert.Equal(456, command.Key);
    }

    [Theory]
    [InlineData("", "Command line is empty")]
    [InlineData("InvalidFormat", "Command missing colon separator")]
    [InlineData("UNKNOWN:123", "Unknown command type: UNKNOWN")]
    [InlineData("INC:abc", "Invalid key value in command")]
    [InlineData("BUS=:xyz", "Invalid key value in command")]
    public void ParseCommand_InvalidInputs_ReturnsError(string invalidLine, string expectedErrorMessage)
    {
        // Arrange
        var lineNumber = 1;

        // Act - Use reflection to access private method
        var method = typeof(CommandParser).GetMethod("ParseCommand",
            BindingFlags.NonPublic | BindingFlags.Static);
        var result = (Result<CommandParser.Command, CommandParser.ParseError>)method.Invoke(
            null, new object[] { invalidLine, lineNumber });

        // Assert
        Assert.True(result.IsError);
        var error = result.GetErrorOrThrow();
        Assert.Equal(expectedErrorMessage, error.Message);
        Assert.Equal(lineNumber, error.LineNumber);
    }

    [Fact]
    public void ParseHeader_ValidHeader_ReturnsCorrectHeader()
    {
        // Arrange
        var validHeader = "FLH/5";
        var lineNumber = 0;

        // Act - Use reflection to access private method
        var method = typeof(CommandParser).GetMethod("ParseHeader",
            BindingFlags.NonPublic | BindingFlags.Static);
        var result = (Result<CommandParser.CommandFileHeader, CommandParser.ParseError>)method.Invoke(
            null, new object[] { validHeader, lineNumber });

        // Assert
        Assert.True(result.IsSuccess);
        var header = result.GetValueOrThrow();
        Assert.Equal(5, header.MaxChildren);
    }

    [Theory]
    [InlineData("", "Header line is empty")]
    [InlineData("InvalidHeader", "Header must start with 'FLH/'")]
    [InlineData("FLH/abc", "Invalid max children value in header")]
    [InlineData("FLH/0", "MaxChildren must be greater than 1")]
    [InlineData("FLH/-5", "MaxChildren must be greater than 1")]
    public void ParseHeader_InvalidInputs_ReturnsError(string invalidHeader, string expectedErrorMessage)
    {
        // Arrange
        var lineNumber = 0;

        // Act - Use reflection to access private method
        var method = typeof(CommandParser).GetMethod("ParseHeader",
            BindingFlags.NonPublic | BindingFlags.Static);
        var result = (Result<CommandParser.CommandFileHeader, CommandParser.ParseError>)method.Invoke(
            null, new object[] { invalidHeader, lineNumber });

        // Assert
        Assert.True(result.IsError);
        var error = result.GetErrorOrThrow();
        Assert.Equal(expectedErrorMessage, error.Message);
        Assert.Equal(lineNumber, error.LineNumber);
    }

    [Fact]
    public void ParseCommandsFromString_ValidInput_ReturnsCorrectCommands()
    {
        // Arrange
        var testData = CommandParserTestHelper.TestCommandData.Take(2).ToList();
        var validContent = CommandParserTestHelper.GenerateTestCommandContent(testData, 5);

        // Act
        var result = CommandParser.ParseCommandsFromString(validContent);

        // Assert
        Assert.True(result.IsSuccess);
        var (header, commands) = result.GetValueOrThrow();
        Assert.Equal(5, header.MaxChildren);
        Assert.Equal(2, commands.Count);
        Assert.Equal(testData[0].Type, commands[0].Type);
        Assert.Equal(testData[0].Key, commands[0].Key);
        Assert.Equal(testData[1].Type, commands[1].Type);
        Assert.Equal(testData[1].Key, commands[1].Key);
    }

    [Fact]
    public void ParseCommandsFromString_EmptyInput_ReturnsError()
    {
        // Arrange
        var emptyContent = "";

        // Act
        var result = CommandParser.ParseCommandsFromString(emptyContent);

        // Assert
        Assert.True(result.IsError);
        var error = result.GetErrorOrThrow();
        Assert.Equal("Command string is empty", error.Message);
        Assert.Equal(0, error.LineNumber);
    }

    [Fact]
    public void ParseCommandsFromString_MissingHeaderLine_ReturnsError()
    {
        // Arrange
        var contentWithoutHeader = "INC:123\nBUS=:456";

        // Act
        var result = CommandParser.ParseCommandsFromString(contentWithoutHeader);

        // Assert
        Assert.True(result.IsError);
        var error = result.GetErrorOrThrow();
        Assert.Equal("Header must start with 'FLH/'", error.Message);
    }

    [Fact]
    public void ParseCommandsFromReader_MissingHeaderLine_ReturnsError()
    {
        // Arrange
        var emptyContent = "";
        using var reader = new StringReader(emptyContent);

        // Act
        var result = CommandParser.ParseCommandsFromReader(reader);

        // Assert
        Assert.True(result.IsError);
        var error = result.GetErrorOrThrow();
        Assert.Equal("Command input is empty", error.Message);
        Assert.Equal(0, error.LineNumber);
    }

    [Fact]
    public void ParseCommandsFromString_SkipsEmptyLines()
    {
        // Arrange
        var contentWithEmptyLines = "FLH/5\n\nINC:123\n\nBUS=:456\n\n";

        // Act
        var result = CommandParser.ParseCommandsFromString(contentWithEmptyLines);

        // Assert
        Assert.True(result.IsSuccess);
        var (header, commands) = result.GetValueOrThrow();
        Assert.Equal(5, header.MaxChildren);
        Assert.Equal(2, commands.Count);
    }

    [Fact]
    public void ParseCommandsFromString_WithMaxIntValues_HandlesCorrectly()
    {
        // Arrange
        var contentWithMaxInt = $"FLH/10\nINC:{int.MaxValue}";

        // Act
        var result = CommandParser.ParseCommandsFromString(contentWithMaxInt);

        // Assert
        Assert.True(result.IsSuccess);
        var (_, commands) = result.GetValueOrThrow();
        Assert.Equal(int.MaxValue, commands[0].Key);
    }

    [Fact]
    public void ParseCommandsFromString_WithLargeDataset_ProcessesAllCommands()
    {
        // Arrange
        const int commandCount = 100;
        var largeDataset = new List<(CommandParser.CommandType Type, int Key)>(commandCount);

        for (var i = 0; i < commandCount; i++)
            largeDataset.Add((i % 2 == 0 ? CommandParser.CommandType.Insert : CommandParser.CommandType.Search, i));

        var largeDatasetContent = CommandParserTestHelper.GenerateTestCommandContent(largeDataset);

        // Act
        var result = CommandParser.ParseCommandsFromString(largeDatasetContent);

        // Assert
        Assert.True(result.IsSuccess);
        var (_, commands) = result.GetValueOrThrow();
        Assert.Equal(commandCount, commands.Count);

        for (var i = 0; i < commandCount; i++)
        {
            Assert.Equal(largeDataset[i].Type, commands[i].Type);
            Assert.Equal(largeDataset[i].Key, commands[i].Key);
        }
    }

    [Fact]
    public void ParseCommandsFromReader_WithOneValidLineBeforeInvalidLine_StopsProcessingAtInvalidLine()
    {
        // Arrange
        var mixedContent = "FLH/10\nINC:123\nINVALID:xyz";
        using var reader = new StringReader(mixedContent);

        // Act
        var result = CommandParser.ParseCommandsFromReader(reader);

        // Assert
        Assert.True(result.IsError);
        var error = result.GetErrorOrThrow();
        Assert.Equal("Invalid key value in command", error.Message);
        Assert.Equal(2, error.LineNumber);
    }

    [Fact]
    public void ParseCommandsFromString_WithMultipleErrorTypes_FailsOnFirstError()
    {
        // Arrange
        var contentWithMultipleErrors = CommandParserTestHelper.GenerateInvalidTestCommandContent();

        // Act
        var result = CommandParser.ParseCommandsFromString(contentWithMultipleErrors);

        // Assert
        Assert.True(result.IsError);
        var error = result.GetErrorOrThrow();
        Assert.Equal("Unknown command type: INVALID", error.Message);
        Assert.Equal(2, error.LineNumber); // Should fail on line 2 (0-based index + 1 for header)
    }

    [Fact]
    public void CommandFileHeader_WithInvalidValue_ThrowsArgumentException()
    {
        // Act & Assert
        var exception = Assert.Throws<ArgumentException>(() => new CommandParser.CommandFileHeader(1));
        Assert.Equal("MaxChildren must be greater than 1", exception.Message);
    }

    [Fact]
    public void ParseError_ToString_ReturnsFormattedString()
    {
        // Arrange
        var error = new CommandParser.ParseError("Test error message", 42);

        // Act
        var result = error.ToString();

        // Assert
        Assert.Equal("Line 42: Test error message", result);
    }
}
