using YggdrasilVinum.Models;
using YggdrasilVinum.Parsers;
using YggdrasilVinum.Tests.Unit.Parsers;

namespace YggdrasilVinum.Tests.Integration.Parsers;

public class CommandParserIntegrationTests
{
    // Use CommandParserTestHelper.CreateTempCommandFile instead
    private string CreateTempFileWithContent(string content)
    {
        return CommandParserTestHelper.CreateTempCommandFile(content);
    }

    [Fact]
    public void ParseCommandFile_NonExistentFile_ReturnsError()
    {
        // Arrange
        var nonExistentFilePath = Path.Combine(Path.GetTempPath(), Guid.NewGuid() + ".txt");

        // Act
        var result = CommandParser.ParseCommandFile(nonExistentFilePath);

        // Assert
        Assert.True(result.IsError);
        var error = result.GetErrorOrThrow();
        Assert.Equal($"Command file not found: {nonExistentFilePath}", error.Message);
        Assert.Equal(0, error.LineNumber);
    }

    [Fact]
    public void ParseCommandFile_ValidFile_ReturnsCorrectData()
    {
        // Arrange
        var testData = CommandParserTestHelper.TestCommandData.Take(2).ToList();
        var commandContent = CommandParserTestHelper.GenerateTestCommandContent(testData, 5);

        var tempFilePath = CreateTempFileWithContent(commandContent);

        try
        {
            // Act
            var result = CommandParser.ParseCommandFile(tempFilePath);

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
        finally
        {
            // Cleanup
            CommandParserTestHelper.DeleteTempFile(tempFilePath);
        }
    }

    [Fact]
    public void ParseCommandFile_InvalidContent_ReturnsError()
    {
        // Arrange
        var commandContent = "FLH/5\nINC:123\nINVALID:456"; // Invalid command type

        var tempFilePath = CreateTempFileWithContent(commandContent);

        try
        {
            // Act
            var result = CommandParser.ParseCommandFile(tempFilePath);

            // Assert
            Assert.True(result.IsError);
            var error = result.GetErrorOrThrow();
            Assert.Equal("Unknown command type: INVALID", error.Message);
            Assert.Equal(2, error.LineNumber);
        }
        finally
        {
            // Cleanup
            CommandParserTestHelper.DeleteTempFile(tempFilePath);
        }
    }

    [Fact]
    public void ParseCommandFile_ThrowsExceptionDuringRead_ReturnsError()
    {
        // This test simulates an exception during file reading
        // by creating a file and then making it inaccessible

        // Arrange
        var commandContent = "FLH/5\nINC:123";
        var tempFilePath = CreateTempFileWithContent(commandContent);

        try
        {
            // Make the file inaccessible by opening it with exclusive access
            using (var fileStream = new FileStream(tempFilePath, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
            {
                // Act
                var result = CommandParser.ParseCommandFile(tempFilePath);

                // Assert
                Assert.True(result.IsError);
                var error = result.GetErrorOrThrow();
                Assert.Contains("Failed to read command file", error.Message);
                Assert.Equal(0, error.LineNumber);
            }
        }
        finally
        {
            // Cleanup
            CommandParserTestHelper.DeleteTempFile(tempFilePath);
        }
    }

    [Fact]
    public void ParseCommandFile_WithMockDataFile_ReturnsCorrectCommands()
    {
        // Arrange
        var testCommands = new List<(CommandParser.CommandType Type, int Key)>
        {
            (CommandParser.CommandType.Insert, 101),
            (CommandParser.CommandType.Search, 102),
            (CommandParser.CommandType.Insert, 103)
        };
        var mockFilePath = CreateTempFileWithContent(
            CommandParserTestHelper.GenerateTestCommandContent(testCommands, 10));

        try
        {
            // Act
            var result = CommandParser.ParseCommandFile(mockFilePath);

            // Assert
            Assert.True(result.IsSuccess);
            var (header, commands) = result.GetValueOrThrow();
            Assert.Equal(10, header.MaxChildren);
            Assert.Equal(3, commands.Count);
            Assert.Equal(101, commands[0].Key);
            Assert.Equal(102, commands[1].Key);
            Assert.Equal(103, commands[2].Key);
            Assert.Equal(CommandParser.CommandType.Insert, commands[0].Type);
            Assert.Equal(CommandParser.CommandType.Search, commands[1].Type);
            Assert.Equal(CommandParser.CommandType.Insert, commands[2].Type);
        }
        finally
        {
            CommandParserTestHelper.DeleteTempFile(mockFilePath);
        }
    }

    [Fact]
    public void ParseCommandFile_WithEmptyFile_ReturnsError()
    {
        // Arrange
        var mockFilePath = CreateTempFileWithContent(""); // Empty file, no header

        try
        {
            // Act
            var result = CommandParser.ParseCommandFile(mockFilePath);

            // Assert
            Assert.True(result.IsError);
            var error = result.GetErrorOrThrow();
            Assert.Equal("Command input is empty", error.Message);
        }
        finally
        {
            CommandParserTestHelper.DeleteTempFile(mockFilePath);
        }
    }

    [Fact]
    public void ParseCommandFile_WithInvalidHeaderFile_ReturnsError()
    {
        // Arrange
        var mockFilePath = CreateTempFileWithContent(
            "InvalidHeader\n" +
            "INC:123");

        try
        {
            // Act
            var result = CommandParser.ParseCommandFile(mockFilePath);

            // Assert
            Assert.True(result.IsError);
            var error = result.GetErrorOrThrow();
            Assert.Equal("Header must start with 'FLH/'", error.Message);
        }
        finally
        {
            CommandParserTestHelper.DeleteTempFile(mockFilePath);
        }
    }

    [Fact]
    public void ParseCommandFile_WithInvalidMaxChildrenValue_ReturnsError()
    {
        // Arrange
        var mockFilePath = CreateTempFileWithContent(
            "FLH/1\n" + // 1 is an invalid value, must be > 1
            "INC:123");

        try
        {
            // Act
            var result = CommandParser.ParseCommandFile(mockFilePath);

            // Assert
            Assert.True(result.IsError);
            var error = result.GetErrorOrThrow();
            Assert.Equal("MaxChildren must be greater than 1", error.Message);
        }
        finally
        {
            CommandParserTestHelper.DeleteTempFile(mockFilePath);
        }
    }

    [Fact]
    public void ParseCommandFile_WithLargeFile_HandlesCorrectly()
    {
        // Arrange
        var largeDataset = new List<(CommandParser.CommandType Type, int Key)>();
        for (int i = 1; i <= 1000; i++)
        {
            largeDataset.Add((i % 2 == 0 ? CommandParser.CommandType.Insert : CommandParser.CommandType.Search, i));
        }
        
        var mockFilePath = CreateTempFileWithContent(
            CommandParserTestHelper.GenerateTestCommandContent(largeDataset, 10));

        try
        {
            // Act
            var result = CommandParser.ParseCommandFile(mockFilePath);

            // Assert
            Assert.True(result.IsSuccess);
            var (header, commands) = result.GetValueOrThrow();
            Assert.Equal(10, header.MaxChildren);
            Assert.Equal(1000, commands.Count);
            
            // Check a few random commands
            Assert.Equal(CommandParser.CommandType.Search, commands[0].Type);
            Assert.Equal(1, commands[0].Key);
            Assert.Equal(CommandParser.CommandType.Insert, commands[999].Type);
            Assert.Equal(1000, commands[999].Key);
        }
        finally
        {
            CommandParserTestHelper.DeleteTempFile(mockFilePath);
        }
    }
}