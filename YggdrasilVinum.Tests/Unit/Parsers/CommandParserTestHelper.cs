using YggdrasilVinum.Parsers;

namespace YggdrasilVinum.Tests.Unit.Parsers;

public static class CommandParserTestHelper
{
    public static readonly string ValidHeaderLine = "FLH/10";

    public static readonly List<(CommandParser.CommandType Type, int Key)> TestCommandData = new()
    {
        (CommandParser.CommandType.Insert, 101),
        (CommandParser.CommandType.Search, 202),
        (CommandParser.CommandType.Insert, 303),
        (CommandParser.CommandType.Search, 404),
        (CommandParser.CommandType.Insert, 505)
    };

    public static string GenerateTestCommandContent(IEnumerable<(CommandParser.CommandType Type, int Key)> entries,
        int maxChildren = 10)
    {
        using var stringWriter = new StringWriter();
        stringWriter.WriteLine($"FLH/{maxChildren}");

        foreach (var entry in entries)
        {
            var typeStr = entry.Type switch
            {
                CommandParser.CommandType.Insert => "INC",
                CommandParser.CommandType.Search => "BUS=",
                _ => throw new ArgumentOutOfRangeException()
            };

            stringWriter.WriteLine($"{typeStr}:{entry.Key}");
        }

        return stringWriter.ToString();
    }

    public static List<CommandParser.Command> CreateCommands(
        IEnumerable<(CommandParser.CommandType Type, int Key)> entries)
    {
        var commands = new List<CommandParser.Command>();
        foreach (var entry in entries)
            commands.Add(new CommandParser.Command(entry.Type, entry.Key));
        return commands;
    }

    public static string GenerateInvalidTestCommandContent()
    {
        return
            $"{ValidHeaderLine}\n" +
            "INC:101\n" +
            "INVALID:202\n" +
            "INC:not_a_number\n" +
            "MissingColon\n" +
            "BUS=:\n" +
            "INC:303:extra\n";
    }

    public static string CreateTempCommandFile(string content)
    {
        var tempFilePath = Path.GetTempFileName();
        File.WriteAllText(tempFilePath, content);
        return tempFilePath;
    }

    public static void DeleteTempFile(string filePath)
    {
        if (File.Exists(filePath)) File.Delete(filePath);
    }
}
