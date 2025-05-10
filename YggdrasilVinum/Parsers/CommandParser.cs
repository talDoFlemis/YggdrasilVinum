using Serilog;
using YggdrasilVinum.Models;

namespace YggdrasilVinum.Parsers;

public static class CommandParser
{
    public readonly struct Command
    {
        public readonly CommandType Type;
        public readonly int Key;

        public Command(CommandType type, int key)
        {
            Type = type;
            Key = key;
        }
    }

    public enum CommandType
    {
        Insert,
        Search
    }

    public readonly struct CommandFileHeader
    {
        public readonly int MaxChildren;

        public CommandFileHeader(int maxChildren)
        {
            if (maxChildren <= 2)
                throw new ArgumentException("MaxChildren must be greater than 2", nameof(maxChildren));
            MaxChildren = maxChildren;
        }
    }

    public readonly struct ParseError
    {
        public readonly string Message;
        public readonly int LineNumber;

        public ParseError(string message, int lineNumber)
        {
            Message = message;
            LineNumber = lineNumber;
        }

        public override string ToString() => $"Line {LineNumber}: {Message}";
    }

    public static Result<(CommandFileHeader Header, List<Command> Commands), ParseError> ParseCommandFile(
        string filePath)
    {
        Log.Information("Parsing command file: {FilePath}", filePath);

        if (!File.Exists(filePath))
        {
            Log.Warning("Command file not found: {FilePath}", filePath);
            return Result<(CommandFileHeader, List<Command>), ParseError>.Error(
                new ParseError($"Command file not found: {filePath}", 0));
        }

        try
        {
            using var fileStream = new FileStream(
                filePath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                bufferSize: 4096, // Explicit buffer size
                useAsync: false);
            using var reader = new StreamReader(fileStream);
            var result = ParseCommandsFromReader(reader);
            Log.Debug("Command file parsing completed: {FilePath}", filePath);
            return result;
        }
        catch (IOException ex)
        {
            Log.Error(ex, "Failed to read command file: {FilePath}", filePath);
            return Result<(CommandFileHeader, List<Command>), ParseError>.Error(
                new ParseError($"Failed to read command file: {ex.Message}", 0));
        }
    }

    public static Result<(CommandFileHeader Header, List<Command> Commands), ParseError> ParseCommandsFromString(
        string content)
    {
        Log.Information("Parsing commands from string input");
        Log.Debug("Command string length: {Length} characters", content?.Length ?? 0);

        if (string.IsNullOrEmpty(content))
        {
            Log.Warning("Command string is empty");
            return Result<(CommandFileHeader, List<Command>), ParseError>.Error(
                new ParseError("Command string is empty", 0));
        }

        using var reader = new StringReader(content);
        var result = ParseCommandsFromReader(reader);
        Log.Debug("String parsing completed");
        return result;
    }

    public static Result<(CommandFileHeader Header, List<Command> Commands), ParseError> ParseCommandsFromReader(
        TextReader reader)
    {
        Log.Information("Parsing commands from TextReader");
        try
        {
            // Read the first line for header
            string headerLine = reader.ReadLine();
            if (headerLine == null)
            {
                Log.Warning("Command input is empty");
                return Result<(CommandFileHeader, List<Command>), ParseError>.Error(
                    new ParseError("Command input is empty", 0));
            }

            Log.Debug("Parsing header line: {HeaderLine}", headerLine);
            var headerResult = ParseHeader(headerLine, 0);
            if (headerResult.IsError)
            {
                Log.Warning("Header parsing failed: {Error}", headerResult.GetErrorOrThrow().Message);
                return Result<(CommandFileHeader, List<Command>), ParseError>.Error(
                    headerResult.GetErrorOrThrow());
            }

            var commandFileHeader = headerResult.GetValueOrThrow();
            Log.Debug("Header parsed successfully, MaxChildren: {MaxChildren}", commandFileHeader.MaxChildren);
            var commands = new List<Command>(capacity: 32); // Pre-allocate with reasonable initial capacity
            string line;
            int lineNumber = 1;

            // Process commands line by line
            while ((line = reader.ReadLine()) != null)
            {
                if (string.IsNullOrWhiteSpace(line))
                    continue;

                Log.Debug("Parsing command line {LineNumber}: {Line}", lineNumber, line);
                var commandResult = ParseCommand(line, lineNumber);
                if (commandResult.IsError)
                {
                    Log.Warning("Command parsing failed at line {LineNumber}: {Error}",
                        lineNumber, commandResult.GetErrorOrThrow().Message);
                    return Result<(CommandFileHeader, List<Command>), ParseError>.Error(
                        commandResult.GetErrorOrThrow());
                }

                commands.Add(commandResult.GetValueOrThrow());
                lineNumber++;
            }

            Log.Information("Successfully parsed {CommandCount} commands", commands.Count);
            return Result<(CommandFileHeader, List<Command>), ParseError>.Success(
                (commandFileHeader, commands));
        }
        catch (Exception ex)
        {
            Log.Error(ex, "Failed to parse commands");
            return Result<(CommandFileHeader, List<Command>), ParseError>.Error(
                new ParseError($"Failed to parse commands: {ex.Message}", 0));
        }
    }

    private static Result<CommandFileHeader, ParseError> ParseHeader(string line, int lineNumber)
    {
        // Check for null or empty
        if (string.IsNullOrEmpty(line))
        {
            Log.Warning("Header line is empty at line {LineNumber}", lineNumber);
            return Result<CommandFileHeader, ParseError>.Error(
                new ParseError("Header line is empty", lineNumber));
        }

        // Verify correct format
        if (!line.StartsWith("FLH/"))
            return Result<CommandFileHeader, ParseError>.Error(
                new ParseError("Header must start with 'FLH/'", lineNumber));

        // Parse value using ReadOnlySpan to avoid allocations
        if (!int.TryParse(line.AsSpan(4), out int maxChildren))
            return Result<CommandFileHeader, ParseError>.Error(
                new ParseError("Invalid max children value in header", lineNumber));

        try
        {
            Log.Debug("Creating CommandFileHeader with MaxChildren: {MaxChildren}", maxChildren);
            return Result<CommandFileHeader, ParseError>.Success(new CommandFileHeader(maxChildren));
        }
        catch (ArgumentException ex)
        {
            Log.Warning(ex, "Invalid MaxChildren value: {MaxChildren} at line {LineNumber}", maxChildren, lineNumber);
            return Result<CommandFileHeader, ParseError>.Error(
                new ParseError(ex.Message, lineNumber));
        }
    }

    private static Result<Command, ParseError> ParseCommand(string line, int lineNumber)
    {
        if (string.IsNullOrEmpty(line))
        {
            Log.Warning("Command line is empty at line {LineNumber}", lineNumber);
            return Result<Command, ParseError>.Error(
                new ParseError("Command line is empty", lineNumber));
        }

        try
        {
            int colonIndex = line.IndexOf(':');
            if (colonIndex == -1)
                return Result<Command, ParseError>.Error(
                    new ParseError("Command missing colon separator", lineNumber));

            // Use ReadOnlySpan to avoid allocations
            ReadOnlySpan<char> operation = line.AsSpan(0, colonIndex);
            if (!int.TryParse(line.AsSpan(colonIndex + 1), out int key))
                return Result<Command, ParseError>.Error(
                    new ParseError("Invalid key value in command", lineNumber));

            CommandType commandType;
            if (operation.SequenceEqual("INC".AsSpan()))
                commandType = CommandType.Insert;
            else if (operation.SequenceEqual("BUS=".AsSpan()))
                commandType = CommandType.Search;
            else
                return Result<Command, ParseError>.Error(
                    new ParseError($"Unknown command type: {line.Substring(0, colonIndex)}", lineNumber));

            Log.Debug("Command parsed successfully: Type={CommandType}, Key={Key}", commandType, key);
            return Result<Command, ParseError>.Success(new Command(commandType, key));
        }
        catch (Exception ex)
        {
            Log.Warning(ex, "Failed to parse command at line {LineNumber}: {Line}", lineNumber, line);
            return Result<Command, ParseError>.Error(
                new ParseError($"Failed to parse command: {ex.Message}", lineNumber));
        }
    }
}