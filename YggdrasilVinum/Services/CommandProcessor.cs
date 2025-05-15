using Serilog;
using YggdrasilVinum.Index;
using YggdrasilVinum.Models;
using YggdrasilVinum.Parsers;

namespace YggdrasilVinum.Services;

/// <summary>
///     Handles processing of commands for the B+ tree database
/// </summary>
public class CommandProcessor
{
    private readonly ILogger _logger;

    public CommandProcessor()
    {
        _logger = Log.ForContext<CommandProcessor>();
    }

    /// <summary>
    ///     Processes a single command
    /// </summary>
    public Task ProcessCommand(CommandParser.Command command, BPlusTreeIndex<int, WineRecord> bPlusTree,
        List<WineRecord> wines)
    {
        _logger.Debug("Processing command: {CommandType} with key {Key}", command.Type, command.Key);

        switch (command.Type)
        {
            case CommandParser.CommandType.Insert:
                var matchingWine = wines.FirstOrDefault(w => w.WineId == command.Key);
                if (matchingWine.WineId == command.Key)
                {
                    bPlusTree.Insert(matchingWine.WineId, matchingWine);
                    _logger.Information("Inserted wine with ID {WineId}: {Label}", command.Key, matchingWine.Label);
                }
                else
                {
                    _logger.Warning("No wine found with ID {WineId}", command.Key);
                }

                break;

            case CommandParser.CommandType.Search:
                var results = bPlusTree.Search(command.Key);
                if (results.Count > 0)
                {
                    _logger.Information("Found {Count} matching wines with ID {WineId}", results.Count, command.Key);

                    foreach (var result in results)
                        _logger.Debug("Found wine: {WineId}, {Label}, {HarvestYear}, {Type}",
                            result.WineId, result.Label, result.HarvestYear, result.Type);
                }
                else
                {
                    _logger.Warning("No wines found with ID {WineId}", command.Key);
                }

                break;
        }

        return Task.CompletedTask;
    }

    /// <summary>
    ///     Processes commands from a file
    /// </summary>
    public async Task ProcessCommandsFromFile(string filePath, BPlusTreeIndex<int, WineRecord> bPlusTree,
        List<WineRecord> wines)
    {
        _logger.Information("Processing commands from file: {FilePath}", filePath);

        if (!File.Exists(filePath))
        {
            _logger.Error("Commands file not found: {FilePath}", filePath);
            return;
        }

        var commandsResult = CommandParser.ParseCommandFile(filePath);

        if (commandsResult.IsError)
        {
            _logger.Error("Failed to parse commands from file: {FilePath}", filePath);
            return;
        }

        var (header, commands) = commandsResult.GetValueOrThrow();
        _logger.Information("Successfully parsed {CommandCount} commands with max children: {MaxChildren}",
            commands.Count, header.MaxChildren);

        // Process each command
        foreach (var command in commands) await ProcessCommand(command, bPlusTree, wines);
    }
}
