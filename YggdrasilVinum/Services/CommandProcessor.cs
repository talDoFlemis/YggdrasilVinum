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
    public async Task<Result<Unit, BPlusTreeError>> ProcessCommandAsync(CommandParser.Command command,
        // TODO: fix this type
        IBPlusTreeIndex<int, int> bPlusTree,
        List<WineRecord> wines)
    {
        _logger.Debug("Processing command: {CommandType} with key {Key}", command.Type, command.Key);

        switch (command.Type)
        {
            case CommandParser.CommandType.Insert:
                var matchingWine = wines.FirstOrDefault(w => w.WineId == command.Key);
                if (matchingWine.WineId == command.Key)
                {
                    var result = await bPlusTree.InsertAsync(matchingWine.WineId, matchingWine.WineId);
                    if (result.IsError)
                    {
                        _logger.Error("Failed to insert wine with ID {WineId}: {ErrorMessage}", command.Key,
                            result.GetErrorOrThrow().Message);
                        return result;
                    }

                    _logger.Information("Inserted wine with ID {WineId}: {Label}", command.Key, matchingWine.Label);
                }
                else
                {
                    _logger.Warning("No wine found with ID {WineId}", command.Key);
                }

                break;

            case CommandParser.CommandType.Search:
                var searchResult = await bPlusTree.SearchAsync(command.Key);
                if (searchResult.IsError)
                {
                    _logger.Error("Error searching for key {Key}: {ErrorMessage}", command.Key,
                        searchResult.GetErrorOrThrow().Message);
                    return Result<Unit, BPlusTreeError>.Error(searchResult.GetErrorOrThrow());
                }

                var results = searchResult.GetValueOrThrow();
                if (results.Count > 0)
                {
                    _logger.Information("Found {Count} matching wines with ID {WineId}", results.Count, command.Key);

                    foreach (var pageId in results)
                    {
                        // Page ID now represents a reference to where the wine is stored
                        var wineId = pageId; // For simplicity, assuming pageId correlates to wineId
                        var wine = wines.FirstOrDefault(w => w.WineId == wineId);
                        if (wine != null)
                            _logger.Debug("Found wine: {WineId}, {Label}, {HarvestYear}, {Type}",
                                wine.WineId, wine.Label, wine.HarvestYear, wine.Type);
                        else
                            _logger.Warning("Found pageId {PageId} but no matching wine", pageId);
                    }
                }
                else
                {
                    _logger.Warning("No wines found with ID {WineId}", command.Key);
                }

                break;
        }

        return Result<Unit, BPlusTreeError>.Success(new Unit());
    }

    /// <summary>
    ///     Processes commands from a file
    /// </summary>
    public async Task<Result<Unit, BPlusTreeError>> ProcessCommandsFromFileAsync(string filePath,
        IBPlusTreeIndex<int, int> bPlusTree,
        List<WineRecord> wines)
    {
        _logger.Information("Processing commands from file: {FilePath}", filePath);

        if (!File.Exists(filePath))
        {
            _logger.Error("Commands file not found: {FilePath}", filePath);
            return Result<Unit, BPlusTreeError>.Error(new BPlusTreeError($"Commands file not found: {filePath}"));
        }

        var commandsResult = CommandParser.ParseCommandFile(filePath);

        if (commandsResult.IsError)
        {
            _logger.Error("Failed to parse commands from file: {FilePath}", filePath);
            return Result<Unit, BPlusTreeError>.Error(
                new BPlusTreeError($"Failed to parse commands from file: {filePath}"));
        }

        var (header, commands) = commandsResult.GetValueOrThrow();
        _logger.Information("Successfully parsed {CommandCount} commands with max children: {MaxChildren}",
            commands.Count, header.MaxChildren);

        // Process each command
        foreach (var command in commands)
        {
            var result = await ProcessCommandAsync(command, bPlusTree, wines);
            if (result.IsError)
                return result;
        }

        return Result<Unit, BPlusTreeError>.Success(new Unit());
    }
}
