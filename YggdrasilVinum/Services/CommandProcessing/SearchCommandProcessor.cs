using System.Text;
using Serilog;
using YggdrasilVinum.Models;
using YggdrasilVinum.Parsers;

namespace YggdrasilVinum.Services.CommandProcessing;

/// <summary>
///     Command processor strategy for search operations
/// </summary>
public class SearchCommandProcessor : ICommandProcessor
{
    private readonly Database _database;
    private readonly ILogger _logger;

    public SearchCommandProcessor(Database database)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
        _logger = Log.ForContext<SearchCommandProcessor>();
    }

    public async Task<Result<Unit, CommandProcessorError>> ExecuteAsync(CommandParser.Command command,
        StringBuilder outputContent)
    {
        _logger.Information("Processing search command with key: {CommandKey}", command.Key);

        var searchResult = await _database.SearchAsync(command.Key);
        if (searchResult.IsError)
        {
            var error = searchResult.GetErrorOrThrow();
            _logger.Error("Error searching for wine with ID {WineId}: {ErrorMessage}",
                command.Key, error.Message);
            return Result<Unit, CommandProcessorError>.Error(new CommandProcessorError(error.Message));
        }

        var winesFound = searchResult.GetValueOrThrow();

        // Write the search result to the output content
        outputContent.AppendLine($"BUS=:{command.Key}/{winesFound.Length}");

        foreach (var wine in winesFound) _logger.Debug("Wine ID: {WineId}, Name: {WineName}", wine.WineId, wine.Label);

        _logger.Information("Found {WineCount} wines with harvest year {HarvestYear}",
            winesFound.Length, command.Key);

        return Result<Unit, CommandProcessorError>.Success(Unit.Value);
    }
}
