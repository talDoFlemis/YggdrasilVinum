using System.Text;
using Serilog;
using YggdrasilVinum.Models;
using YggdrasilVinum.Parsers;

namespace YggdrasilVinum.Services.CommandProcessing;

/// <summary>
///     Command processor strategy for insert operations
/// </summary>
public class InsertCommandProcessor : ICommandProcessor
{
    private readonly Database _database;
    private readonly HarvestYearSearchProcessor _harvestYearSearchProcessor;
    private readonly ILogger _logger;

    public InsertCommandProcessor(
        Database database,
        HarvestYearSearchProcessor harvestYearSearchProcessor)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
        _harvestYearSearchProcessor = harvestYearSearchProcessor ??
                                      throw new ArgumentNullException(nameof(harvestYearSearchProcessor));
        _logger = Log.ForContext<InsertCommandProcessor>();
    }

    public async Task<Result<Unit, CommandProcessorError>> ExecuteAsync(CommandParser.Command command,
        StringBuilder outputContent)
    {
        _logger.Information("Processing insert command with key: {CommandKey}", command.Key);

        var winesResult = await _harvestYearSearchProcessor.SearchByHarvestYearAsync(command.Key);
        if (winesResult.IsError)
        {
            var error = winesResult.GetErrorOrThrow();
            _logger.Error("Error searching for wine with harvest year {HarvestYear}: {ErrorMessage}",
                command.Key, error.Message);
            return Result<Unit, CommandProcessorError>.Error(
                new CommandProcessorError(error.Message)
            );
        }

        var wines = winesResult.GetValueOrThrow();

        foreach (var wine in wines)
        {
            var insertResult = await _database.InsertAsync(wine);
            if (insertResult.IsError)
            {
                var error = insertResult.GetErrorOrThrow();
                _logger.Error("Error inserting wine with ID {WineId}: {ErrorMessage}",
                    wine.WineId, error);
                return Result<Unit, CommandProcessorError>.Error(new CommandProcessorError(error.ToString()));
            }

            _logger.Debug("Inserted wine with ID {WineId}", wine.WineId);
        }

        outputContent.AppendLine($"INC:{command.Key}/{wines.Length}");

        _logger.Information("Inserted {WineCount} wines with harvest year {HarvestYear}",
            wines.Length, command.Key);

        return Result<Unit, CommandProcessorError>.Success(new Unit());
    }
}
