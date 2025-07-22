using YggdrasilVinum.Parsers;

namespace YggdrasilVinum.Services;

/// <summary>
///     Factory for creating command processor strategies
/// </summary>
public class CommandProcessorFactory
{
    private readonly Database _database;
    private readonly HarvestYearSearchProcessor _harvestYearSearchProcessor;

    public CommandProcessorFactory(
        Database database,
        HarvestYearSearchProcessor harvestYearSearchProcessor)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
        _harvestYearSearchProcessor = harvestYearSearchProcessor ??
                                      throw new ArgumentNullException(nameof(harvestYearSearchProcessor));
    }

    /// <summary>
    ///     Creates command processors mapped by command type
    /// </summary>
    /// <returns>Dictionary of command processors mapped by command type</returns>
    public Dictionary<CommandParser.CommandType, ICommandProcessor> CreateCommandProcessors()
    {
        return new Dictionary<CommandParser.CommandType, ICommandProcessor>
        {
            {
                CommandParser.CommandType.Insert, new InsertCommandProcessor(_database, _harvestYearSearchProcessor)
            },
            { CommandParser.CommandType.Search, new SearchCommandProcessor(_database) }
        };
    }
}
