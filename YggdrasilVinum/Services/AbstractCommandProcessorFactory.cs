using YggdrasilVinum.Parsers;

namespace YggdrasilVinum.Services;

/// <summary>
/// Abstract factory for creating command processors using the Factory Method pattern
/// </summary>
public abstract class AbstractCommandProcessorFactory
{
    /// <summary>
    /// Factory method to create a command processor for a specific command type
    /// </summary>
    /// <param name="type">The type of command to create a processor for</param>
    /// <returns>An instance of ICommandProcessor appropriate for the command type</returns>
    public abstract ICommandProcessor CreateProcessor(CommandParser.CommandType type);
}