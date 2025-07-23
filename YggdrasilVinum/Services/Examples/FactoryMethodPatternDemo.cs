using System.Text;
using Microsoft.Extensions.DependencyInjection;
using YggdrasilVinum.Models;
using YggdrasilVinum.Parsers;
using YggdrasilVinum.Services.CommandProcessing;
using YggdrasilVinum.Services.Factories;

namespace YggdrasilVinum.Services.Examples;

/// <summary>
///     Concrete implementation of the Factory Method pattern for command processors
/// </summary>
public class StandardCommandProcessorFactory : AbstractCommandProcessorFactory
{
    private readonly IServiceProvider _serviceProvider;

    public StandardCommandProcessorFactory(IServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
    }

    /// <summary>
    ///     Creates a command processor for the specified command type
    /// </summary>
    /// <param name="type">The type of command to create a processor for</param>
    /// <returns>An instance of ICommandProcessor appropriate for the command type</returns>
    /// <exception cref="NotSupportedException">Thrown when the command type is not supported</exception>
    public override ICommandProcessor CreateProcessor(CommandParser.CommandType type)
    {
        return type switch
        {
            CommandParser.CommandType.Insert => _serviceProvider.GetRequiredService<InsertCommandProcessor>(),
            CommandParser.CommandType.Search => _serviceProvider.GetRequiredService<SearchCommandProcessor>(),
            _ => throw new NotSupportedException($"Command type '{type}' is not supported.")
        };
    }
}

/// <summary>
///     Alternative factory implementation that could be used for different scenarios
///     (e.g., testing, different environments, etc.)
/// </summary>
public class TestCommandProcessorFactory : AbstractCommandProcessorFactory
{
    private readonly Database _database;
    private readonly HarvestYearSearchProcessor _harvestYearSearchProcessor;

    public TestCommandProcessorFactory(
        Database database,
        HarvestYearSearchProcessor harvestYearSearchProcessor)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
        _harvestYearSearchProcessor = harvestYearSearchProcessor ??
                                      throw new ArgumentNullException(nameof(harvestYearSearchProcessor));
    }

    public override ICommandProcessor CreateProcessor(CommandParser.CommandType type)
    {
        return type switch
        {
            CommandParser.CommandType.Insert => new InsertCommandProcessor(_database, _harvestYearSearchProcessor),
            CommandParser.CommandType.Search => new SearchCommandProcessor(_database),
            _ => throw new NotSupportedException($"Command type '{type}' is not supported.")
        };
    }
}

/// <summary>
///     Factory Method Pattern demonstration and usage example
/// </summary>
public static class FactoryMethodPatternDemo
{
    /// <summary>
    ///     Demonstrates how to use the Factory Method pattern for command processing
    /// </summary>
    /// <param name="factory">The factory to use for creating processors</param>
    /// <param name="command">The command to process</param>
    /// <param name="outputContent">StringBuilder to append output to</param>
    /// <returns>Result of the command execution</returns>
    public static async Task<Result<Unit, CommandProcessorError>> ProcessCommandUsingFactory(
        AbstractCommandProcessorFactory factory,
        CommandParser.Command command,
        StringBuilder outputContent)
    {
        try
        {
            var processor = factory.CreateProcessor(command.Type);
            return await processor.ExecuteAsync(command, outputContent);
        }
        catch (NotSupportedException ex)
        {
            return Result<Unit, CommandProcessorError>.Error(
                new CommandProcessorError($"Factory method failed: {ex.Message}")
            );
        }
    }

    /// <summary>
    ///     Example of processing multiple commands using the factory pattern
    /// </summary>
    /// <param name="factory">The factory to use for creating processors</param>
    /// <param name="commands">List of commands to process</param>
    /// <param name="outputContent">StringBuilder to append output to</param>
    /// <returns>Result indicating success or the first error encountered</returns>
    public static async Task<Result<Unit, CommandProcessorError>> ProcessCommandsUsingFactory(
        AbstractCommandProcessorFactory factory,
        IEnumerable<CommandParser.Command> commands,
        StringBuilder outputContent)
    {
        foreach (var command in commands)
        {
            var result = await ProcessCommandUsingFactory(factory, command, outputContent);
            if (result.IsError) return result;
        }

        return Result<Unit, CommandProcessorError>.Success(new Unit());
    }
}
