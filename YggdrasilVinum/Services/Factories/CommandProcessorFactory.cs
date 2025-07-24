using Microsoft.Extensions.DependencyInjection;
using YggdrasilVinum.Parsers;
using YggdrasilVinum.Services.CommandProcessing;

namespace YggdrasilVinum.Services.Factories;

/// <summary>
///     Factory for creating command processor strategies
/// </summary>
public class CommandProcessorFactory(IServiceProvider serviceProvider) : AbstractCommandProcessorFactory
{
    private readonly IServiceProvider _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));

    public override ICommandProcessor CreateProcessor(CommandParser.CommandType type) => type switch
    {
        CommandParser.CommandType.Insert => _serviceProvider.GetRequiredService<InsertCommandProcessor>(),
        CommandParser.CommandType.Search => _serviceProvider.GetRequiredService<SearchCommandProcessor>(),
        _ => throw new NotSupportedException($"Command type '{type}' is not supported.")
    };
}
