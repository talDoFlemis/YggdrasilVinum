using System.Text;
using YggdrasilVinum.Models;
using YggdrasilVinum.Parsers;

namespace YggdrasilVinum.Services;

/// <summary>
///     Interface for command processing strategies
/// </summary>
public interface ICommandProcessor
{
    /// <summary>
    ///     Executes a command and appends the result to the output content
    /// </summary>
    /// <param name="command">The command to execute</param>
    /// <param name="outputContent">StringBuilder to append the output to</param>
    /// <returns>Result indicating success or failure</returns>
    Task<Result<Unit, CommandProcessorError>> ExecuteAsync(CommandParser.Command command, StringBuilder outputContent);
}

public readonly struct CommandProcessorError(string message)
{
    private string Message { get; } = message ?? throw new ArgumentNullException(nameof(message));

    public override string ToString()
    {
        return Message;
    }
}
