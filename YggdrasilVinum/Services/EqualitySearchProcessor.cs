using YggdrasilVinum.Models;

namespace YggdrasilVinum.Services;

public class EqualitySearchProcessor
{
    public async Task<Result<WineRecord[], SearchError>> ExecuteAsync(
    )
    {
        throw new NotImplementedException();
    }
}

public readonly struct SearchError
{
    public SearchError(string message)
    {
        Message = message;
    }

    public string Message { get; }
}
