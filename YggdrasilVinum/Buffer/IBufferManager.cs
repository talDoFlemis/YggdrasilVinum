using YggdrasilVinum.Models;

namespace YggdrasilVinum.Buffer;

public interface IBufferManager
{
    Task<Result<Unit, BufferError>> InitializeAsync();

    // Page operations
    Task<Result<Page, BufferError>> LoadPageAsync(ulong pageId);
    Task<Result<Unit, BufferError>> PutPageAsync(Page page);
    Task<Result<Unit, BufferError>> FlushPageAsync(ulong pageId);

    Task<Result<Unit, BufferError>> SetPageDirty(ulong pageId);

    // Frame operations
    Task<Result<Unit, BufferError>> FlushAllFramesAsync();
}

public readonly struct BufferError
{
    public string Message { get; }

    public BufferError(string message)
    {
        Message = message;
    }
}
