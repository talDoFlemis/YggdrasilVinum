using YggdrasilVinum.Models;

namespace YggdrasilVinum.Storage;

public interface IFileManager
{
    Task<Result<Unit, StoreError>> InitializeAsync();
    Task<Result<Page, StoreError>> ReadPageAsync(ulong pageId);
    Task<Result<Unit, StoreError>> WritePageAsync(Page page);
    Task<Result<bool, StoreError>> PageExistsAsync(ulong pageId);
    Task<Result<Page, StoreError>> AllocateNewPageAsync();
    Task<Result<Unit, StoreError>> FlushAsync();
}

public readonly struct StoreError
{
    public string Message { get; }

    public StoreError(string message)
    {
        Message = message;
    }
}
