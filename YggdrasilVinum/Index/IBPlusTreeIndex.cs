using YggdrasilVinum.Models;

namespace YggdrasilVinum.Index;

public interface IBPlusTreeIndex<in TKey>
    where TKey : IComparable<TKey>
{
    Task<Result<Unit, BPlusTreeError>> InitializeAsync();

    Task<Result<List<ulong>, BPlusTreeError>> SearchAsync(TKey key);

    Task<Result<Unit, BPlusTreeError>> InsertAsync(TKey key, ulong pageId);

    Task<Result<int, BPlusTreeError>> HeightAsync();

    Task<Result<Unit, BPlusTreeError>> PrintTreeAsync();
}

public readonly struct BPlusTreeError(string message)
{
    public string Message { get; } = message;
}
