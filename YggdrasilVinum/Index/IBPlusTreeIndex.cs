using YggdrasilVinum.Models;

namespace YggdrasilVinum.Index;

public interface IBPlusTreeIndex<in TKey, TValue>
    where TKey : IComparable<TKey>
    where TValue : IParsable<TValue>
{
    Task<Result<Unit, BPlusTreeError>> InitializeAsync();

    Task<Result<List<TValue>, BPlusTreeError>> SearchAsync(TKey key);

    Task<Result<Unit, BPlusTreeError>> InsertAsync(TKey key, TValue pageId);

    Task<Result<int, BPlusTreeError>> HeightAsync();

    Task<Result<Unit, BPlusTreeError>> PrintTreeAsync();
}

public readonly struct BPlusTreeError(string message)
{
    public string Message { get; } = message;
}
