namespace YggdrasilVinum.Models;

public class Page
{
    public ulong PageId { get; set; }
    public WineRecord[] Content { get; set; }
    public bool IsDirty { get; set; }
    public DateTime LastAccessed { get; set; }

    public Page(ulong pageId, WineRecord[] content)
    {
        PageId = pageId;
        Content = content;
        IsDirty = false;
        LastAccessed = DateTime.UtcNow;
    }
}
