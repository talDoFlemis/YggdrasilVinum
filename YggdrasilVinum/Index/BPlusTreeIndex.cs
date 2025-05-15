using System.Diagnostics;
using System.Text;
using Serilog;
using YggdrasilVinum.Buffer;
using YggdrasilVinum.Models;

namespace YggdrasilVinum.Index;

public class BPlusTreeIndex<TKey> : IBPlusTreeIndex<TKey>
    where TKey : IComparable<TKey>
{
    private readonly int _degree;
    private readonly string _indexFilePath;
    private readonly ILogger _logger = Log.ForContext<BPlusTreeIndex<TKey>>();
    private int _height;
    private int _nextId;
    private int _rootId;

    public BPlusTreeIndex(string indexFilePath, int degree)
    {
        if (string.IsNullOrEmpty(indexFilePath))
            throw new ArgumentNullException(nameof(indexFilePath), "Index file path cannot be null or empty.");
        if (degree < 2) throw new ArgumentOutOfRangeException(nameof(degree), "Degree must be at least 2.");

        _indexFilePath = indexFilePath;
        _degree = degree;

        if (!File.Exists(indexFilePath))
        {
            _rootId = 0;
            _nextId = 1;
            _height = 0;

            var rootNode = new Node
            {
                Id = _rootId,
                IsLeaf = true,
                Keys = [],
                Children = [],
                PageIds = [],
                Next = -1
            };

            SaveNodeAsync(rootNode).GetAwaiter().GetResult();
            SaveMetadataAsync().GetAwaiter().GetResult();
        }
        else
        {
            LoadMetadataAsync().GetAwaiter().GetResult();
        }
    }

    public async Task<Result<Unit, BPlusTreeError>> InitializeAsync()
    {
        _logger.Debug("Initializing B+ Tree");
        Debug.Assert(_degree > 0);
        Debug.Assert(!string.IsNullOrEmpty(_indexFilePath));

        _logger.Information(
            "B+ Tree initialized with degree {degree} and '{indexPath}' file path",
            _degree, _indexFilePath);
        return await Task.FromResult(Result<Unit, BPlusTreeError>.Success(Unit.Value));
    }

    public async Task<Result<List<ulong>, BPlusTreeError>> SearchAsync(TKey key)
    {
        try
        {
            var rootNode = await LoadNodeAsync(_rootId);
            List<ulong> result = [];

            var current = rootNode;

            while (!current.IsLeaf)
            {
                var i = current.Keys.FindIndex(k => key.CompareTo(k) <= 0);
                i = i == -1 ? current.Keys.Count : i;
                current = await LoadNodeAsync(current.Children[i]);
            }

            while (current != null)
            {
                for (var i = 0; i < current.Keys.Count; i++)
                {
                    var comparison = current.Keys[i].CompareTo(key);

                    switch (comparison)
                    {
                        case 0:
                            result.Add(current.PageIds[i]);
                            break;
                        case > 0:
                            return Result<List<ulong>, BPlusTreeError>.Success(result);
                    }
                }

                current = current.Next != -1 ? await LoadNodeAsync(current.Next) : null;
            }

            return Result<List<ulong>, BPlusTreeError>.Success(result);
        }
        catch (Exception ex)
        {
            _logger.Error(ex, "Error searching for key {Key} in B+ tree", key);
            return Result<List<ulong>, BPlusTreeError>.Error(
                new BPlusTreeError($"Failed to search for key: {ex.Message}"));
        }
    }

    public async Task<Result<Unit, BPlusTreeError>> InsertAsync(TKey key, ulong pageID)
    {
        try
        {
            var rootNode = await LoadNodeAsync(_rootId);

            var splitResult = await InsertInternalAsync(rootNode, key, pageID);
            if (splitResult == null)
                return Result<Unit, BPlusTreeError>.Success(new Unit());

            var newRoot = new Node
            {
                Id = _nextId++,
                IsLeaf = false,
                Keys = [splitResult.Item1],
                Children = [rootNode.Id, splitResult.Item2.Id],
                PageIds = []
            };

            _rootId = newRoot.Id;
            _height++;

            await SaveNodeAsync(newRoot);
            await SaveMetadataAsync();

            return Result<Unit, BPlusTreeError>.Success(new Unit());
        }
        catch (Exception ex)
        {
            _logger.Error(ex, "Error inserting key {Key} with pageID {PageID} in B+ tree", key, pageID);
            return Result<Unit, BPlusTreeError>.Error(new BPlusTreeError($"Failed to insert key: {ex.Message}"));
        }
    }

    public Task<Result<int, BPlusTreeError>> HeightAsync()
    {
        try
        {
            return Task.FromResult(Result<int, BPlusTreeError>.Success(_height));
        }
        catch (Exception ex)
        {
            _logger.Error(ex, "Error getting B+ tree height");
            return Task.FromResult(
                Result<int, BPlusTreeError>.Error(new BPlusTreeError($"Failed to get height: {ex.Message}")));
        }
    }

    public async Task<Result<Unit, BPlusTreeError>> PrintTreeAsync()
    {
        try
        {
            Console.WriteLine("B+ Tree Structure:");
            await PrintNodeAsync(await LoadNodeAsync(_rootId), 0);

            Console.WriteLine("\nLeaf Nodes (left to right):");
            await PrintLeafNodesAsync();

            return Result<Unit, BPlusTreeError>.Success(new Unit());
        }
        catch (Exception ex)
        {
            _logger.Error(ex, "Error printing B+ tree");
            return Result<Unit, BPlusTreeError>.Error(new BPlusTreeError($"Failed to print tree: {ex.Message}"));
        }
    }

    private async Task LoadMetadataAsync()
    {
        using var reader = new StreamReader(_indexFilePath);
        while (await reader.ReadLineAsync() is { } line)
        {
            if (line.StartsWith("ROOT_ID="))
                _rootId = int.Parse(line.Substring("ROOT_ID=".Length));
            else if (line.StartsWith("NEXT_ID="))
                _nextId = int.Parse(line.Substring("NEXT_ID=".Length));
            else if (line.StartsWith("HEIGHT="))
                _height = int.Parse(line.Substring("HEIGHT=".Length));

            if (line.Trim() == string.Empty)
                break;
        }
    }

    private async Task SaveMetadataAsync()
    {
        var allLines = File.Exists(_indexFilePath) ? await File.ReadAllLinesAsync(_indexFilePath) : [];
        var nodeLines = allLines.SkipWhile(l => !string.IsNullOrEmpty(l) && !l.StartsWith("NODE ")).ToList();

        var metadataLines = new List<string> { $"ROOT_ID={_rootId}", $"NEXT_ID={_nextId}", $"HEIGHT={_height}", "" };

        await File.WriteAllLinesAsync(_indexFilePath, metadataLines.Concat(nodeLines));
    }

    private async Task<Node> LoadNodeAsync(int nodeId)
    {
        var node = new Node { Id = nodeId };

        using var reader = new StreamReader(_indexFilePath);
        while (await reader.ReadLineAsync() is { } line)
            if (line.StartsWith($"NODE {nodeId} | "))
            {
                var parts = line.Split('|');

                var isLeafPart = parts[1].Trim();
                node.IsLeaf = isLeafPart.Substring(isLeafPart.IndexOf('=') + 1) == "true";

                var keysPart = parts[2].Trim();
                var keysStr = keysPart.Substring(keysPart.IndexOf('=') + 1);
                node.Keys = string.IsNullOrEmpty(keysStr)
                    ? []
                    : keysStr.Split(',').Select(k => (TKey)Convert.ChangeType(k, typeof(TKey))).ToList();

                if (node.IsLeaf)
                {
                    var valuesPart = parts[3].Trim();
                    var valuesStr = valuesPart.Substring(valuesPart.IndexOf('=') + 1);
                    node.PageIds = string.IsNullOrEmpty(valuesStr)
                        ? new List<ulong>()
                        : valuesStr.Split(',').Select(ulong.Parse).ToList();

                    var nextPart = parts[4].Trim();
                    var nextStr = nextPart.Substring(nextPart.IndexOf('=') + 1);
                    node.Next = nextStr == "null" ? -1 : int.Parse(nextStr);
                }
                else
                {
                    var childrenPart = parts[3].Trim();
                    var childrenStr = childrenPart.Substring(childrenPart.IndexOf('=') + 1);
                    node.Children = string.IsNullOrEmpty(childrenStr)
                        ? []
                        : childrenStr.Split(',').Select(int.Parse).ToList();
                }

                break;
            }

        return node;
    }

    private async Task SaveNodeAsync(Node node)
    {
        var sb = new StringBuilder();

        if (node.IsLeaf)
        {
            sb.Append($"NODE {node.Id} | LEAF=true | KEYS=");
            sb.Append(string.Join(",", node.Keys));
            sb.Append(" | VALUES=");
            sb.Append(string.Join(",", node.PageIds));
            sb.Append(" | NEXT=");
            sb.Append(node.Next == -1 ? "null" : node.Next.ToString());
        }
        else
        {
            sb.Append($"NODE {node.Id} | LEAF=false | KEYS=");
            sb.Append(string.Join(",", node.Keys));
            sb.Append(" | CHILDREN=");
            sb.Append(string.Join(",", node.Children));
        }

        var nodeStr = sb.ToString();

        var allLines = File.Exists(_indexFilePath) ? await File.ReadAllLinesAsync(_indexFilePath) : [];
        var linesList = allLines.ToList();

        var nodeIndex = linesList.FindIndex(l => l.StartsWith($"NODE {node.Id} | "));
        if (nodeIndex >= 0)
            linesList[nodeIndex] = nodeStr;
        else
            linesList.Add(nodeStr);

        await File.WriteAllLinesAsync(_indexFilePath, linesList);
    }

    private async Task<Tuple<TKey, Node>?> InsertInternalAsync(Node node, TKey key, ulong valueOffset)
    {
        if (node.IsLeaf)
        {
            var i = node.Keys.FindIndex(k => key.CompareTo(k) < 0);

            if (i == -1)
                i = node.Keys.Count;

            node.Keys.Insert(i, key);
            node.PageIds.Insert(i, valueOffset);

            await SaveNodeAsync(node);

            return node.Keys.Count >= _degree ? await SplitLeafAsync(node) : null;
        }
        else
        {
            var i = node.Keys.FindIndex(k => key.CompareTo(k) < 0);
            i = i == -1 ? node.Keys.Count : i;

            var childNode = await LoadNodeAsync(node.Children[i]);
            var splitResult = await InsertInternalAsync(childNode, key, valueOffset);

            if (splitResult == null)
                return null;

            node.Keys.Insert(i, splitResult.Item1);
            node.Children.Insert(i + 1, splitResult.Item2.Id);

            await SaveNodeAsync(node);

            if (node.Keys.Count < _degree)
                return null;

            return await SplitInternalAsync(node);
        }
    }

    private async Task<Tuple<TKey, Node>> SplitLeafAsync(Node node)
    {
        var mid = node.Keys.Count / 2;

        var newNode = new Node
        {
            Id = _nextId++,
            IsLeaf = true,
            Keys = node.Keys.GetRange(mid, node.Keys.Count - mid),
            Children = new List<int>(),
            PageIds = node.PageIds.GetRange(mid, node.PageIds.Count - mid),
            Next = node.Next
        };

        node.Keys.RemoveRange(mid, node.Keys.Count - mid);
        node.PageIds.RemoveRange(mid, node.PageIds.Count - mid);
        node.Next = newNode.Id;

        await SaveNodeAsync(newNode);
        await SaveNodeAsync(node);

        return Tuple.Create(newNode.Keys[0], newNode);
    }

    private async Task<Tuple<TKey, Node>> SplitInternalAsync(Node node)
    {
        var mid = node.Keys.Count / 2;

        var newNode = new Node
        {
            Id = _nextId++,
            IsLeaf = false,
            Keys = node.Keys.GetRange(mid + 1, node.Keys.Count - mid - 1),
            Children = node.Children.GetRange(mid + 1, node.Children.Count - mid - 1),
            PageIds = []
        };

        var midKey = node.Keys[mid];

        node.Keys.RemoveRange(mid, node.Keys.Count - mid);
        node.Children.RemoveRange(mid + 1, node.Children.Count - mid - 1);

        await SaveNodeAsync(newNode);
        await SaveNodeAsync(node);

        return Tuple.Create(midKey, newNode);
    }

    private async Task PrintNodeAsync(Node node, int level)
    {
        var indent = new string(' ', level * 4);
        Console.WriteLine($"{indent}Node {node.Id} - Keys: [{string.Join(", ", node.Keys)}]");

        if (!node.IsLeaf)
            for (var i = node.Children.Count - 1; i >= 0; i--)
                await PrintNodeAsync(await LoadNodeAsync(i), level + 1);
        else if (level > 0)
            for (var i = 0; i < node.Keys.Count; i++)
                Console.WriteLine($"{indent}    Key {node.Keys[i]}: Value Offset: {node.PageIds[i]}");
    }

    private async Task PrintLeafNodesAsync()
    {
        var current = await LoadNodeAsync(_rootId);

        while (!current.IsLeaf) current = await LoadNodeAsync(current.Children[0]);

        while (current != null)
        {
            Console.Write($"[{string.Join(", ", current.Keys)}] -> ");
            current = current.Next != -1 ? await LoadNodeAsync(current.Next) : null;
        }

        Console.WriteLine("null");
    }

    private class Node
    {
        public int Id { get; set; }
        public bool IsLeaf { get; set; }
        public List<TKey> Keys { get; set; } = [];
        public List<int> Children { get; set; } = [];
        public List<ulong> PageIds { get; set; } = [];
        public int Next { get; set; } // For leaf nodes
    }
}
