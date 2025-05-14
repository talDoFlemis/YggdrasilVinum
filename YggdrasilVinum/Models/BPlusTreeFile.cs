using System.Text;

namespace YggdrasilVinum.Models;

public class BPlusTreeFile<TKey, TValue>
    where TKey : IComparable<TKey>
{
    private readonly string _dataFilePath;
    private readonly int _degree;
    private readonly string _indexFilePath;
    private int _height;
    private int _nextId;
    private int _rootId;

    public BPlusTreeFile(string indexFilePath, string dataFilePath, int degree)
    {
        _indexFilePath = indexFilePath;
        _dataFilePath = dataFilePath;
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
                Values = [],
                Next = -1
            };

            SaveNode(rootNode);
            SaveMetadata();
        }
        else
        {
            LoadMetadata();
        }

        if (!File.Exists(dataFilePath)) File.WriteAllText(dataFilePath, "");
    }

    private void LoadMetadata()
    {
        using var reader = new StreamReader(_indexFilePath);
        while (reader.ReadLine() is { } line)
        {
            if (line.StartsWith("ROOT_ID="))
                _rootId = int.Parse(line.Substring("ROOT_ID=".Length));
            else if (line.StartsWith("NEXT_ID="))
                _nextId = int.Parse(line.Substring("NEXT_ID=".Length));
            else if (line.StartsWith("HEIGHT="))
                _height = int.Parse(line.Substring("HEIGHT=".Length));

            // Stop after reading metadata
            if (line.Trim() == string.Empty)
                break;
        }
    }

    private void SaveMetadata()
    {
        var allLines = File.Exists(_indexFilePath) ? File.ReadAllLines(_indexFilePath) : [];
        var nodeLines = allLines.SkipWhile(l => !string.IsNullOrEmpty(l) && !l.StartsWith("NODE ")).ToList();

        var metadataLines = new List<string> { $"ROOT_ID={_rootId}", $"NEXT_ID={_nextId}", $"HEIGHT={_height}", "" };

        File.WriteAllLines(_indexFilePath, metadataLines.Concat(nodeLines));
    }

    private Node LoadNode(int nodeId)
    {
        var node = new Node { Id = nodeId };

        using var reader = new StreamReader(_indexFilePath);
        while (reader.ReadLine() is { } line)
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
                    // Parse Values for leaf node
                    var valuesPart = parts[3].Trim();
                    var valuesStr = valuesPart.Substring(valuesPart.IndexOf('=') + 1);
                    node.Values = string.IsNullOrEmpty(valuesStr) ? new List<string>() : valuesStr.Split(',').ToList();

                    // Parse Next for leaf node
                    var nextPart = parts[4].Trim();
                    var nextStr = nextPart.Substring(nextPart.IndexOf('=') + 1);
                    node.Next = nextStr == "null" ? -1 : int.Parse(nextStr);
                }
                else
                {
                    // Parse Children for internal node
                    var childrenPart = parts[3].Trim();
                    var childrenStr = childrenPart.Substring(childrenPart.IndexOf('=') + 1);
                    node.Children = string.IsNullOrEmpty(childrenStr)
                        ? new List<int>()
                        : childrenStr.Split(',').Select(int.Parse).ToList();
                }

                break;
            }

        return node;
    }

    private void SaveNode(Node node)
    {
        var sb = new StringBuilder();

        if (node.IsLeaf)
        {
            sb.Append($"NODE {node.Id} | LEAF=true | KEYS=");
            sb.Append(string.Join(",", node.Keys));
            sb.Append(" | VALUES=");
            sb.Append(string.Join(",", node.Values));
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

        var allLines = File.Exists(_indexFilePath) ? File.ReadAllLines(_indexFilePath).ToList() : [];

        var nodeIndex = allLines.FindIndex(l => l.StartsWith($"NODE {node.Id} | "));
        if (nodeIndex >= 0)
            allLines[nodeIndex] = nodeStr;
        else
            allLines.Add(nodeStr);

        File.WriteAllLines(_indexFilePath, allLines);
    }

    private string AppendToDataFile(TValue value)
    {
        var offset = new FileInfo(_dataFilePath).Length;

        using (var writer = new StreamWriter(_dataFilePath, true))
        {
            writer.WriteLine(value.ToString());
        }

        return $"OFFSET:{offset}";
    }

    private TValue ReadFromDataFile(string offsetValue)
    {
        // Parse offset
        var offsetStr = offsetValue.Split(':')[1];
        var offset = long.Parse(offsetStr);

        // Read value from file at offset
        using var reader = new StreamReader(_dataFilePath);
        reader.BaseStream.Seek(offset, SeekOrigin.Begin);
        var line = reader.ReadLine();

        return (TValue)Convert.ChangeType(line, typeof(TValue));
    }

    public List<TValue> Search(TKey key)
    {
        var rootNode = LoadNode(_rootId);
        List<TValue> result = [];

        var current = rootNode;

        // Traverse down to leaf level
        while (!current.IsLeaf)
        {
            var i = current.Keys.FindIndex(k => key.CompareTo(k) <= 0);
            i = i == -1 ? current.Keys.Count : i;
            current = LoadNode(current.Children[i]);
        }

        // Search leaf nodes
        while (current != null)
        {
            for (var i = 0; i < current.Keys.Count; i++)
            {
                var comparison = current.Keys[i].CompareTo(key);

                if (comparison == 0)
                    // Found matching key, retrieve value from data file
                    result.Add(ReadFromDataFile(current.Values[i]));
                else if (comparison > 0)
                    // Keys are sorted, so if current key is greater, no more matches
                    return result;
            }

            // Move to next leaf node if any
            current = current.Next != -1 ? LoadNode(current.Next) : null;
        }

        return result;
    }

    public void Insert(TKey key, TValue value)
    {
        var rootNode = LoadNode(_rootId);
        var valueOffset = AppendToDataFile(value);

        var splitResult = InsertInternal(rootNode, key, valueOffset);
        if (splitResult == null)
            return;

        // Create new root
        var newRoot = new Node
        {
            Id = _nextId++,
            IsLeaf = false,
            Keys = [splitResult.Item1],
            Children = [rootNode.Id, splitResult.Item2.Id],
            Values = []
        };

        _rootId = newRoot.Id;
        _height++;

        SaveNode(newRoot);
        SaveMetadata();
    }

    private Tuple<TKey, Node>? InsertInternal(Node node, TKey key, string valueOffset)
    {
        if (node.IsLeaf)
        {
            var i = node.Keys.FindIndex(k => key.CompareTo(k) < 0);

            // If key already exists, we'll still add it with a new value offset
            // Note: This implementation allows duplicate keys in leaf nodes

            if (i == -1)
                i = node.Keys.Count;

            node.Keys.Insert(i, key);
            node.Values.Insert(i, valueOffset);

            SaveNode(node);

            return node.Keys.Count >= _degree ? SplitLeaf(node) : null;
        }
        else
        {
            var i = node.Keys.FindIndex(k => key.CompareTo(k) < 0);
            i = i == -1 ? node.Keys.Count : i;

            var childNode = LoadNode(node.Children[i]);
            var splitResult = InsertInternal(childNode, key, valueOffset);

            if (splitResult == null)
                return null;

            node.Keys.Insert(i, splitResult.Item1);
            node.Children.Insert(i + 1, splitResult.Item2.Id);

            SaveNode(node);

            if (node.Keys.Count < _degree)
                return null;

            return SplitInternal(node);
        }
    }

    private Tuple<TKey, Node> SplitLeaf(Node node)
    {
        var mid = node.Keys.Count / 2;

        var newNode = new Node
        {
            Id = _nextId++,
            IsLeaf = true,
            Keys = node.Keys.GetRange(mid, node.Keys.Count - mid),
            Children = new List<int>(),
            Values = node.Values.GetRange(mid, node.Values.Count - mid),
            Next = node.Next
        };

        node.Keys.RemoveRange(mid, node.Keys.Count - mid);
        node.Values.RemoveRange(mid, node.Values.Count - mid);
        node.Next = newNode.Id;

        SaveNode(newNode);
        SaveNode(node);

        return Tuple.Create(newNode.Keys[0], newNode);
    }

    private Tuple<TKey, Node> SplitInternal(Node node)
    {
        var mid = node.Keys.Count / 2;

        var newNode = new Node
        {
            Id = _nextId++,
            IsLeaf = false,
            Keys = node.Keys.GetRange(mid + 1, node.Keys.Count - mid - 1),
            Children = node.Children.GetRange(mid + 1, node.Children.Count - mid - 1),
            Values = []
        };

        var midKey = node.Keys[mid];

        node.Keys.RemoveRange(mid, node.Keys.Count - mid);
        node.Children.RemoveRange(mid + 1, node.Children.Count - mid - 1);

        SaveNode(newNode);
        SaveNode(node);

        return Tuple.Create(midKey, newNode);
    }

    public int Height()
    {
        return _height;
    }

    public void PrintTree()
    {
        Console.WriteLine("B+ Tree Structure:");
        PrintNode(LoadNode(_rootId), 0);

        Console.WriteLine("\nLeaf Nodes (left to right):");
        PrintLeafNodes();
    }

    private void PrintNode(Node node, int level)
    {
        var indent = new string(' ', level * 4);
        Console.WriteLine($"{indent}Node {node.Id} - Keys: [{string.Join(", ", node.Keys)}]");

        if (!node.IsLeaf)
            for (var i = node.Children.Count - 1; i >= 0; i--)
                PrintNode(LoadNode(i), level + 1);
        else if (level > 0)
            for (var i = 0; i < node.Keys.Count; i++)
                Console.WriteLine($"{indent}    Key {node.Keys[i]}: Value Offset: {node.Values[i]}");
    }

    private void PrintLeafNodes()
    {
        var current = LoadNode(_rootId);

        while (!current.IsLeaf) current = LoadNode(current.Children[0]);

        while (current != null)
        {
            Console.Write($"[{string.Join(", ", current.Keys)}] -> ");
            current = current.Next != -1 ? LoadNode(current.Next) : null;
        }

        Console.WriteLine("null");
    }

    private class Node
    {
        public int Id { get; set; }
        public bool IsLeaf { get; set; }
        public List<TKey> Keys { get; set; } = [];
        public List<int> Children { get; set; } = [];
        public List<string> Values { get; set; } = [];
        public int Next { get; set; } // For leaf nodes
    }
}
