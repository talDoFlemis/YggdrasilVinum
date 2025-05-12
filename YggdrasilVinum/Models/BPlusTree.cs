namespace YggdrasilVinum.Models;

public class BPlusTree<TKey, TValue>(int degree)
    where TKey : IComparable<TKey>
{
    private Node _root = new() { IsLeaf = true };

    public List<TValue> Search(TKey key)
    {
        List<TValue> result = [];
        var current = _root;

        while (!current.IsLeaf)
        {
            var i = current.Keys.FindIndex(k => key.CompareTo(k) <= 0);
            i = i == -1 ? current.Keys.Count : i;
            current = current.Children[i];
        }

        while (current != null)
        {
            for (var i = 0; i < current.Keys.Count; i++)
            {
                var comparison = current.Keys[i].CompareTo(key);

                switch (comparison)
                {
                    case 0:
                        result.AddRange(current.Values[i]);
                        break;
                    case > 0:
                        return result;
                }
            }

            current = current.Next;
        }

        return result;
    }

    public void Insert(TKey key, TValue value)
    {
        var split = InsertInternal(_root, key, value);
        if (split == null) return;
        var newRoot = new Node { IsLeaf = false };
        newRoot.Keys.Add(split.Item1);
        newRoot.Children.Add(_root);
        newRoot.Children.Add(split.Item2);
        _root = newRoot;
    }

    private Tuple<TKey, Node>? InsertInternal(Node node, TKey key, TValue value)
    {
        if (node.IsLeaf)
        {
            var i = node.Keys.FindIndex(k => key.CompareTo(k) < 0);
            if (i != -1 && node.Keys.Count > i && node.Keys[i].CompareTo(key) == 0)
            {
                node.Values[i].Add(value);
                return null;
            }

            if (i == -1) i = node.Keys.Count;

            node.Keys.Insert(i, key);
            node.Values.Insert(i, [value]);

            return node.Keys.Count < degree ? null : SplitLeaf(node);
        }
        else
        {
            var i = node.Keys.FindIndex(k => key.CompareTo(k) < 0);
            i = i == -1 ? node.Keys.Count : i;

            var split = InsertInternal(node.Children[i], key, value);
            if (split == null) return null;

            node.Keys.Insert(i, split.Item1);
            node.Children.Insert(i + 1, split.Item2);

            if (node.Keys.Count < degree)
                return null;

            return SplitInternal(node);
        }
    }

    private static Tuple<TKey, Node> SplitLeaf(Node node)
    {
        var mid = node.Keys.Count / 2;
        var newNode = new Node { IsLeaf = true };
        newNode.Keys.AddRange(node.Keys.GetRange(mid, node.Keys.Count - mid));
        newNode.Values.AddRange(node.Values.GetRange(mid, node.Values.Count - mid));
        node.Keys.RemoveRange(mid, node.Keys.Count - mid);
        node.Values.RemoveRange(mid, node.Values.Count - mid);

        newNode.Next = node.Next;
        node.Next = newNode;

        return Tuple.Create(newNode.Keys[0], newNode);
    }

    private static Tuple<TKey, Node> SplitInternal(Node node)
    {
        var mid = node.Keys.Count / 2;
        var newNode = new Node { IsLeaf = false };

        newNode.Keys.AddRange(node.Keys.GetRange(mid + 1, node.Keys.Count - mid - 1));
        newNode.Children.AddRange(node.Children.GetRange(mid + 1, node.Children.Count - mid - 1));

        var midKey = node.Keys[mid];

        node.Keys.RemoveRange(mid, node.Keys.Count - mid);
        node.Children.RemoveRange(mid + 1, node.Children.Count - mid - 1);

        return Tuple.Create(midKey, newNode);
    }

    public int Height()
    {
        var height = 0;
        var current = _root;
        while (!current.IsLeaf)
        {
            height++;
            current = current.Children[0];
        }

        return height;
    }

    public void PrintTree()
    {
        Console.WriteLine("B+ Tree Structure:");
        PrintNode(_root, 0);
        Console.WriteLine("\nLeaf Nodes (left to right):");
        PrintLeafNodes();
    }

    private static void PrintNode(Node node, int level)
    {
        var indent = new string(' ', level * 4);
        Console.Write($"{indent}Keys: [");
        Console.Write(string.Join(", ", node.Keys));
        Console.WriteLine("]");

        if (!node.IsLeaf)
            for (var i = 0; i < node.Children.Count; i++)
            {
                Console.WriteLine($"{indent}Child {i}:");
                PrintNode(node.Children[i], level + 1);
            }
        else if (level > 0)
            for (var i = 0; i < node.Keys.Count; i++)
                Console.WriteLine($"{indent}    Key {node.Keys[i]}: Values: [{string.Join(", ", node.Values[i])}]");
    }

    private void PrintLeafNodes()
    {
        var current = _root;
        while (!current.IsLeaf) current = current.Children[0];

        while (current != null)
        {
            Console.Write($"[{string.Join(", ", current.Keys)}] -> ");
            current = current.Next;
        }

        Console.WriteLine("null");
    }

    private class Node
    {
        public readonly List<Node> Children = [];
        public bool IsLeaf;
        public readonly List<TKey> Keys = [];
        public Node? Next;
        public readonly List<List<TValue>> Values = [];
    }
}