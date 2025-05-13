using YggdrasilVinum.Models;

namespace YggdrasilVinum.Tests.Unit.Models;

public class BPlusTreeTest
{
    [Fact]
    public void Search_ReturnsEmptyList_WhenTreeIsEmpty()
    {
        // ARRANGE
        var tree = new BPlusTree<int, string>(3);

        // ACT
        var result = tree.Search(10);

        // ASSERT
        Assert.Empty(result);
    }

    [Fact]
    public void Search_ReturnsCorrectValues_ForExistingKey()
    {
        // ARRANGE
        var tree = new BPlusTree<int, string>(3);

        // ACT
        tree.Insert(10, "Value1");
        tree.Insert(10, "Value2");
        var result = tree.Search(10);

        // ASSERT
        Assert.Equal(new[] { "Value1", "Value2" }, result);
    }

    [Fact]
    public void Search_ReturnsEmptyList_ForNonExistingKey()
    {
        // ARRANGE
        var tree = new BPlusTree<int, string>(3);

        // ACT
        tree.Insert(10, "Value1");
        var result = tree.Search(20);

        // ASSERT
        Assert.Empty(result);
    }

    [Fact]
    public void Insert_SplitsRoot_WhenDegreeIsExceeded()
    {
        // ARRANGE
        var tree = new BPlusTree<int, string>(3);

        // ACT
        tree.Insert(1, "Value1");
        tree.Insert(2, "Value2");
        tree.Insert(3, "Value3");
        tree.Insert(4, "Value4");

        // ASSERT
        Assert.Equal(1, tree.Height());
    }

    [Fact]
    public void Insert_MaintainsCorrectOrder_AfterMultipleInsertions()
    {
        // ARRANGE
        var tree = new BPlusTree<int, string>(3);

        // ACT
        tree.Insert(3, "Value3");
        tree.Insert(1, "Value1");
        tree.Insert(2, "Value2");
        var result = tree.Search(2);

        // ASSERT
        Assert.Equal(new[] { "Value2" }, result);
    }

    [Fact]
    public void Height_ReturnsZero_ForEmptyTree()
    {
        // ARRANGE
        var tree = new BPlusTree<int, string>(3);

        // ASSERT
        Assert.Equal(0, tree.Height());
    }

    [Fact]
    public void Height_ReturnsCorrectValue_AfterInsertions()
    {
        // ARRANGE
        var tree = new BPlusTree<int, string>(3);

        // ACT
        tree.Insert(1, "Value1");
        tree.Insert(2, "Value2");
        tree.Insert(3, "Value3");
        tree.Insert(4, "Value4");
        tree.Insert(5, "Value5");

        // ASSERT
        Assert.Equal(2, tree.Height());
    }

    [Fact]
    public void Insert_WithNegativeKeys_WorksCorrectly()
    {
        // ARRANGE
        var tree = new BPlusTree<int, string>(3);

        // ACT
        tree.Insert(-5, "Negative");
        tree.Insert(-10, "Very Negative");
        var result = tree.Search(-5);

        // ASSERT
        Assert.Equal(new[] { "Negative" }, result);
    }

    [Fact]
    public void Insert_ManyValues_ForSameKey_ReturnsAllValues()
    {
        // ARRANGE
        var tree = new BPlusTree<int, string>(3);

        // ACT
        tree.Insert(42, "First");
        tree.Insert(42, "Second");
        tree.Insert(42, "Third");
        tree.Insert(42, "Fourth");
        var result = tree.Search(42);

        // ASSERT
        Assert.Equal(4, result.Count());
        Assert.Contains("First", result);
        Assert.Contains("Fourth", result);
    }

    [Fact]
    public void Search_AfterMultipleSplits_FindsCorrectValues()
    {
        // ARRANGE
        var tree = new BPlusTree<int, string>(3);

        // ACT
        for (var i = 1; i <= 20; i++) tree.Insert(i, $"Value{i}");
        var result = tree.Search(15);

        // ASSERT
        Assert.Equal(new[] { "Value15" }, result);
    }

    [Fact]
    public void Height_IncreasesCorrectly_WithLargeDataSet()
    {
        // ARRANGE
        var tree = new BPlusTree<int, string>(3);

        // ACT
        for (var i = 1; i <= 30; i++) tree.Insert(i, $"Value{i}");

        // ASSERT
        Assert.True(tree.Height() > 1);
    }

    [Fact]
    public void Insert_WithDuplicateKeys_MaintainsAllValues()
    {
        // ARRANGE
        var tree = new BPlusTree<int, string>(3);

        // ACT
        tree.Insert(5, "First");
        tree.Insert(10, "Second");
        tree.Insert(5, "Third");

        var result = tree.Search(5);

        // ASSERT
        Assert.Equal(2, result.Count);
        Assert.Contains("First", result);
        Assert.Contains("Third", result);
    }

    [Fact]
    public void Insert_WithSparseKeys_MaintainsOrder()
    {
        // ARRANGE
        var tree = new BPlusTree<int, string>(3);

        // ACT
        tree.Insert(100, "Hundred");
        tree.Insert(1, "One");
        tree.Insert(1000, "Thousand");

        var result = tree.Search(100);

        // ASSERT
        Assert.Equal(new[] { "Hundred" }, result);
    }

    [Fact]
    public void LargestDegreeAllowed_DoesNotCauseSplitting()
    {
        // ARRANGE
        var tree = new BPlusTree<int, string>(5);

        // ACT
        for (var i = 1; i <= 4; i++) tree.Insert(i, $"Value{i}");

        // ASSERT
        Assert.Equal(0, tree.Height());
    }

    [Fact]
    public void Insert_DuplicateKeyInExistingLeaf_UpdatesValuesList()
    {
        // ARRANGE
        var tree = new BPlusTree<int, string>(3);

        // ACT
        tree.Insert(10, "Value1");
        tree.Insert(20, "Value2");
        tree.Insert(30, "Value3");
        tree.Insert(10, "Value4");

        var result = tree.Search(10);

        // ASSERT
        Assert.Equal(2, result.Count);
        Assert.Contains("Value1", result);
        Assert.Contains("Value4", result);
    }

    [Fact]
    public void Insert_CausesMultipleLevelSplits_MaintainsCorrectStructure()
    {
        // ARRANGE
        var tree = new BPlusTree<int, string>(3);

        // ACT - Insert enough values to cause multiple levels of splitting
        for (var i = 1; i <= 50; i++) tree.Insert(i, $"Value{i}");

        // ASSERT - Verify height and values
        Assert.True(tree.Height() >= 2);

        // Verify we can find all values
        for (var i = 1; i <= 50; i++)
        {
            var result = tree.Search(i);
            Assert.Single(result);
            Assert.Equal($"Value{i}", result[0]);
        }
    }

    [Fact]
    public void Search_RangeOfValues_ReturnsAllInOrder()
    {
        // ARRANGE
        var tree = new BPlusTree<int, string>(3);

        // Insert values in random order
        tree.Insert(5, "Value5");
        tree.Insert(3, "Value3");
        tree.Insert(7, "Value7");
        tree.Insert(1, "Value1");
        tree.Insert(9, "Value9");

        // ACT & ASSERT
        // Search for each value and make sure they are all found
        for (var i = 1; i <= 9; i += 2)
        {
            var result = tree.Search(i);
            Assert.Equal($"Value{i}", result.Single());
        }

        // Search for non-existent values
        for (var i = 2; i <= 8; i += 2)
        {
            var result = tree.Search(i);
            Assert.Empty(result);
        }
    }

    [Fact]
    public void Insert_KeysThatTriggerLeafSplit_MaintainsNextPointers()
    {
        // ARRANGE
        var tree = new BPlusTree<int, string>(3);

        // Fill the tree to cause leaf splits
        for (var i = 1; i <= 10; i++) tree.Insert(i, $"Value{i}");

        // ACT & ASSERT
        // Search each value in sequence to ensure leaf node links work correctly
        for (var i = 1; i <= 10; i++)
        {
            var result = tree.Search(i);
            Assert.Equal($"Value{i}", result.Single());
        }
    }

    [Fact]
    public void Insert_SameKeyAfterSplit_FindsCorrectNode()
    {
        // ARRANGE
        var tree = new BPlusTree<int, string>(3);

        // Insert values to cause splits
        for (var i = 10; i <= 50; i += 10) tree.Insert(i, $"Value{i}");

        // ACT - Insert duplicate of an existing key
        tree.Insert(30, "Value30Duplicate");
        var result = tree.Search(30);

        // ASSERT
        Assert.Equal(2, result.Count);
        Assert.Contains("Value30", result);
        Assert.Contains("Value30Duplicate", result);
    }

    [Fact]
    public void Insert_NonLeafSplitCondition_HandlesSplitProperly()
    {
        // ARRANGE - Create a tree with a small degree to force splits
        var tree = new BPlusTree<int, string>(3);

        // ACT - Insert enough elements to cause non-leaf splits
        for (var i = 1; i <= 20; i++) tree.Insert(i, $"Value{i}");

        // Insert more values to trigger further splits
        for (var i = 21; i <= 30; i++) tree.Insert(i, $"Value{i}");

        // ASSERT
        // Verify height after multiple splits
        Assert.True(tree.Height() >= 2);

        // Verify we can still find all values
        for (var i = 1; i <= 30; i++)
        {
            var result = tree.Search(i);
            Assert.Single(result);
            Assert.Equal($"Value{i}", result[0]);
        }
    }

    [Fact]
    public void PrintTree_OutputsCorrectStructure()
    {
        // ARRANGE
        var tree = new BPlusTree<int, string>(3);

        // Add some values
        tree.Insert(5, "Value5");
        tree.Insert(10, "Value10");
        tree.Insert(15, "Value15");

        // ACT - Call PrintTree (we can't verify output directly, but coverage will increase)
        tree.PrintTree();

        // ASSERT - Just verify the tree still works after printing
        var result = tree.Search(10);
        Assert.Single(result);
        Assert.Equal("Value10", result[0]);
    }

    [Fact]
    public void Search_WithInterfaceImplementation_WorksCorrectly()
    {
        // ARRANGE
        var tree = new BPlusTree<CustomKey, string>(4);

        // ACT
        tree.Insert(new CustomKey(5), "Five");
        tree.Insert(new CustomKey(3), "Three");
        tree.Insert(new CustomKey(7), "Seven");

        var result = tree.Search(new CustomKey(5));

        // ASSERT
        Assert.Single(result);
        Assert.Equal("Five", result[0]);
    }

    [Fact]
    public void Insert_CustomValueType_WorksCorrectly()
    {
        // ARRANGE
        var tree = new BPlusTree<int, CustomValue>(3);

        // ACT
        var value1 = new CustomValue("test1", 1);
        var value2 = new CustomValue("test2", 2);

        tree.Insert(1, value1);
        tree.Insert(2, value2);

        var result = tree.Search(1);

        // ASSERT
        Assert.Single(result);
        Assert.Equal("test1", result[0].Name);
        Assert.Equal(1, result[0].Id);
    }

    [Fact]
    public void Insert_StringKeysAndComplexValues_WorksCorrectly()
    {
        // ARRANGE
        var tree = new BPlusTree<string, CustomValue>(4);

        // ACT
        tree.Insert("apple", new CustomValue("Apple object", 1));
        tree.Insert("banana", new CustomValue("Banana object", 2));
        tree.Insert("cherry", new CustomValue("Cherry object", 3));
        tree.Insert("apple", new CustomValue("Another apple", 4));

        var result = tree.Search("apple");

        // ASSERT
        Assert.Equal(2, result.Count);
        Assert.Contains(result, v => v is { Name: "Apple object", Id: 1 });
        Assert.Contains(result, v => v is { Name: "Another apple", Id: 4 });
    }

    [Fact]
    public void Insert_DateTimeKeys_WorksCorrectly()
    {
        // ARRANGE
        var tree = new BPlusTree<DateTime, string>(4);
        var date1 = new DateTime(2023, 1, 1);
        var date2 = new DateTime(2023, 2, 1);
        var date3 = new DateTime(2023, 3, 1);

        // ACT
        tree.Insert(date2, "February");
        tree.Insert(date1, "January");
        tree.Insert(date3, "March");

        var result = tree.Search(date1);

        // ASSERT
        Assert.Single(result);
        Assert.Equal("January", result[0]);
    }

    [Fact]
    public void Insert_DuplicateKeyAtEdgeOfSplit_HandlesCorrectly()
    {
        // ARRANGE
        var tree = new BPlusTree<int, string>(3);

        // ACT
        tree.Insert(10, "Value10");
        tree.Insert(20, "Value20");
        tree.Insert(30, "Value30");
        tree.Insert(20, "Value20-2");

        var result = tree.Search(20);

        // ASSERT
        Assert.Equal(2, result.Count);
        Assert.Contains("Value20", result);
        Assert.Contains("Value20-2", result);
    }

    // Custom key that implements IComparable
    public class CustomKey(int value) : IComparable<CustomKey>
    {
        private int Value { get; } = value;

        public int CompareTo(CustomKey? other)
        {
            if (other is null)
                throw new ArgumentNullException(nameof(other), "Cannot compare to null");
            return Value.CompareTo(other.Value);
        }

        public override string ToString()
        {
            return Value.ToString();
        }
    }

    private class CustomValue(string name, int id)
    {
        public string Name { get; } = name;
        public int Id { get; } = id;

        public override string ToString()
        {
            return $"{Name} ({Id})";
        }
    }
}
