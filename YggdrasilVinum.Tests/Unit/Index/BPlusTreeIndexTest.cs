using YggdrasilVinum.Index;

namespace YggdrasilVinum.Tests.Unit.Index;

public class BPlusTreeIndexTest : IDisposable
{
    private const string _testIndexPath = "test_index.txt";
    private const string _testDataPath = "test_data.csv";
    private const int _defaultDegree = 3;

    public void Dispose()
    {
        if (File.Exists(_testIndexPath))
            File.Delete(_testIndexPath);

        if (File.Exists(_testDataPath))
            File.Delete(_testDataPath);
    }

    [Fact]
    public void Constructor_CreatesNewFiles_WhenNotExist()
    {
        // Act
        _ = new BPlusTreeIndex<int, string>(_testIndexPath, _testDataPath, _defaultDegree);

        // Assert
        Assert.True(File.Exists(_testIndexPath));
        Assert.True(File.Exists(_testDataPath));
    }

    [Fact]
    public void Search_EmptyTree_ReturnsEmptyList()
    {
        // Arrange
        var tree = new BPlusTreeIndex<int, string>(_testIndexPath, _testDataPath, _defaultDegree);

        // Act
        var result = tree.Search(1);

        // Assert
        Assert.Empty(result);
    }

    [Fact]
    public void Insert_SingleValue_CanBeRetrieved()
    {
        // Arrange
        var tree = new BPlusTreeIndex<int, string>(_testIndexPath, _testDataPath, _defaultDegree);

        // Act
        tree.Insert(5, "Test Value");
        var result = tree.Search(5);

        // Assert
        Assert.Single(result);
        Assert.Equal("Test Value", result[0]);
    }

    [Fact]
    public void Insert_MultipleValues_CanBeRetrieved()
    {
        // Arrange
        var tree = new BPlusTreeIndex<int, string>(_testIndexPath, _testDataPath, _defaultDegree);

        // Act
        tree.Insert(1, "First");
        tree.Insert(2, "Second");
        tree.Insert(3, "Third");

        // Assert
        Assert.Equal("First", tree.Search(1)[0]);
        Assert.Equal("Second", tree.Search(2)[0]);
        Assert.Equal("Third", tree.Search(3)[0]);
    }

    [Fact]
    public void Insert_DuplicateKeys_StoresAllValues()
    {
        // Arrange
        var tree = new BPlusTreeIndex<int, string>(_testIndexPath, _testDataPath, _defaultDegree);

        // Act
        tree.Insert(10, "Value1");
        tree.Insert(10, "Value2");
        var result = tree.Search(10);

        // Assert
        Assert.Equal(2, result.Count);
        Assert.Contains("Value1", result);
        Assert.Contains("Value2", result);
    }

    [Fact]
    public void Search_NonExistentKey_ReturnsEmptyList()
    {
        // Arrange
        var tree = new BPlusTreeIndex<int, string>(_testIndexPath, _testDataPath, _defaultDegree);
        tree.Insert(1, "One");

        // Act
        var result = tree.Search(2);

        // Assert
        Assert.Empty(result);
    }

    [Fact]
    public void Height_EmptyTree_ReturnsZero()
    {
        // Arrange
        var tree = new BPlusTreeIndex<int, string>(_testIndexPath, _testDataPath, _defaultDegree);

        // Act & Assert
        Assert.Equal(0, tree.Height());
    }

    [Fact]
    public void Height_AfterSplits_IncreasesCorrectly()
    {
        // Arrange
        var tree = new BPlusTreeIndex<int, string>(_testIndexPath, _testDataPath, _defaultDegree);

        // Act
        for (var i = 1; i <= 10; i++) tree.Insert(i, $"Value{i}");

        // Assert
        Assert.True(tree.Height() > 0);
    }

    [Fact]
    public void Insert_ManyValues_MaintainsCorrectOrder()
    {
        // Arrange
        var tree = new BPlusTreeIndex<int, string>(_testIndexPath, _testDataPath, _defaultDegree);

        // Act
        tree.Insert(8, "Value8");
        tree.Insert(3, "Value3");
        tree.Insert(1, "Value1");
        tree.Insert(6, "Value6");
        tree.Insert(4, "Value4");

        // Assert
        Assert.Equal("Value1", tree.Search(1)[0]);
        Assert.Equal("Value3", tree.Search(3)[0]);
        Assert.Equal("Value4", tree.Search(4)[0]);
        Assert.Equal("Value6", tree.Search(6)[0]);
        Assert.Equal("Value8", tree.Search(8)[0]);
    }

    [Fact]
    public void Insert_EnoughValuesToCauseSplits_CanStillRetrieveAll()
    {
        // Arrange
        var tree = new BPlusTreeIndex<int, string>(_testIndexPath, _testDataPath, _defaultDegree);

        // Act
        for (var i = 1; i <= 20; i++) tree.Insert(i, $"Value{i}");

        // Assert
        Assert.Equal("Value5", tree.Search(5)[0]);
        Assert.Equal("Value12", tree.Search(12)[0]);
        Assert.Equal("Value20", tree.Search(20)[0]);
    }

    [Fact]
    public void DataPersistence_CanRetrieveAfterRecreatingTree()
    {
        // Arrange
        var tree = new BPlusTreeIndex<int, string>(_testIndexPath, _testDataPath, _defaultDegree);
        tree.Insert(1, "Persistent");

        // Act
        var newTree = new BPlusTreeIndex<int, string>(_testIndexPath, _testDataPath, _defaultDegree);
        var result = newTree.Search(1);

        // Assert
        Assert.Single(result);
        Assert.Equal("Persistent", result[0]);
    }

    [Fact]
    public void Height_GrowsIncreasesPredictably_WithInsertions()
    {
        // Arrange
        var tree = new BPlusTreeIndex<int, string>(_testIndexPath, _testDataPath, _defaultDegree);

        // Act & Assert
        Assert.Equal(0, tree.Height());

        // Act
        for (var i = 1; i <= 3; i++)
            tree.Insert(i, $"Value{i}");

        // Assert
        Assert.Equal(1, tree.Height());

        // Act
        for (var i = 4; i <= 6; i++)
            tree.Insert(i, $"Value{i}");

        // Assert
        Assert.Equal(2, tree.Height());

        // Act
        for (var i = 7; i <= 15; i++)
            tree.Insert(i, $"Value{i}");

        // Assert
        Assert.Equal(3, tree.Height());
    }

    [Fact]
    public void Insert_ReverseOrder_MaintainsCorrectStructure()
    {
        // Arrange
        var tree = new BPlusTreeIndex<int, string>(_testIndexPath, _testDataPath, _defaultDegree);

        // Act
        for (var i = 20; i > 0; i--)
            tree.Insert(i, $"Value{i}");

        // Assert
        for (var i = 1; i <= 20; i++)
            Assert.Equal($"Value{i}", tree.Search(i)[0]);
    }

    [Fact]
    public void Insert_NonSequentialKeys_MaintainsCorrectOrder()
    {
        // Arrange
        var tree = new BPlusTreeIndex<int, string>(_testIndexPath, _testDataPath, _defaultDegree);

        // Act
        var keys = new[] { 50, 25, 75, 12, 37, 62, 87 };
        foreach (var key in keys)
            tree.Insert(key, $"Value{key}");

        // Assert
        foreach (var key in keys.OrderBy(k => k))
            Assert.Equal($"Value{key}", tree.Search(key)[0]);
    }

    [Fact]
    public void Insert_NegativeKeys_WorksCorrectly()
    {
        // Arrange
        var tree = new BPlusTreeIndex<int, string>(_testIndexPath, _testDataPath, _defaultDegree);

        // Act
        tree.Insert(-10, "Negative10");
        tree.Insert(-5, "Negative5");
        tree.Insert(0, "Zero");
        tree.Insert(5, "Positive5");

        // Assert
        Assert.Equal("Negative10", tree.Search(-10)[0]);
        Assert.Equal("Negative5", tree.Search(-5)[0]);
        Assert.Equal("Zero", tree.Search(0)[0]);
        Assert.Equal("Positive5", tree.Search(5)[0]);
    }

    [Fact]
    public void WithStringKeys_WorksCorrectly()
    {
        // Arrange
        const string indexPath = "string_index.txt";
        const string dataPath = "string_data.csv";
        try
        {
            var tree = new BPlusTreeIndex<string, int>(indexPath, dataPath, _defaultDegree);

            // Act
            tree.Insert("apple", 1);
            tree.Insert("banana", 2);
            tree.Insert("cherry", 3);
            tree.Insert("date", 4);

            // Assert
            Assert.Equal(1, tree.Search("apple")[0]);
            Assert.Equal(2, tree.Search("banana")[0]);
            Assert.Equal(3, tree.Search("cherry")[0]);
            Assert.Equal(4, tree.Search("date")[0]);
            Assert.Empty(tree.Search("elderberry"));
        }
        finally
        {
            // Clean up
            if (File.Exists(indexPath)) File.Delete(indexPath);
            if (File.Exists(dataPath)) File.Delete(dataPath);
        }
    }

    [Fact]
    public void WithDoubleKeys_WorksCorrectly()
    {
        // Arrange
        const string indexPath = "double_index.txt";
        const string dataPath = "double_data.csv";
        try
        {
            var tree = new BPlusTreeIndex<double, string>(indexPath, dataPath, _defaultDegree);

            // Act
            tree.Insert(1.5, "OnePointFive");
            tree.Insert(2.25, "TwoPointTwoFive");
            tree.Insert(3.75, "ThreePointSevenFive");

            // Assert
            Assert.Equal("OnePointFive", tree.Search(1.5)[0]);
            Assert.Equal("TwoPointTwoFive", tree.Search(2.25)[0]);
            Assert.Equal("ThreePointSevenFive", tree.Search(3.75)[0]);
        }
        finally
        {
            // Clean up
            if (File.Exists(indexPath)) File.Delete(indexPath);
            if (File.Exists(dataPath)) File.Delete(dataPath);
        }
    }

    [Fact]
    public void WithComplexValueType_WorksCorrectly()
    {
        // Arrange
        const string indexPath = "complex_index.txt";
        const string dataPath = "complex_data.csv";
        try
        {
            var tree = new BPlusTreeIndex<int, TestRecord>(indexPath, dataPath, _defaultDegree);

            // Act
            tree.Insert(1, new TestRecord { Id = 1, Name = "Alice" });
            tree.Insert(2, new TestRecord { Id = 2, Name = "Bob" });

            // Assert
            var result1 = tree.Search(1)[0];
            Assert.Equal(1, result1.Id);
            Assert.Equal("Alice", result1.Name);

            var result2 = tree.Search(2)[0];
            Assert.Equal(2, result2.Id);
            Assert.Equal("Bob", result2.Name);
        }
        finally
        {
            // Clean up
            if (File.Exists(indexPath)) File.Delete(indexPath);
            if (File.Exists(dataPath)) File.Delete(dataPath);
        }
    }

    [Fact]
    public void LargeDataset_PerformsCorrectly()
    {
        // Arrange
        var tree = new BPlusTreeIndex<int, string>(_testIndexPath, _testDataPath, _defaultDegree);

        // Act
        for (var i = 1; i <= 100; i++)
            tree.Insert(i, $"Value{i}");

        // Assert
        Assert.Equal("Value25", tree.Search(25)[0]);
        Assert.Equal("Value50", tree.Search(50)[0]);
        Assert.Equal("Value75", tree.Search(75)[0]);
        Assert.Equal("Value100", tree.Search(100)[0]);
    }

    [Fact]
    public void CustomDegree_AffectsTreeStructure()
    {
        // Arrange
        var smallDegreeTree = new BPlusTreeIndex<int, string>("small_degree.txt", "small_degree_data.csv", 3);
        var largeDegreeTree = new BPlusTreeIndex<int, string>("large_degree.txt", "large_degree_data.csv", 10);

        try
        {
            // Act
            for (var i = 1; i <= 30; i++)
            {
                smallDegreeTree.Insert(i, $"Value{i}");
                largeDegreeTree.Insert(i, $"Value{i}");
            }

            // Assert
            Assert.True(smallDegreeTree.Height() > largeDegreeTree.Height());

            Assert.Equal("Value15", smallDegreeTree.Search(15)[0]);
            Assert.Equal("Value15", largeDegreeTree.Search(15)[0]);
        }
        finally
        {
            // Clean up
            if (File.Exists("small_degree.txt")) File.Delete("small_degree.txt");
            if (File.Exists("small_degree_data.csv")) File.Delete("small_degree_data.csv");
            if (File.Exists("large_degree.txt")) File.Delete("large_degree.txt");
            if (File.Exists("large_degree_data.csv")) File.Delete("large_degree_data.csv");
        }
    }

    [Fact]
    public void MinimumDegree_WorksCorrectly()
    {
        // Arrange
        var tree = new BPlusTreeIndex<int, string>("min_degree.txt", "min_degree_data.csv", 2);

        try
        {
            // Act
            for (var i = 1; i <= 10; i++)
                tree.Insert(i, $"Value{i}");

            // Assert
            for (var i = 1; i <= 10; i++)
                Assert.Equal($"Value{i}", tree.Search(i)[0]);
        }
        finally
        {
            // Clean up
            if (File.Exists("min_degree.txt")) File.Delete("min_degree.txt");
            if (File.Exists("min_degree_data.csv")) File.Delete("min_degree_data.csv");
        }
    }

    public class TestRecord : IParsable<TestRecord>
    {
        public int Id { get; init; }
        public required string Name { get; init; }

        public static TestRecord Parse(string s, IFormatProvider? provider)
        {
            var parts = s.Split(',');
            return new TestRecord { Id = int.Parse(parts[0]), Name = parts[1] };
        }

        public static bool TryParse(string? s, IFormatProvider? provider, out TestRecord result)
        {
            try
            {
                result = Parse(s ?? string.Empty, provider);
                return true;
            }
            catch
            {
                result = null!;
                return false;
            }
        }

        public override string ToString()
        {
            return $"{Id},{Name}";
        }
    }
}
