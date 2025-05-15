using YggdrasilVinum.Index;
using YggdrasilVinum.Models;

namespace YggdrasilVinum.Tests.Unit.Index;

public class BPlusTreeIndexTest : IDisposable
{
    private const string _testIndexPath = "test_index.txt";
    private const int _defaultDegree = 3;

    public void Dispose()
    {
        if (File.Exists(_testIndexPath))
            File.Delete(_testIndexPath);
    }

    private static async Task<TValue> UnwrapResult<TValue, TError>(Task<Result<TValue, TError>> resultTask)
    {
        var result = await resultTask;
        return result.GetValueOrThrow();
    }

    [Fact]
    public void Constructor_CreatesNewFiles_WhenNotExist()
    {
        // Act
        _ = new BPlusTreeIndex<int>(_testIndexPath, _defaultDegree);

        // Assert
        Assert.True(File.Exists(_testIndexPath));
    }

    [Fact]
    public async Task Search_EmptyTree_ReturnsEmptyList()
    {
        // Arrange
        var tree = new BPlusTreeIndex<int>(_testIndexPath, _defaultDegree);

        // Act
        var result = await UnwrapResult(tree.SearchAsync(1));

        // Assert
        Assert.Empty(result);
    }

    [Fact]
    public async Task Insert_SingleValue_CanBeRetrieved()
    {
        // Arrange
        var tree = new BPlusTreeIndex<int>(_testIndexPath, _defaultDegree);

        // Act
        ulong pageId = 12345UL;
        await tree.InsertAsync(5, pageId);
        var result = await UnwrapResult(tree.SearchAsync(5));

        // Assert
        Assert.Single(result);
        Assert.Equal(pageId, result[0]);
    }

    [Fact]
    public async Task Insert_MultipleValues_CanBeRetrieved()
    {
        // Arrange
        var tree = new BPlusTreeIndex<int>(_testIndexPath, _defaultDegree);

        // Act
        await tree.InsertAsync(1, 100UL);
        await tree.InsertAsync(2, 200UL);
        await tree.InsertAsync(3, 300UL);

        // Assert
        Assert.Equal(100UL, (await UnwrapResult(tree.SearchAsync(1)))[0]);
        Assert.Equal(200UL, (await UnwrapResult(tree.SearchAsync(2)))[0]);
        Assert.Equal(300UL, (await UnwrapResult(tree.SearchAsync(3)))[0]);
    }

    [Fact]
    public async Task Insert_DuplicateKeys_StoresAllValues()
    {
        // Arrange
        var tree = new BPlusTreeIndex<int>(_testIndexPath, _defaultDegree);

        // Act
        await tree.InsertAsync(10, 1000UL);
        await tree.InsertAsync(10, 2000UL);
        var result = await UnwrapResult(tree.SearchAsync(10));

        // Assert
        Assert.Equal(2, result.Count);
        Assert.Contains(1000UL, result);
        Assert.Contains(2000UL, result);
    }

    [Fact]
    public async Task Search_NonExistentKey_ReturnsEmptyList()
    {
        // Arrange
        var tree = new BPlusTreeIndex<int>(_testIndexPath, _defaultDegree);
        await tree.InsertAsync(1, 100UL);

        // Act
        var result = await UnwrapResult(tree.SearchAsync(2));

        // Assert
        Assert.Empty(result);
    }

    [Fact]
    public async Task Height_EmptyTree_ReturnsZero()
    {
        // Arrange
        var tree = new BPlusTreeIndex<int>(_testIndexPath, _defaultDegree);

        // Act & Assert
        Assert.Equal(0, await UnwrapResult(tree.HeightAsync()));
    }

    [Fact]
    public async Task Height_AfterSplits_IncreasesCorrectly()
    {
        // Arrange
        var tree = new BPlusTreeIndex<int>(_testIndexPath, _defaultDegree);

        // Act
        for (var i = 1; i <= 10; i++) await tree.InsertAsync(i, (ulong)i * 100);

        // Assert
        Assert.True(await UnwrapResult(tree.HeightAsync()) > 0);
    }

    [Fact]
    public async Task Insert_ManyValues_MaintainsCorrectOrder()
    {
        // Arrange
        var tree = new BPlusTreeIndex<int>(_testIndexPath, _defaultDegree);

        // Act
        await tree.InsertAsync(8, 800UL);
        await tree.InsertAsync(3, 300UL);
        await tree.InsertAsync(1, 100UL);
        await tree.InsertAsync(6, 600UL);
        await tree.InsertAsync(4, 400UL);

        // Assert
        Assert.Equal(100UL, (await UnwrapResult(tree.SearchAsync(1)))[0]);
        Assert.Equal(300UL, (await UnwrapResult(tree.SearchAsync(3)))[0]);
        Assert.Equal(400UL, (await UnwrapResult(tree.SearchAsync(4)))[0]);
        Assert.Equal(600UL, (await UnwrapResult(tree.SearchAsync(6)))[0]);
        Assert.Equal(800UL, (await UnwrapResult(tree.SearchAsync(8)))[0]);
    }

    [Fact]
    public async Task Insert_EnoughValuesToCauseSplits_CanStillRetrieveAll()
    {
        // Arrange
        var tree = new BPlusTreeIndex<int>(_testIndexPath, _defaultDegree);

        // Act
        for (var i = 1; i <= 20; i++) await tree.InsertAsync(i, (ulong)i * 100);

        // Assert
        Assert.Equal(500UL, (await UnwrapResult(tree.SearchAsync(5)))[0]);
        Assert.Equal(1200UL, (await UnwrapResult(tree.SearchAsync(12)))[0]);
        Assert.Equal(2000UL, (await UnwrapResult(tree.SearchAsync(20)))[0]);
    }

    [Fact]
    public async Task DataPersistence_CanRetrieveAfterRecreatingTree()
    {
        // Arrange
        var tree = new BPlusTreeIndex<int>(_testIndexPath, _defaultDegree);
        await tree.InsertAsync(1, 12345UL);

        // Act
        var newTree = new BPlusTreeIndex<int>(_testIndexPath, _defaultDegree);
        var result = await UnwrapResult(newTree.SearchAsync(1));

        // Assert
        Assert.Single(result);
        Assert.Equal(12345UL, result[0]);
    }

    [Fact]
    public async Task Height_GrowsIncreasesPredictably_WithInsertions()
    {
        // Arrange
        var tree = new BPlusTreeIndex<int>(_testIndexPath, _defaultDegree);

        // Act & Assert
        Assert.Equal(0, await UnwrapResult(tree.HeightAsync()));

        // Act
        for (var i = 1; i <= 3; i++)
            await tree.InsertAsync(i, (ulong)i * 100);

        // Assert
        Assert.Equal(1, await UnwrapResult(tree.HeightAsync()));

        // Act
        for (var i = 4; i <= 6; i++)
            await tree.InsertAsync(i, (ulong)i * 100);

        // Assert
        Assert.Equal(2, await UnwrapResult(tree.HeightAsync()));

        // Act
        for (var i = 7; i <= 15; i++)
            await tree.InsertAsync(i, (ulong)i * 100);

        // Assert
        Assert.Equal(3, await UnwrapResult(tree.HeightAsync()));
    }

    [Fact]
    public async Task Insert_ReverseOrder_MaintainsCorrectStructure()
    {
        // Arrange
        var tree = new BPlusTreeIndex<int>(_testIndexPath, _defaultDegree);

        // Act
        for (var i = 20; i > 0; i--)
            await tree.InsertAsync(i, (ulong)i * 100);

        // Assert
        for (var i = 1; i <= 20; i++)
            Assert.Equal((ulong)i * 100, (await UnwrapResult(tree.SearchAsync(i)))[0]);
    }

    [Fact]
    public async Task Insert_NonSequentialKeys_MaintainsCorrectOrder()
    {
        // Arrange
        var tree = new BPlusTreeIndex<int>(_testIndexPath, _defaultDegree);

        // Act
        var keys = new[] { 50, 25, 75, 12, 37, 62, 87 };
        foreach (var key in keys)
            await tree.InsertAsync(key, (ulong)key * 100);

        // Assert
        foreach (var key in keys.OrderBy(k => k))
            Assert.Equal((ulong)key * 100, (await UnwrapResult(tree.SearchAsync(key)))[0]);
    }

    [Fact]
    public async Task Insert_NegativeKeys_WorksCorrectly()
    {
        // Arrange
        var tree = new BPlusTreeIndex<int>(_testIndexPath, _defaultDegree);

        // Act
        await tree.InsertAsync(-10, 1000UL);
        await tree.InsertAsync(-5, 500UL);
        await tree.InsertAsync(0, 0UL);
        await tree.InsertAsync(5, 500UL);

        // Assert
        Assert.Equal(1000UL, (await UnwrapResult(tree.SearchAsync(-10)))[0]);
        Assert.Equal(500UL, (await UnwrapResult(tree.SearchAsync(-5)))[0]);
        Assert.Equal(0UL, (await UnwrapResult(tree.SearchAsync(0)))[0]);
        Assert.Equal(500UL, (await UnwrapResult(tree.SearchAsync(5)))[0]);
    }

    [Fact]
    public async Task WithStringKeys_WorksCorrectly()
    {
        // Arrange
        const string indexPath = "string_index.txt";
        try
        {
            var tree = new BPlusTreeIndex<string>(indexPath, _defaultDegree);

            // Act
            await tree.InsertAsync("apple", 1UL);
            await tree.InsertAsync("banana", 2UL);
            await tree.InsertAsync("cherry", 3UL);
            await tree.InsertAsync("date", 4UL);

            // Assert
            Assert.Equal(1UL, (await UnwrapResult(tree.SearchAsync("apple")))[0]);
            Assert.Equal(2UL, (await UnwrapResult(tree.SearchAsync("banana")))[0]);
            Assert.Equal(3UL, (await UnwrapResult(tree.SearchAsync("cherry")))[0]);
            Assert.Equal(4UL, (await UnwrapResult(tree.SearchAsync("date")))[0]);
            Assert.Empty(await UnwrapResult(tree.SearchAsync("elderberry")));
        }
        finally
        {
            // Clean up
            if (File.Exists(indexPath)) File.Delete(indexPath);
        }
    }

    [Fact]
    public async Task WithDoubleKeys_WorksCorrectly()
    {
        // Arrange
        const string indexPath = "double_index.txt";
        try
        {
            var tree = new BPlusTreeIndex<double>(indexPath, _defaultDegree);

            // Act
            await tree.InsertAsync(1.5, 15UL);
            await tree.InsertAsync(2.25, 225UL);
            await tree.InsertAsync(3.75, 375UL);

            // Assert
            Assert.Equal(15UL, (await UnwrapResult(tree.SearchAsync(1.5)))[0]);
            Assert.Equal(225UL, (await UnwrapResult(tree.SearchAsync(2.25)))[0]);
            Assert.Equal(375UL, (await UnwrapResult(tree.SearchAsync(3.75)))[0]);
        }
        finally
        {
            // Clean up
            if (File.Exists(indexPath)) File.Delete(indexPath);
        }
    }

    [Fact]
    public async Task PageId_CorrectlyStored()
    {
        // Arrange
        const string indexPath = "pageid_index.txt";
        try
        {
            var tree = new BPlusTreeIndex<int>(indexPath, _defaultDegree);

            // Act
            ulong maxPageId = ulong.MaxValue;
            ulong minPageId = ulong.MinValue;
            await tree.InsertAsync(1, maxPageId);
            await tree.InsertAsync(2, minPageId);

            // Assert
            Assert.Equal(maxPageId, (await UnwrapResult(tree.SearchAsync(1)))[0]);
            Assert.Equal(minPageId, (await UnwrapResult(tree.SearchAsync(2)))[0]);
        }
        finally
        {
            // Clean up
            if (File.Exists(indexPath)) File.Delete(indexPath);
        }
    }

    [Fact]
    public async Task Insert_LargePageIds_HandledCorrectly()
    {
        // Arrange
        var tree = new BPlusTreeIndex<int>(_testIndexPath, _defaultDegree);
        
        // Act
        ulong largePageId1 = 18446744073709551000; // Close to ulong.MaxValue
        ulong largePageId2 = 9223372036854775000;  // Large value
        
        await tree.InsertAsync(42, largePageId1);
        await tree.InsertAsync(43, largePageId2);
        
        // Assert
        Assert.Equal(largePageId1, (await UnwrapResult(tree.SearchAsync(42)))[0]);
        Assert.Equal(largePageId2, (await UnwrapResult(tree.SearchAsync(43)))[0]);
    }

    [Fact]
    public async Task LargeDataset_PerformsCorrectly()
    {
        // Arrange
        var tree = new BPlusTreeIndex<int>(_testIndexPath, _defaultDegree);

        // Act
        for (var i = 1; i <= 100; i++)
            await tree.InsertAsync(i, (ulong)i * 100);

        // Assert
        Assert.Equal(2500UL, (await UnwrapResult(tree.SearchAsync(25)))[0]);
        Assert.Equal(5000UL, (await UnwrapResult(tree.SearchAsync(50)))[0]);
        Assert.Equal(7500UL, (await UnwrapResult(tree.SearchAsync(75)))[0]);
        Assert.Equal(10000UL, (await UnwrapResult(tree.SearchAsync(100)))[0]);
    }

    [Fact]
    public async Task CustomDegree_AffectsTreeStructure()
    {
        // Arrange
        var smallDegreeTree = new BPlusTreeIndex<int>("small_degree.txt", 3);
        var largeDegreeTree = new BPlusTreeIndex<int>("large_degree.txt", 10);

        try
        {
            // Act
            for (var i = 1; i <= 30; i++)
            {
                await smallDegreeTree.InsertAsync(i, (ulong)i * 100);
                await largeDegreeTree.InsertAsync(i, (ulong)i * 100);
            }

            // Assert
            Assert.True(await UnwrapResult(smallDegreeTree.HeightAsync()) > await UnwrapResult(largeDegreeTree.HeightAsync()));

            Assert.Equal(1500UL, (await UnwrapResult(smallDegreeTree.SearchAsync(15)))[0]);
            Assert.Equal(1500UL, (await UnwrapResult(largeDegreeTree.SearchAsync(15)))[0]);
        }
        finally
        {
            // Clean up
            if (File.Exists("small_degree.txt")) File.Delete("small_degree.txt");
            if (File.Exists("large_degree.txt")) File.Delete("large_degree.txt");
        }
    }

    [Fact]
    public async Task MinimumDegree_WorksCorrectly()
    {
        // Arrange
        var tree = new BPlusTreeIndex<int>("min_degree.txt", 2);

        try
        {
            // Act
            for (var i = 1; i <= 10; i++)
                await tree.InsertAsync(i, (ulong)i * 100);

            // Assert
            for (var i = 1; i <= 10; i++)
                Assert.Equal((ulong)i * 100, (await UnwrapResult(tree.SearchAsync(i)))[0]);
        }
        finally
        {
            // Clean up
            if (File.Exists("min_degree.txt")) File.Delete("min_degree.txt");
        }
    }

    // TestRecord class removed as the BPlusTreeIndex now only stores page IDs
    // and not complex value types directly
}
