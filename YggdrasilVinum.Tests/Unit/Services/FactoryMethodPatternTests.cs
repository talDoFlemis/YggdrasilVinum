using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using Xunit;
using YggdrasilVinum.Buffer;
using YggdrasilVinum.Index;
using YggdrasilVinum.Models;
using YggdrasilVinum.Parsers;
using YggdrasilVinum.Services;
using YggdrasilVinum.Storage;

namespace YggdrasilVinum.Tests.Unit.Services;

/// <summary>
/// Unit tests for the Factory Method pattern implementation
/// </summary>
public class FactoryMethodPatternTests
{
    // Create a mock service provider for testing StandardCommandProcessorFactory
    private readonly IServiceProvider _mockServiceProvider;
    private readonly Database _testDatabase;
    private readonly HarvestYearSearchProcessor _testHarvestYearSearchProcessor;

    public FactoryMethodPatternTests()
    {
        // Setup mock services for dependency injection tests
        var services = new ServiceCollection();

        // Add mock dependencies
        var mockBufferManager = new Mock<IBufferManager>();
        var mockFileManager = new Mock<IFileManager>();
        var mockBPlusTree = new Mock<IBPlusTreeIndex<int, RID>>();
        var mockWineProcessor = new Mock<IWineProcessor>();

        // Create concrete instances for testing (we'll test factory behavior, not processor execution)
        var insertProcessor = new Mock<InsertProcessor>(
            mockBufferManager.Object,
            mockFileManager.Object,
            mockBPlusTree.Object).Object;

        var equalitySearchProcessor = new Mock<EqualitySearchProcessor>(
            mockBufferManager.Object,
            mockBPlusTree.Object).Object;

        _testDatabase = new Database(insertProcessor, equalitySearchProcessor);
        _testHarvestYearSearchProcessor = new HarvestYearSearchProcessor(mockWineProcessor.Object);

        // Register processors for StandardCommandProcessorFactory tests
        services.AddTransient<InsertCommandProcessor>(_ =>
            new InsertCommandProcessor(_testDatabase, _testHarvestYearSearchProcessor));
        services.AddTransient<SearchCommandProcessor>(_ =>
            new SearchCommandProcessor(_testDatabase));

        _mockServiceProvider = services.BuildServiceProvider();
    }

    [Fact]
    public void StandardCommandProcessorFactory_Constructor_WithNullServiceProvider_ThrowsArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new StandardCommandProcessorFactory(null!));
    }

    [Fact]
    public void TestCommandProcessorFactory_Constructor_WithNullDatabase_ThrowsArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
            new TestCommandProcessorFactory(null!, _testHarvestYearSearchProcessor));
    }

    [Fact]
    public void TestCommandProcessorFactory_Constructor_WithNullHarvestYearSearchProcessor_ThrowsArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
            new TestCommandProcessorFactory(_testDatabase, null!));
    }

    [Fact]
    public void TestCommandProcessorFactory_CreateProcessor_ForInsertCommand_ReturnsInsertCommandProcessor()
    {
        // Arrange
        var factory = new TestCommandProcessorFactory(_testDatabase, _testHarvestYearSearchProcessor);

        // Act
        var processor = factory.CreateProcessor(CommandParser.CommandType.Insert);

        // Assert
        Assert.NotNull(processor);
        Assert.IsType<InsertCommandProcessor>(processor);
    }

    [Fact]
    public void TestCommandProcessorFactory_CreateProcessor_ForSearchCommand_ReturnsSearchCommandProcessor()
    {
        // Arrange
        var factory = new TestCommandProcessorFactory(_testDatabase, _testHarvestYearSearchProcessor);

        // Act
        var processor = factory.CreateProcessor(CommandParser.CommandType.Search);

        // Assert
        Assert.NotNull(processor);
        Assert.IsType<SearchCommandProcessor>(processor);
    }

    [Fact]
    public void TestCommandProcessorFactory_CreateProcessor_ForUnsupportedCommand_ThrowsNotSupportedException()
    {
        // Arrange
        var factory = new TestCommandProcessorFactory(_testDatabase, _testHarvestYearSearchProcessor);
        var invalidCommandType = (CommandParser.CommandType)999; // Invalid enum value

        // Act & Assert
        var exception = Assert.Throws<NotSupportedException>(() => factory.CreateProcessor(invalidCommandType));
        Assert.Contains("Command type '999' is not supported.", exception.Message);
    }

    [Fact]
    public void StandardCommandProcessorFactory_CreateProcessor_WithValidServiceProvider_ReturnsCorrectProcessor()
    {
        // Arrange
        var factory = new StandardCommandProcessorFactory(_mockServiceProvider);

        // Act
        var insertProcessor = factory.CreateProcessor(CommandParser.CommandType.Insert);
        var searchProcessor = factory.CreateProcessor(CommandParser.CommandType.Search);

        // Assert
        Assert.NotNull(insertProcessor);
        Assert.IsType<InsertCommandProcessor>(insertProcessor);
        Assert.NotNull(searchProcessor);
        Assert.IsType<SearchCommandProcessor>(searchProcessor);
    }

    [Fact]
    public async Task FactoryMethodPatternDemo_ProcessCommandUsingFactory_WithUnsupportedCommand_ReturnsError()
    {
        // Arrange
        var factory = new TestCommandProcessorFactory(_testDatabase, _testHarvestYearSearchProcessor);
        var command = new CommandParser.Command((CommandParser.CommandType)999, 123);
        var outputContent = new StringBuilder();

        // Act
        var result = await FactoryMethodPatternDemo.ProcessCommandUsingFactory(factory, command, outputContent);

        // Assert
        Assert.True(result.IsError);
        var error = result.GetErrorOrThrow();
        Assert.Contains("Factory method failed", error.ToString());
    }

    [Fact]
    public void AbstractCommandProcessorFactory_IsAbstract()
    {
        // Assert
        Assert.True(typeof(AbstractCommandProcessorFactory).IsAbstract);
    }

    [Fact]
    public void StandardCommandProcessorFactory_InheritsFromAbstractCommandProcessorFactory()
    {
        // Assert
        Assert.True(typeof(StandardCommandProcessorFactory).IsSubclassOf(typeof(AbstractCommandProcessorFactory)));
    }

    [Fact]
    public void TestCommandProcessorFactory_InheritsFromAbstractCommandProcessorFactory()
    {
        // Assert
        Assert.True(typeof(TestCommandProcessorFactory).IsSubclassOf(typeof(AbstractCommandProcessorFactory)));
    }

    [Fact]
    public void FactoryMethodPattern_DemonstratesPolymorphism()
    {
        // Arrange - Create two different factories
        var testFactory = new TestCommandProcessorFactory(_testDatabase, _testHarvestYearSearchProcessor);
        var standardFactory = new StandardCommandProcessorFactory(_mockServiceProvider);

        // Act - Both factories can create processors through the same interface
        var testInsertProcessor = testFactory.CreateProcessor(CommandParser.CommandType.Insert);
        var standardInsertProcessor = standardFactory.CreateProcessor(CommandParser.CommandType.Insert);

        // Assert - Both return the same type but were created by different factory implementations
        Assert.IsType<InsertCommandProcessor>(testInsertProcessor);
        Assert.IsType<InsertCommandProcessor>(standardInsertProcessor);
        Assert.NotSame(testInsertProcessor, standardInsertProcessor); // Different instances
    }

    [Fact]
    public void FactoryMethodPattern_EncapsulatesObjectCreation()
    {
        // Arrange
        var factory = new TestCommandProcessorFactory(_testDatabase, _testHarvestYearSearchProcessor);

        // Act & Assert - The factory encapsulates the creation logic
        // Client code doesn't need to know about the dependencies required to create processors
        var insertProcessor = factory.CreateProcessor(CommandParser.CommandType.Insert);
        var searchProcessor = factory.CreateProcessor(CommandParser.CommandType.Search);

        Assert.NotNull(insertProcessor);
        Assert.NotNull(searchProcessor);
        Assert.NotSame(insertProcessor, searchProcessor);
    }
}

