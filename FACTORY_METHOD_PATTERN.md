# Factory Method Pattern Implementation

## Overview

This project implements the Factory Method pattern to provide a flexible way to create command processors. The pattern
allows for the creation of objects without specifying their exact class, promoting loose coupling and extensibility.

## Pattern Structure

### Abstract Factory

```csharp
public abstract class AbstractCommandProcessorFactory
{
    public abstract ICommandProcessor CreateProcessor(CommandParser.CommandType type);
}
```

### Concrete Implementations

#### StandardCommandProcessorFactory

Uses dependency injection to create command processors:

```csharp
public class StandardCommandProcessorFactory : AbstractCommandProcessorFactory
{
    private readonly IServiceProvider _serviceProvider;

    public StandardCommandProcessorFactory(IServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider;
    }

    public override ICommandProcessor CreateProcessor(CommandParser.CommandType type)
    {
        return type switch
        {
            CommandParser.CommandType.Insert => _serviceProvider.GetRequiredService<InsertCommandProcessor>(),
            CommandParser.CommandType.Search => _serviceProvider.GetRequiredService<SearchCommandProcessor>(),
            _ => throw new NotSupportedException($"Command type '{type}' is not supported.")
        };
    }
}
```

#### TestCommandProcessorFactory

Creates command processors directly for testing scenarios:

```csharp
public class TestCommandProcessorFactory : AbstractCommandProcessorFactory
{
    private readonly Database _database;
    private readonly HarvestYearSearchProcessor _harvestYearSearchProcessor;

    public TestCommandProcessorFactory(Database database, HarvestYearSearchProcessor harvestYearSearchProcessor)
    {
        _database = database;
        _harvestYearSearchProcessor = harvestYearSearchProcessor;
    }

    public override ICommandProcessor CreateProcessor(CommandParser.CommandType type)
    {
        return type switch
        {
            CommandParser.CommandType.Insert => new InsertCommandProcessor(_database, _harvestYearSearchProcessor),
            CommandParser.CommandType.Search => new SearchCommandProcessor(_database),
            _ => throw new NotSupportedException($"Command type '{type}' is not supported.")
        };
    }
}
```

## Benefits

### 1. Encapsulation

The factory encapsulates the complex object creation logic, hiding dependencies from client code.

### 2. Flexibility

Easy to introduce new types of command processors without modifying existing client code.

### 3. Testability

Different factory implementations can be used for testing vs. production scenarios.

### 4. Dependency Management

The StandardCommandProcessorFactory integrates seamlessly with the dependency injection container.

## Usage Examples

### Basic Usage

```csharp
// Using dependency injection
var serviceProvider = // ... configure services
var factory = new StandardCommandProcessorFactory(serviceProvider);
var processor = factory.CreateProcessor(CommandParser.CommandType.Insert);

// Using direct instantiation for testing
var database = // ... create database
var harvestProcessor = // ... create harvest processor
var testFactory = new TestCommandProcessorFactory(database, harvestProcessor);
var testProcessor = testFactory.CreateProcessor(CommandParser.CommandType.Search);
```

### Processing Commands with Factory

```csharp
public static async Task<Result<Unit, CommandProcessorError>> ProcessCommandUsingFactory(
    AbstractCommandProcessorFactory factory,
    CommandParser.Command command,
    StringBuilder outputContent)
{
    try
    {
        var processor = factory.CreateProcessor(command.Type);
        return await processor.ExecuteAsync(command, outputContent);
    }
    catch (NotSupportedException ex)
    {
        return Result<Unit, CommandProcessorError>.Error(
            new CommandProcessorError($"Factory method failed: {ex.Message}")
        );
    }
}
```

## Dependency Injection Configuration

The pattern is registered in the DI container in `ServiceCollectionExtensions.cs`:

```csharp
// Register Factory Method pattern implementations
services.AddSingleton<AbstractCommandProcessorFactory, StandardCommandProcessorFactory>();

services.AddTransient<TestCommandProcessorFactory>(provider =>
{
    var database = provider.GetRequiredService<Database>();
    var harvestYearSearchProcessor = provider.GetRequiredService<HarvestYearSearchProcessor>();
    return new TestCommandProcessorFactory(database, harvestYearSearchProcessor);
});
```

## Testing

The implementation includes comprehensive unit tests that verify:

- Factory construction with null parameters throws appropriate exceptions
- Correct processor types are returned for each command type
- Unsupported command types throw NotSupportedException
- Both factory implementations inherit from the abstract base class
- Polymorphic behavior works correctly
- Object creation is properly encapsulated

## Future Extensibility

To add a new command type:

1. Add the new enum value to `CommandParser.CommandType`
2. Create the new command processor implementing `ICommandProcessor`
3. Update both factory implementations to handle the new type
4. Register the new processor in the DI container

The Factory Method pattern makes this extension process straightforward and maintainable.
