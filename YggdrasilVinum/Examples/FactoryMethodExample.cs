using System.Text;
using YggdrasilVinum.Models;
using YggdrasilVinum.Parsers;
using YggdrasilVinum.Services;

namespace YggdrasilVinum.Examples;

/// <summary>
/// Example demonstrating the Factory Method pattern usage
/// </summary>
public static class FactoryMethodExample
{
    /// <summary>
    /// Demonstrates how to use different factory implementations
    /// </summary>
    public static void DemonstrateFactoryMethodPattern()
    {
        Console.WriteLine("=== Factory Method Pattern Demonstration ===\n");

        Console.WriteLine("1. Factory Method Pattern Structure:");
        Console.WriteLine("   AbstractCommandProcessorFactory (abstract class)");
        Console.WriteLine("   ├── StandardCommandProcessorFactory (uses DI)");
        Console.WriteLine("   └── TestCommandProcessorFactory (direct instantiation)");

        Console.WriteLine("\n2. Pattern Benefits:");
        Console.WriteLine("✓ Encapsulation: Object creation logic is hidden from client");
        Console.WriteLine("✓ Flexibility: Easy to add new processor types");
        Console.WriteLine("✓ Testability: Different factories for different scenarios");
        Console.WriteLine("✓ Maintainability: Changes to object creation are centralized");

        Console.WriteLine("\n3. Supported Command Types:");
        foreach (var commandType in Enum.GetValues<CommandParser.CommandType>())
        {
            Console.WriteLine($"   - {commandType}");
        }

        Console.WriteLine("\n4. Factory Method Implementation:");
        Console.WriteLine("   Each factory implements CreateProcessor(CommandType type)");
        Console.WriteLine("   Returns appropriate ICommandProcessor implementation");
        Console.WriteLine("   Throws NotSupportedException for unsupported types");
    }

    /// <summary>
    /// Demonstrates the factory method pattern concept without actual execution
    /// </summary>
    public static void DemonstrateFactoryMethodConcept()
    {
        Console.WriteLine("\n=== Factory Method Pattern Concept ===\n");

        Console.WriteLine("1. Abstract Factory Method:");
        Console.WriteLine("   public abstract ICommandProcessor CreateProcessor(CommandType type);");

        Console.WriteLine("\n2. Concrete Implementation Example (StandardCommandProcessorFactory):");
        Console.WriteLine("   return type switch");
        Console.WriteLine("   {");
        Console.WriteLine("       CommandType.Insert => serviceProvider.GetRequiredService<InsertCommandProcessor>(),");
        Console.WriteLine("       CommandType.Search => serviceProvider.GetRequiredService<SearchCommandProcessor>(),");
        Console.WriteLine("       _ => throw new NotSupportedException()");
        Console.WriteLine("   };");

        Console.WriteLine("\n3. Alternative Implementation (TestCommandProcessorFactory):");
        Console.WriteLine("   return type switch");
        Console.WriteLine("   {");
        Console.WriteLine("       CommandType.Insert => new InsertCommandProcessor(database, harvestProcessor),");
        Console.WriteLine("       CommandType.Search => new SearchCommandProcessor(database),");
        Console.WriteLine("       _ => throw new NotSupportedException()");
        Console.WriteLine("   };");

        Console.WriteLine("\n4. Client Usage:");
        Console.WriteLine("   var processor = factory.CreateProcessor(CommandType.Insert);");
        Console.WriteLine("   var result = await processor.ExecuteAsync(command, output);");
    }

    /// <summary>
    /// Demonstrates how to extend the factory pattern
    /// </summary>
    public static void DemonstrateExtensibility()
    {
        Console.WriteLine("\n=== Extending the Factory Pattern ===\n");

        Console.WriteLine("To add a new command type (e.g., 'Update'):");
        Console.WriteLine("1. Add CommandType.Update to the enum");
        Console.WriteLine("2. Create UpdateCommandProcessor implementing ICommandProcessor");
        Console.WriteLine("3. Update factory implementations:");
        Console.WriteLine("   CommandType.Update => new UpdateCommandProcessor(...),");
        Console.WriteLine("4. Register in DI container if using StandardCommandProcessorFactory");

        Console.WriteLine("\nTo add a new factory implementation:");
        Console.WriteLine("1. Inherit from AbstractCommandProcessorFactory");
        Console.WriteLine("2. Implement CreateProcessor method");
        Console.WriteLine("3. Handle object creation based on your requirements");
        Console.WriteLine("   (e.g., configuration-based, caching, etc.)");
    }
}

/// <summary>
/// Console application entry point for demonstration
/// </summary>
public static class Program
{
    public static void Main(string[] args)
    {
        try
        {
            FactoryMethodExample.DemonstrateFactoryMethodPattern();
            FactoryMethodExample.DemonstrateFactoryMethodConcept();
            FactoryMethodExample.DemonstrateExtensibility();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error running demonstration: {ex.Message}");
        }
    }
}
