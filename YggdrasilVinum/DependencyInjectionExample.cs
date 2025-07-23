using Microsoft.Extensions.DependencyInjection;
using YggdrasilVinum.Services;

namespace YggdrasilVinum;

/// <summary>
///     Example class showing how DI makes testing easier by allowing dependency substitution
/// </summary>
public static class DependencyInjectionExample
{
    /// <summary>
    ///     Demonstrates how to configure services for testing with mock dependencies
    /// </summary>
    public static IServiceProvider ConfigureTestServices(ApplicationConfiguration configuration)
    {
        var services = new ServiceCollection();

        // In tests, you can easily substitute real implementations with mocks
        // For example, instead of using the real file manager you could use a mock implementation

        // Or use the real configuration but with test-specific settings:
        var testConfiguration = new ApplicationConfiguration
        {
            StoragePath = "./test_storage",
            HeapSizeInBytes = 1024 * 1024, // Smaller heap for tests
            PageSizeInBytes = 512, // Smaller pages for tests
            AmountOfPageFrames = 1,
            AmountOfIndexFrames = 1,
            IndexPath = "./test_storage/test_index.txt",
            MaxNumberOfKeysPerNode = 2, // Smaller tree for tests
            ProcessedWinesPath = "./test_storage/test_processed_wines.txt"
        };

        services.ConfigureApplicationServices(testConfiguration);

        return services.BuildServiceProvider();
    }

    /// <summary>
    ///     Example of how to get a specific service for testing
    /// </summary>
    public static T GetTestService<T>(IServiceProvider serviceProvider) where T : notnull
    {
        return serviceProvider.GetRequiredService<T>();
    }
}
