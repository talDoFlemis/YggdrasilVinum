using Microsoft.Extensions.DependencyInjection;
using YggdrasilVinum.Buffer;
using YggdrasilVinum.Index;
using YggdrasilVinum.Models;
using YggdrasilVinum.Storage;

namespace YggdrasilVinum.Services;

/// <summary>
///     Extension methods for configuring dependency injection
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    ///     Configures all application services for dependency injection
    /// </summary>
    public static IServiceCollection ConfigureApplicationServices(
        this IServiceCollection services,
        ApplicationConfiguration configuration)
    {
        // Register configuration
        services.AddSingleton(configuration);

        // Configure core storage services
        services.AddSingleton<IFileManager>(provider =>
            new SequentialHeapFileManager(configuration.StoragePath, configuration.HeapSizeInBytes,
                configuration.PageSizeInBytes));

        services.AddSingleton<IBufferManager>(provider =>
        {
            var fileManager = provider.GetRequiredService<IFileManager>();
            return new LruBufferManager(fileManager, configuration.AmountOfPageFrames,
                configuration.AmountOfIndexFrames);
        });

        // Configure B+ Tree index
        services.AddSingleton<IBPlusTreeIndex<int, RID>>(provider =>
            new BPlusTreeIndex<int, RID>(configuration.IndexPath, configuration.MaxNumberOfKeysPerNode));

        // Configure processors
        services.AddSingleton<InsertProcessor>(provider =>
        {
            var bufferManager = provider.GetRequiredService<IBufferManager>();
            var fileManager = provider.GetRequiredService<IFileManager>();
            var bPlusTree = provider.GetRequiredService<IBPlusTreeIndex<int, RID>>();
            return new InsertProcessor(bufferManager, fileManager, bPlusTree);
        });

        services.AddSingleton<EqualitySearchProcessor>(provider =>
        {
            var bufferManager = provider.GetRequiredService<IBufferManager>();
            var bPlusTree = provider.GetRequiredService<IBPlusTreeIndex<int, RID>>();
            return new EqualitySearchProcessor(bufferManager, bPlusTree);
        });

        // Configure wine processor
        services.AddSingleton<IWineProcessor>(provider =>
            new WineProcessor(configuration.ProcessedWinesPath));

        services.AddSingleton<HarvestYearSearchProcessor>(provider =>
        {
            var wineProcessor = provider.GetRequiredService<IWineProcessor>();
            return new HarvestYearSearchProcessor(wineProcessor);
        });

        // Configure database
        services.AddSingleton<Database>(provider =>
        {
            var insertProcessor = provider.GetRequiredService<InsertProcessor>();
            var equalityProcessor = provider.GetRequiredService<EqualitySearchProcessor>();
            return new Database(insertProcessor, equalityProcessor);
        });

        // Configure command processors
        services.AddTransient<InsertCommandProcessor>(provider =>
        {
            var database = provider.GetRequiredService<Database>();
            var harvestYearSearchProcessor = provider.GetRequiredService<HarvestYearSearchProcessor>();
            return new InsertCommandProcessor(database, harvestYearSearchProcessor);
        });

        services.AddTransient<SearchCommandProcessor>(provider =>
        {
            var database = provider.GetRequiredService<Database>();
            return new SearchCommandProcessor(database);
        });

        services.AddSingleton<CommandProcessorFactory>(provider =>
        {
            var database = provider.GetRequiredService<Database>();
            var harvestYearSearchProcessor = provider.GetRequiredService<HarvestYearSearchProcessor>();
            return new CommandProcessorFactory(database, harvestYearSearchProcessor);
        });

        // Register Factory Method pattern implementations
        services.AddSingleton<AbstractCommandProcessorFactory, StandardCommandProcessorFactory>();

        services.AddTransient<TestCommandProcessorFactory>(provider =>
        {
            var database = provider.GetRequiredService<Database>();
            var harvestYearSearchProcessor = provider.GetRequiredService<HarvestYearSearchProcessor>();
            return new TestCommandProcessorFactory(database, harvestYearSearchProcessor);
        });

        return services;
    }

    /// <summary>
    ///     Initializes all services that require async initialization
    /// </summary>
    public static async Task<Result<Unit, string>> InitializeServicesAsync(this IServiceProvider serviceProvider)
    {
        try
        {
            // Initialize file manager
            var fileManager = serviceProvider.GetRequiredService<IFileManager>();
            var fileManagerResult = await fileManager.InitializeAsync();
            if (fileManagerResult.IsError)
            {
                var error = fileManagerResult.GetErrorOrThrow();
                return Result<Unit, string>.Error($"Failed to initialize file manager: {error.Message}");
            }

            // Initialize buffer manager
            var bufferManager = serviceProvider.GetRequiredService<IBufferManager>();
            var bufferManagerResult = await bufferManager.InitializeAsync();
            if (bufferManagerResult.IsError)
            {
                var error = bufferManagerResult.GetErrorOrThrow();
                return Result<Unit, string>.Error($"Failed to initialize buffer manager: {error.Message}");
            }

            // Initialize B+ Tree
            var bPlusTree = serviceProvider.GetRequiredService<IBPlusTreeIndex<int, RID>>();
            var bPlusTreeResult = await bPlusTree.InitializeAsync();
            if (bPlusTreeResult.IsError)
            {
                var error = bPlusTreeResult.GetErrorOrThrow();
                return Result<Unit, string>.Error($"Failed to initialize B+ tree: {error.Message}");
            }

            return Result<Unit, string>.Success(new Unit());
        }
        catch (Exception ex)
        {
            return Result<Unit, string>.Error($"Exception during service initialization: {ex.Message}");
        }
    }
}
