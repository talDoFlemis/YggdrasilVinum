# Facade Pattern Implementation

## Overview

The Facade pattern has been implemented in the YggdrasilVinum project to simplify the interaction with the storage
subsystem. This document explains the implementation, benefits, and usage of the pattern.

## Problem Statement

### Before Facade Pattern

The original architecture required command processors to interact directly with multiple storage components:

```csharp
// Original InsertProcessor - High Coupling
public class InsertProcessor(
    IBufferManager bufferManager,      // Dependency 1
    IFileManager fileManager,          // Dependency 2
    IBPlusTreeIndex<int, RID> bPlusTree // Dependency 3
)
```

This created several problems:

- **High Coupling**: Processors were tightly coupled to three different subsystem interfaces
- **Complex Coordination**: Clients had to orchestrate multiple operations correctly
- **Repeated Logic**: Same coordination patterns appeared in multiple processors
- **Error Handling**: Multiple error sources to handle
- **Testing Complexity**: Required mocking three dependencies

### Code Complexity Example

Original insertion logic (~70 lines):

```csharp
public async Task<Result<Unit, InsertError>> ExecuteAsync(WineRecord record)
{
    // 1. Get random page from buffer manager
    var randomPageResult = await bufferManager.GetRandomPageAsync();
    if (randomPageResult.IsError) { /* handle error */ }

    // 2. Check if page has enough space via file manager
    var hasSpaceResult = await fileManager.PageHasEnoughSpaceToInsertRecord(page, record);
    if (hasSpaceResult.IsError) { /* handle error */ }

    // 3. If no space, allocate new page
    if (!hasSpace) {
        var allocateResult = await fileManager.AllocateNewPageAsync();
        if (allocateResult.IsError) { /* handle error */ }
    }

    // 4. Add record to page
    page.Content = page.Content.Append(record).ToArray();

    // 5. Put page back via buffer manager
    var putResult = await bufferManager.PutPageAsync(page);
    if (putResult.IsError) { /* handle error */ }

    // 6. Create RID and insert into B+ tree
    var rid = new RID(page.PageId, (uint)(page.Content.Length - 1));
    var indexResult = await bPlusTree.InsertAsync(record.HarvestYear, rid);
    if (indexResult.IsError) { /* handle error */ }

    return Result<Unit, InsertError>.Success(Unit.Value);
}
```

## Solution: Storage Facade

### Architecture

```
┌─────────────────────────────────────────────────┐
│                 Client Layer                    │
│  ┌─────────────────┐ ┌─────────────────────────┐ │
│  │ Simplified      │ │ Simplified              │ │
│  │ InsertProcessor │ │ SearchProcessor         │ │
│  └─────────────────┘ └─────────────────────────┘ │
└─────────────────────┬───────────────────────────┘
                      │ Single dependency
                      ▼
┌─────────────────────────────────────────────────┐
│                Storage Facade                   │
│  ┌─────────────────────────────────────────────┐ │
│  │ + InsertWineRecordAsync(WineRecord)         │ │
│  │ + FindRecordsByYearAsync(int year)          │ │
│  │ + InitializeAsync()                         │ │
│  └─────────────────────────────────────────────┘ │
└─────┬─────────────────────────────┬─────────────┘
      │                             │
      ▼                             ▼
┌─────────────┐         ┌───────────────────────┐
│IFileManager │         │IBPlusTreeIndex<int,RID>│
└─────────────┘         └───────────────────────┘
```

### Implementation

#### StorageFacade Class

```csharp
public class StorageFacade
{
    private readonly IBPlusTreeIndex<int, RID> _index;
    private readonly IFileManager _heapFile;
    private readonly ILogger _logger;

    // Unified insertion operation
    public async Task<Result<RID, StorageFacadeError>> InsertWineRecordAsync(WineRecord record)
    {
        // 1. Allocate page and insert record
        // 2. Create RID
        // 3. Insert into B+ tree index
        // All coordination logic encapsulated!
    }

    // Unified search operation
    public async Task<Result<IEnumerable<WineRecord>, StorageFacadeError>> FindRecordsByYearAsync(int year)
    {
        // 1. Search B+ tree for RIDs
        // 2. Retrieve records from heap file
        // All coordination logic encapsulated!
    }
}
```

#### Simplified Processors

```csharp
// After Facade Pattern - Low Coupling
public class SimplifiedInsertProcessor
{
    private readonly StorageFacade _storageFacade; // Single dependency!

    public async Task<Result<RID, InsertError>> ExecuteAsync(WineRecord record)
    {
        // Simple delegation - ~10 lines instead of ~70
        var result = await _storageFacade.InsertWineRecordAsync(record);
        return result.IsError
            ? Result<RID, InsertError>.Error(new InsertError(result.GetErrorOrThrow().Message))
            : Result<RID, InsertError>.Success(result.GetValueOrThrow());
    }
}
```

## Benefits Achieved

### 1. Reduced Complexity

| Metric                     | Before Facade | After Facade | Improvement   |
|----------------------------|---------------|--------------|---------------|
| Dependencies per processor | 3             | 1            | 67% reduction |
| Lines of code (Insert)     | ~70           | ~15          | 79% reduction |
| Lines of code (Search)     | ~50           | ~10          | 80% reduction |
| Error handling points      | 7             | 2            | 71% reduction |
| Mock objects in tests      | 3             | 1            | 67% reduction |

### 2. Improved Maintainability

- **Centralized Logic**: Storage coordination logic is centralized in one place
- **Single Responsibility**: Each processor focuses on its specific business logic
- **Easier Evolution**: Changes to storage subsystem don't affect processors

### 3. Better Testability

```csharp
// Testing before: Mock three dependencies
var mockBuffer = new Mock<IBufferManager>();
var mockFile = new Mock<IFileManager>();
var mockIndex = new Mock<IBPlusTreeIndex<int, RID>>();
var processor = new InsertProcessor(mockBuffer.Object, mockFile.Object, mockIndex.Object);

// Testing after: Mock only the facade
var mockFacade = new Mock<StorageFacade>();
var processor = new SimplifiedInsertProcessor(mockFacade.Object);
```

### 4. Reduced Coupling

- Processors depend only on the facade interface
- Storage subsystem implementation can change without affecting clients
- Clear separation between business logic and storage concerns

## Usage Examples

### Creating Storage Facade

```csharp
// Using the factory
var storageFacade = ApplicationFactory.CreateStorageFacade(
    storagePath: "/path/to/storage",
    indexPath: "/path/to/index.txt",
    degree: 4
);

await storageFacade.InitializeAsync();
```

### Using Simplified Processors

```csharp
// Create processors using the facade
var (insertProcessor, searchProcessor) = ApplicationFactory.CreateSimplifiedProcessors(storageFacade);

// Insert a wine record
var wineRecord = new WineRecord(1, "Cabernet Sauvignon", 2020, WineType.Red);
var insertResult = await insertProcessor.ExecuteAsync(wineRecord);

// Search for records
var searchResult = await searchProcessor.ExecuteAsync(2020);
```

## Extension Points

### Adding New Operations

```csharp
// Extend the facade with new operations
public class StorageFacade
{
    // Existing methods...

    public async Task<Result<Unit, StorageFacadeError>> UpdateWineRecordAsync(RID rid, WineRecord newRecord)
    {
        // Implementation for update operation
    }

    public async Task<Result<Unit, StorageFacadeError>> DeleteWineRecordAsync(RID rid)
    {
        // Implementation for delete operation
    }
}
```

### Specialized Facades

For specific use cases, create specialized facades:

```csharp
// Read-only operations
public class ReadOnlyStorageFacade : StorageFacade
{
    // Only expose read operations
}

// Bulk operations
public class BulkStorageFacade : StorageFacade
{
    public async Task<Result<Unit, StorageFacadeError>> BulkInsertAsync(IEnumerable<WineRecord> records)
    {
        // Optimized bulk insertion logic
    }
}

// Cached operations
public class CachedStorageFacade : StorageFacade
{
    // Add caching layer on top of storage operations
}
```

## Integration with Factory Pattern

The StorageFacade integrates seamlessly with the existing Factory Method pattern:

```csharp
public static class ApplicationFactory
{
    public static StorageFacade CreateStorageFacade(string storagePath, string indexPath)
    {
        var fileManager = CreateFileManager(storagePath);
        var bPlusTree = CreateBPlusTree<int, RID>(indexPath, 4);
        return new StorageFacade(bPlusTree, fileManager);
    }
}
```

## Best Practices

### When to Use Facade

✅ **Use Facade when:**

- Subsystem has many interdependent classes
- Clients perform repeated complex operations
- You want to decouple clients from subsystem implementation
- Multiple clients need similar high-level operations

❌ **Don't use Facade when:**

- Subsystem is simple with few classes
- Clients need fine-grained control over subsystem
- Performance overhead is critical
- Existing interfaces are already simple and well-designed

### Design Guidelines

1. **Keep the facade interface simple**: Only expose what clients actually need
2. **Maintain single responsibility**: Each facade method should have one clear purpose
3. **Handle errors appropriately**: Convert subsystem errors to facade-specific errors
4. **Document the abstraction level**: Make it clear what the facade hides vs. exposes
5. **Consider versioning**: Plan for facade evolution as requirements change

## Conclusion

The Facade pattern implementation successfully addresses the complexity issues in the YggdrasilVinum storage subsystem
by:

- Reducing dependencies from 3 to 1 per processor
- Simplifying code by 67-80%
- Centralizing storage coordination logic
- Improving testability and maintainability
- Maintaining clear separation of concerns

This implementation demonstrates how the Facade pattern can significantly improve code quality in systems with complex
subsystem interactions.
