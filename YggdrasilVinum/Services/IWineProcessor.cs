using YggdrasilVinum.Models;

namespace YggdrasilVinum.Services;

/// <summary>
///     Interface for wine processing operations including binary search by harvest year.
/// </summary>
public interface IWineProcessor : IDisposable
{
    /// <summary>
    ///     Gets the total number of records in the processed file.
    /// </summary>
    long RecordCount { get; }

    /// <summary>
    ///     Processes the CSV file and creates a sorted binary file.
    /// </summary>
    Task<Result<Unit, WineProcessorError>> ProcessCsvFileAsync(string csvFilePath);

    /// <summary>
    ///     Opens an existing processed file for binary search operations.
    /// </summary>
    Task<Result<Unit, WineProcessorError>> OpenProcessedFileAsync();

    /// <summary>
    ///     Searches for wines with the specified harvest year using binary search.
    /// </summary>
    Task<Result<List<WineRecord>, WineProcessorError>> SearchByHarvestYearAsync(int harvestYear);
}
