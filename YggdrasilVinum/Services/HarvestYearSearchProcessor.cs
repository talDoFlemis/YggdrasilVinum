using Serilog;
using YggdrasilVinum.Models;

namespace YggdrasilVinum.Services;

/// <summary>
///     A search processor that uses the WineProcessor for efficient binary search by harvest year.
///     This provides an alternative to the B+ tree index for searching by harvest year.
/// </summary>
public class HarvestYearSearchProcessor
{
    private readonly ILogger _logger = Log.ForContext<HarvestYearSearchProcessor>();
    private readonly IWineProcessor _wineProcessor;

    public HarvestYearSearchProcessor(IWineProcessor wineProcessor)
    {
        _wineProcessor = wineProcessor ?? throw new ArgumentNullException(nameof(wineProcessor));
    }

    /// <summary>
    ///     Searches for wines with the specified harvest year using binary search on processed file.
    /// </summary>
    public async Task<Result<WineRecord[], SearchError>> SearchByHarvestYearAsync(int harvestYear)
    {
        _logger.Debug("Searching for wines with harvest year {HarvestYear}", harvestYear);

        try
        {
            var result = await _wineProcessor.SearchByHarvestYearAsync(harvestYear);

            if (result.IsError)
            {
                var error = result.GetErrorOrThrow();
                _logger.Error("Failed to search by harvest year: {Error}", error.Message);
                return Result<WineRecord[], SearchError>.Error(
                    new SearchError($"Failed to search by harvest year: {error.Message}"));
            }

            var wines = result.GetValueOrThrow();
            _logger.Information("Found {Count} wines with harvest year {HarvestYear}", wines.Count, harvestYear);

            return Result<WineRecord[], SearchError>.Success(wines.ToArray());
        }
        catch (Exception ex)
        {
            _logger.Error(ex, "Unexpected error searching by harvest year {HarvestYear}", harvestYear);
            return Result<WineRecord[], SearchError>.Error(
                new SearchError($"Unexpected error during search: {ex.Message}"));
        }
    }

    /// <summary>
    ///     Searches for wines within a range of harvest years.
    /// </summary>
    public async Task<Result<WineRecord[], SearchError>> SearchByHarvestYearRangeAsync(int fromYear, int toYear)
    {
        _logger.Debug("Searching for wines with harvest year range {FromYear} to {ToYear}", fromYear, toYear);

        if (fromYear > toYear)
            return Result<WineRecord[], SearchError>.Error(
                new SearchError("fromYear cannot be greater than toYear"));

        try
        {
            var allWines = new List<WineRecord>();

            for (var year = fromYear; year <= toYear; year++)
            {
                var result = await _wineProcessor.SearchByHarvestYearAsync(year);

                if (result.IsError)
                {
                    var error = result.GetErrorOrThrow();
                    _logger.Error("Failed to search by harvest year {Year}: {Error}", year, error.Message);
                    return Result<WineRecord[], SearchError>.Error(
                        new SearchError($"Failed to search by harvest year {year}: {error.Message}"));
                }

                allWines.AddRange(result.GetValueOrThrow());
            }

            _logger.Information("Found {Count} wines with harvest year range {FromYear}-{ToYear}",
                allWines.Count, fromYear, toYear);

            return Result<WineRecord[], SearchError>.Success(allWines.ToArray());
        }
        catch (Exception ex)
        {
            _logger.Error(ex, "Unexpected error searching by harvest year range {FromYear}-{ToYear}",
                fromYear, toYear);
            return Result<WineRecord[], SearchError>.Error(
                new SearchError($"Unexpected error during range search: {ex.Message}"));
        }
    }

    /// <summary>
    ///     Gets statistics about wines by harvest year.
    /// </summary>
    public async Task<Result<Dictionary<int, int>, SearchError>> GetWineCountByYearAsync(int fromYear, int toYear)
    {
        _logger.Debug("Getting wine count statistics for years {FromYear} to {ToYear}", fromYear, toYear);

        if (fromYear > toYear)
            return Result<Dictionary<int, int>, SearchError>.Error(
                new SearchError("fromYear cannot be greater than toYear"));

        try
        {
            var statistics = new Dictionary<int, int>();

            for (var year = fromYear; year <= toYear; year++)
            {
                var result = await _wineProcessor.SearchByHarvestYearAsync(year);

                if (result.IsError)
                {
                    var error = result.GetErrorOrThrow();
                    _logger.Error("Failed to search by harvest year {Year}: {Error}", year, error.Message);
                    return Result<Dictionary<int, int>, SearchError>.Error(
                        new SearchError($"Failed to search by harvest year {year}: {error.Message}"));
                }

                var count = result.GetValueOrThrow().Count;
                if (count > 0) statistics[year] = count;
            }

            _logger.Information("Generated statistics for {Years} years with wines", statistics.Count);
            return Result<Dictionary<int, int>, SearchError>.Success(statistics);
        }
        catch (Exception ex)
        {
            _logger.Error(ex, "Unexpected error generating statistics for years {FromYear}-{ToYear}",
                fromYear, toYear);
            return Result<Dictionary<int, int>, SearchError>.Error(
                new SearchError($"Unexpected error during statistics generation: {ex.Message}"));
        }
    }
}
