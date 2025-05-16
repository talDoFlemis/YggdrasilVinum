using System.CommandLine;
using System.Text;
using Serilog;
using YggdrasilVinum.Models;
using YggdrasilVinum.Parsers;
using YggdrasilVinum.Services;

namespace YggdrasilVinum;

internal static class Program
{
    private static async Task<int> Main(string[] args)
    {
        // Configure Serilog
        ApplicationFactory.ConfigureSerilog();
        Log.Information("Starting YggdrasilVinum B+ Tree Database Application");

        try
        {
            // Define command line arguments
            var rootCommand = new RootCommand("YggdrasilVinum Wine Database CLI");

            // Argument for wine data file
            var wineDataArgument = new Option<FileInfo?>(
                "--wine-data",
                "Path to the wine data CSV file that will be parsed"
            ) { Arity = ArgumentArity.ZeroOrOne };
            wineDataArgument.SetDefaultValue(new FileInfo("YggdrasilVinum/Data/wines.csv"));
            rootCommand.AddOption(wineDataArgument);

            // Argument for page size
            var pageSizeArgument = new Option<int>(
                "--page-size-in-bytes",
                "Heap file page size in bytes"
            );
            pageSizeArgument.SetDefaultValue(4096);
            rootCommand.AddOption(pageSizeArgument);

            // Argument for Max number of Keys per Node in B+ Tree
            var maxKeysArgument = new Option<int>(
                "--max-keys-per-node",
                "Max number of keys per node in B+ Tree"
            );
            maxKeysArgument.SetDefaultValue(4);
            rootCommand.AddOption(maxKeysArgument);

            // Argument for Heap Size In Bytes
            var heapSizeArgument = new Option<int>(
                "--heap-size-in-bytes",
                "Heap size in bytes"
            );
            heapSizeArgument.SetDefaultValue(40 * 1024 * 1024); // 40 MB
            rootCommand.AddOption(heapSizeArgument);

            // Argument for amount of page frames
            var pageFramesArgument = new Option<int>(
                "--amount-of-page-frames",
                "Amount of page frames"
            );
            pageFramesArgument.SetDefaultValue(1);
            rootCommand.AddOption(pageFramesArgument);

            // Argument for amount of index frames
            var indexFramesArgument = new Option<int>(
                "--amount-of-index-frames",
                "Amount of index frames"
            );
            indexFramesArgument.SetDefaultValue(1);
            rootCommand.AddOption(indexFramesArgument);

            // Argument for commands input file
            var commandsArgument = new Option<FileInfo?>(
                "--commands-file",
                "Path to the file containing commands, or omit to use stdin"
            ) { Arity = ArgumentArity.ZeroOrOne };
            commandsArgument.SetDefaultValue("in.txt");
            rootCommand.AddOption(commandsArgument);

            // Argument for out file
            var outFileArgument = new Option<FileInfo?>(
                "--out-file",
                "Path to the output file for results"
            ) { Arity = ArgumentArity.ZeroOrOne };
            outFileArgument.SetDefaultValue(new FileInfo("out.txt"));
            rootCommand.AddOption(outFileArgument);


            rootCommand.SetHandler(async context =>
            {
                var wineData = context.ParseResult.GetValueForOption(wineDataArgument);
                var pageSize = (ulong)context.ParseResult.GetValueForOption(pageSizeArgument);
                var heapSize = (ulong)context.ParseResult.GetValueForOption(heapSizeArgument);
                var maxKeys = (ulong)context.ParseResult.GetValueForOption(maxKeysArgument);
                var pageFrames = (ulong)context.ParseResult.GetValueForOption(pageFramesArgument);
                var indexFrames = (ulong)context.ParseResult.GetValueForOption(indexFramesArgument);
                var commandsFile = context.ParseResult.GetValueForOption(commandsArgument);
                var outFile = context.ParseResult.GetValueForOption(outFileArgument);
                await RunApplication(wineData, commandsFile, outFile, pageSize, heapSize, pageFrames, indexFrames,
                    maxKeys);
            });

            return await rootCommand.InvokeAsync(args);
        }
        catch (Exception ex)
        {
            Log.Fatal(ex, "Application terminated unexpectedly");
            return 1;
        }
        finally
        {
            await Log.CloseAndFlushAsync();
        }
    }

    private static async Task RunApplication(
        FileInfo? wineDataFile,
        FileInfo? commandsFile,
        FileInfo? outFile,
        ulong pageSizeInBytes,
        ulong heapSizeInBytes,
        ulong amountOfPageFrames,
        ulong amountOfIndexFrames,
        ulong maxNumberOfKeysPerNode
    )
    {
        // Parse wine data
        var wineDataPath = wineDataFile?.FullName ?? "YggdrasilVinum/Data/wines.csv";
        Log.Debug("Using wine data file: {WineDataPath}", wineDataPath);

        var fileManager = ApplicationFactory.CreateFileManager(
            "./storage",
            heapSizeInBytes,
            pageSizeInBytes
        );

        (await fileManager.InitializeAsync()).GetValueOrThrow();

        var bufferManager = ApplicationFactory.CreateBufferManager(
            fileManager,
            amountOfPageFrames,
            amountOfIndexFrames
        );

        (await bufferManager.InitializeAsync()).GetValueOrThrow();

        var bPlusTree = ApplicationFactory.CreateBPlusTree<int, RID>(
            "./storage/index.txt",
            (int)maxNumberOfKeysPerNode
        );

        (await bPlusTree.InitializeAsync()).GetValueOrThrow();

        var insertProcessor = new InsertProcessor(bufferManager, fileManager, bPlusTree);
        var equalityProcessor = new EqualitySearchProcessor(bufferManager, bPlusTree);

        var database = new Database(insertProcessor, equalityProcessor);

        var wineProcessor = ApplicationFactory.CreateWineProcessor("./storage/processed_wines.txt");
        var harvestYearSearchProcessor =
            ApplicationFactory.CreateHarvestYearSearchProcessor(wineProcessor);
        var processResult = await wineProcessor.ProcessCsvFileAsync(wineDataPath);
        if (processResult.IsError)
        {
            var error = processResult.GetErrorOrThrow();
            Log.Error("Error processing wine data: {ErrorMessage}", error.Message);
            return;
        }


        Log.Debug("Processing commands from file: {CommandsFile}", commandsFile.FullName);
        var commandsResult = CommandParser.ParseCommandFile(commandsFile.FullName);

        if (commandsResult.IsError)
        {
            var error = commandsResult.GetErrorOrThrow();
            Log.Error(
                "Error parsing commands: {ErrorMessage} at line {LineNumber}",
                error.Message,
                error.LineNumber
            );
            return;
        }

        var (header, commands) = commandsResult.GetValueOrThrow();
        Log.Information(
            "Successfully parsed {CommandCount} commands with max children: {MaxChildren}",
            commands.Count,
            header.MaxChildren
        );

        // Create StringBuilder for output content
        var outputContent = new StringBuilder();
        // Write the header line
        outputContent.AppendLine($"FLH/{header.MaxChildren}");

        // Process each command
        foreach (var command in commands)
        {
            Log.Information(
                "Processing command: {CommandType} with key: {CommandKey}",
                command.Type,
                command.Key
            );
            var commandResult = await ProcessCommandAsync(command, database, harvestYearSearchProcessor, outputContent);
            if (commandResult.IsError)
            {
                var error = commandResult.GetErrorOrThrow();
                Log.Error("Error processing command: {ErrorMessage}", error);
                return;
            }
        }

        // Add the height of the tree as the last line
        var height = await bPlusTree.HeightAsync();
        if (height.IsError)
        {
            var error = height.GetErrorOrThrow();
            Log.Error("Error getting height of B+ tree: {ErrorMessage}", error.Message);
            return;
        }

        outputContent.AppendLine($"H/{height.GetValueOrThrow()}");

        // Write the output content to file
        if (outFile != null)
            try
            {
                await File.WriteAllTextAsync(outFile.FullName, outputContent.ToString());
                Log.Information("Output written to file: {OutFile}", outFile.FullName);
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Error writing to output file: {OutFile}", outFile.FullName);
            }

        var bufferFlushResult = await bufferManager.FlushAllFramesAsync();
        if (bufferFlushResult.IsError)
        {
            var error = bufferFlushResult.GetErrorOrThrow();
            Log.Error("Error flushing buffer: {ErrorMessage}", error.Message);
        }

        var heapFlushResult = await fileManager.FlushAsync();
        if (heapFlushResult.IsError)
        {
            var error = heapFlushResult.GetErrorOrThrow();
            Log.Error("Error flushing heap: {ErrorMessage}", error.Message);
        }
    }

    private static async Task<Result<Unit, string>> ProcessCommandAsync(
        CommandParser.Command command,
        Database database,
        HarvestYearSearchProcessor harvestYearSearchProcessor,
        StringBuilder outputContent
    )
    {
        switch (command.Type)
        {
            case CommandParser.CommandType.Insert:
                var winesResult = await harvestYearSearchProcessor.SearchByHarvestYearAsync(command.Key);
                if (winesResult.IsError)
                {
                    var error = winesResult.GetErrorOrThrow();
                    Log.Error("Error searching for wine with harvest year {HarvestYear}: {ErrorMessage}", command.Key,
                        error.Message);
                    return Result<Unit, string>.Error(error.Message);
                }

                var wines = winesResult.GetValueOrThrow();

                foreach (var wine in wines)
                {
                    var insertResult = await database.InsertAsync(wine);
                    if (insertResult.IsError)
                    {
                        var error = insertResult.GetErrorOrThrow();
                        Log.Error("Error inserting wine with ID {WineId}: {ErrorMessage}", wine.WineId, error.Message);
                        return Result<Unit, string>.Error(error.Message);
                    }

                    Log.Debug("Inserted wine with ID {WineId}", wine.WineId);
                }

                outputContent.AppendLine($"INC:{command.Key}/{wines.Length}");

                Log.Information("Inserted {WineCount} wines with harvest year {HarvestYear}", wines.Length,
                    command.Key);
                break;

            case CommandParser.CommandType.Search:
                var searchResult = await database.SearchAsync(command.Key);
                if (searchResult.IsError)
                {
                    var error = searchResult.GetErrorOrThrow();
                    Log.Error("Error searching for wine with ID {WineId}: {ErrorMessage}", command.Key, error.Message);
                    return Result<Unit, string>.Error(error.Message);
                }

                var winesFound = searchResult.GetValueOrThrow();

                // Write the search result to the output content
                outputContent.AppendLine($"BUS=:{command.Key}/{winesFound.Length}");

                foreach (var wine in winesFound)
                    Log.Debug("Wine ID: {WineId}, Name: {WineName}", wine.WineId, wine.Label);

                Log.Information("Found {WineCount} wines with harvest year {HarvestYear}", winesFound.Length,
                    command.Key);

                break;
        }

        return Result<Unit, string>.Success(new Unit());
    }
}
