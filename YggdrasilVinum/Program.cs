using System.CommandLine;
using Serilog;
using YggdrasilVinum.Index;
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
            var wineDataArgument = new Argument<FileInfo?>(
                "wine-data",
                "Path to the wine data CSV file that will be parsed"
            ) { Arity = ArgumentArity.ZeroOrOne };
            wineDataArgument.SetDefaultValue(new FileInfo("YggdrasilVinum/Data/wines.csv"));
            rootCommand.AddArgument(wineDataArgument);

            // Argument for page size
            var pageSizeArgument = new Argument<int>(
                "page-size",
                "B+ Tree page size (max children per node)"
            );
            pageSizeArgument.SetDefaultValue(4);
            rootCommand.AddArgument(pageSizeArgument);

            // Argument for commands input file
            var commandsArgument = new Argument<FileInfo?>(
                "commands-file",
                "Path to the file containing commands, or omit to use stdin"
            ) { Arity = ArgumentArity.ZeroOrOne };
            rootCommand.AddArgument(commandsArgument);

            // Option for starting a REPL
            var replOption = new Option<bool>(
                "--repl",
                "Start an interactive REPL for text matching"
            );
            rootCommand.AddOption(replOption);

            rootCommand.SetHandler(async context =>
            {
                var wineData = context.ParseResult.GetValueForArgument(wineDataArgument);
                var pageSize = context.ParseResult.GetValueForArgument(pageSizeArgument);
                var commandsFile = context.ParseResult.GetValueForArgument(commandsArgument);
                var repl = context.ParseResult.GetValueForOption(replOption);
                await RunApplication(wineData, pageSize, commandsFile, repl, context.Console);
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
        int pageSize,
        FileInfo? commandsFile,
        bool startRepl,
        IConsole console
    )
    {
        // Parse wine data
        var wineDataPath = wineDataFile?.FullName ?? "YggdrasilVinum/Data/wines.csv";
        Log.Debug("Using wine data file: {WineDataPath} - {PageSize}", wineDataPath, pageSize);

        var wineResult = WineDataParser.ParseCsvFile(wineDataPath);

        if (wineResult.IsError)
        {
            var error = wineResult.GetErrorOrThrow();
            Log.Error(
                "Error parsing wine data: {ErrorMessage} at line {LineNumber}",
                error.Message,
                error.LineNumber
            );
            return;
        }

        var wines = wineResult.GetValueOrThrow();
        Log.Information("Successfully parsed {Count} wine records", wines.Count);

        var heapSizeInBytes = (ulong)40 * 1024 * 1024; // 40 MB
        var pageSizeInBytes = (ulong)pageSize * 1024; // 1 KB
        var fileManager = ApplicationFactory.CreateFileManager(
            "./storage",
            heapSizeInBytes,
            pageSizeInBytes
        );

        (await fileManager.InitializeAsync()).GetValueOrThrow();

        var amountOfPageFrames = 1UL;
        var amountOfIndexFrames = 1UL;
        var bufferManager = ApplicationFactory.CreateBufferManager(
            fileManager,
            amountOfPageFrames,
            amountOfIndexFrames
        );

        (await bufferManager.InitializeAsync()).GetValueOrThrow();

        var bPlusTree = ApplicationFactory.CreateBPlusTree<int, RID>(
            "./storage/index.txt",
            pageSize
        );

        (await bPlusTree.InitializeAsync()).GetValueOrThrow();

        var insertProcessor = new InsertProcessor(bufferManager, fileManager, bPlusTree);
        var equalityProcessor = new EqualitySearchProcessor(bufferManager, bPlusTree);

        var database = new Database(insertProcessor, equalityProcessor);

        foreach (var wine in wines)
        {
            var result = await database.InsertAsync(wine);
            if (result.IsError)
            {
                var error = result.GetErrorOrThrow();
                Log.Error("Error inserting wine: {ErrorMessage}", error.Message);
            }
        }

        Log.Information(
            "Inserted {Count} wine records into the database",
            database.GetRecordsInsertedCount()
        );

        if (commandsFile == null)
            return;

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

        // Process each command
        foreach (var command in commands)
        {
            Log.Information(
                "Processing command: {CommandType} with key: {CommandKey}",
                command.Type,
                command.Key
            );
            await ProcessCommandAsync(command, bPlusTree, wines, console);
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

    private static async Task<Result<Unit, BPlusTreeError>> ProcessCommandAsync(
        CommandParser.Command command,
        IBPlusTreeIndex<int, RID> bPlusTree,
        List<WineRecord> wines,
        IConsole console
    )
    {
        throw new Exception("Not implemented");
    }
}
