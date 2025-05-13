using System.CommandLine;
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
            var wineDataArgument = new Argument<FileInfo?>(
                "wine-data",
                "Path to the wine data CSV file that will be parsed") { Arity = ArgumentArity.ZeroOrOne };
            wineDataArgument.SetDefaultValue(new FileInfo("YggdrasilVinum/Data/wines.csv"));
            rootCommand.AddArgument(wineDataArgument);

            // Argument for page size
            var pageSizeArgument = new Argument<int>(
                "page-size",
                "B+ Tree page size (max children per node)");
            pageSizeArgument.SetDefaultValue(4);
            rootCommand.AddArgument(pageSizeArgument);

            // Argument for commands input file
            var commandsArgument = new Argument<FileInfo?>(
                "commands-file",
                "Path to the file containing commands, or omit to use stdin") { Arity = ArgumentArity.ZeroOrOne };
            rootCommand.AddArgument(commandsArgument);

            // Option for starting a REPL
            var replOption = new Option<bool>(
                "--repl",
                "Start an interactive REPL for text matching");
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

    private static async Task RunApplication(FileInfo? wineDataFile, int pageSize, FileInfo? commandsFile,
        bool startRepl, IConsole console)
    {
        // Parse wine data
        var wineDataPath = wineDataFile?.FullName ?? "YggdrasilVinum/Data/wines.csv";
        Log.Debug("Using wine data file: {WineDataPath} - {PageSize}", wineDataPath, pageSize);

        var wineResult = WineDataParser.ParseCsvFile(wineDataPath);

        if (wineResult.IsError)
        {
            var error = wineResult.GetErrorOrThrow();
            Log.Error("Error parsing wine data: {ErrorMessage} at line {LineNumber}", error.Message, error.LineNumber);
            return;
        }

        var wines = wineResult.GetValueOrThrow();
        Log.Information("Successfully parsed {Count} wine records", wines.Count);

        // Print each wine record
        foreach (var wine in wines)
            Log.Debug("Found Wine ID: {WineId}, Label: {Label}, Harvest Year: {HarvestYear}, Type: {Type}", wine.WineId,
                wine.Label, wine.HarvestYear, wine.Type);

        // Create a B+ Tree with the wines data
        var bPlusTree = new BPlusTree<int, WineRecord>(pageSize);

        // Insert all wines into the B+ tree
        foreach (var wine in wines)
        {
            bPlusTree.Insert(wine.WineId, wine);
            Log.Information("Inserted wine: {WineId} - {Label}", wine.WineId, wine.Label);
        }


        if (commandsFile == null)
            return;

        Log.Debug("Processing commands from file: {CommandsFile}", commandsFile.FullName);
        var commandsResult = CommandParser.ParseCommandFile(commandsFile.FullName);

        if (commandsResult.IsError)
        {
            var error = commandsResult.GetErrorOrThrow();
            Log.Error("Error parsing commands: {ErrorMessage} at line {LineNumber}", error.Message, error.LineNumber);
            return;
        }

        var (header, commands) = commandsResult.GetValueOrThrow();
        Log.Information("Successfully parsed {CommandCount} commands with max children: {MaxChildren}", commands.Count,
            header.MaxChildren);

        // Print and process each command
        foreach (var command in commands)
        {
            Log.Information("Processing command: {CommandType} with key: {CommandKey}", command.Type, command.Key);
            ProcessCommand(command, bPlusTree, wines, console);
        }
    }

    private static void ProcessCommand(CommandParser.Command command, BPlusTree<int, WineRecord> bPlusTree,
        List<WineRecord> wines, IConsole console)
    {
        switch (command.Type)
        {
            case CommandParser.CommandType.Insert:
                var matchingWine = wines.FirstOrDefault(w => w.WineId == command.Key);
                if (matchingWine.WineId == command.Key)
                {
                    bPlusTree.Insert(matchingWine.WineId, matchingWine);
                    console.WriteLine($"Inserted wine with ID {command.Key}: {matchingWine.Label}");
                }
                else
                {
                    console.WriteLine($"No wine found with ID {command.Key}");
                }

                break;

            case CommandParser.CommandType.Search:
                var results = bPlusTree.Search(command.Key);
                if (results.Count > 0)
                {
                    console.WriteLine($"Found {results.Count} matching wines:");
                    foreach (var result in results)
                        console.WriteLine(
                            $"  Wine ID: {result.WineId}, Label: {result.Label}, Harvest Year: {result.HarvestYear}, Type: {result.Type}");
                }
                else
                {
                    console.WriteLine($"No wines found with ID {command.Key}");
                }

                break;
        }
    }
}
