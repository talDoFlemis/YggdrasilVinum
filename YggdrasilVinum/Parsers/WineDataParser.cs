using YggdrasilVinum.Models;
using Serilog;

namespace YggdrasilVinum.Parsers;

using System;
using System.Collections.Generic;
using System.IO;

public static class WineDataParser
{
    public readonly struct ParseError
    {
        public readonly string Message;
        public readonly int LineNumber;

        public ParseError(string message, int lineNumber)
        {
            Message = message;
            LineNumber = lineNumber;
        }

        public override string ToString() => $"Line {LineNumber}: {Message}";
    }

    public static Result<List<WineRecord>, ParseError> ParseCsvFile(string filePath)
    {
        Log.Information("Parsing wine data CSV file: {FilePath}", filePath);

        if (!File.Exists(filePath))
        {
            Log.Warning("CSV file not found: {FilePath}", filePath);
            return Result<List<WineRecord>, ParseError>.Error(
                new ParseError($"CSV file not found: {filePath}", 0));
        }

        try
        {
            using var fileReader = new StreamReader(filePath);
            var result = ParseCsvReader(fileReader);
            Log.Debug("CSV file parsing completed: {FilePath}", filePath);
            return result;
        }
        catch (Exception ex)
        {
            Log.Error(ex, "Failed to read CSV file: {FilePath}", filePath);
            return Result<List<WineRecord>, ParseError>.Error(
                new ParseError($"Failed to read CSV file: {ex.Message}", 0));
        }
    }

    public static Result<List<WineRecord>, ParseError> ParseCsvString(string csvContent)
    {
        Log.Information("Parsing wine data from string input");
        Log.Debug("CSV string length: {Length} characters", csvContent?.Length ?? 0);

        if (string.IsNullOrEmpty(csvContent))
        {
            Log.Warning("CSV string is empty");
            return Result<List<WineRecord>, ParseError>.Error(
                new ParseError("CSV string is empty", 0));
        }

        using var stringReader = new StringReader(csvContent);
        var result = ParseCsvReader(stringReader);
        Log.Debug("CSV string parsing completed");
        return result;
    }

    public static Result<List<WineRecord>, ParseError> ParseCsvReader(TextReader reader)
    {
        Log.Information("Parsing wine data from TextReader");
        try
        {
            var records = new List<WineRecord>();
            // Read and skip header line
            string headerLine = reader.ReadLine();
            Log.Debug("Skipped CSV header: {HeaderLine}", headerLine);

            if (headerLine == null)
            {
                Log.Warning("CSV input is empty");
                return Result<List<WineRecord>, ParseError>.Error(
                    new ParseError("CSV input is empty", 0));
            }

            string line;
            int lineNumber = 1;

            while ((line = reader.ReadLine()) != null)
            {
                if (string.IsNullOrWhiteSpace(line))
                {
                    Log.Debug("Skipping empty line at line {LineNumber}", lineNumber);
                    continue;
                }

                Log.Debug("Parsing CSV line {LineNumber}: {Line}", lineNumber, line);
                var lineResult = ParseCsvLine(line, lineNumber);
                if (lineResult.IsError)
                {
                    Log.Warning("CSV line parsing failed at line {LineNumber}: {Error}",
                        lineNumber, lineResult.GetErrorOrThrow().Message);
                    return Result<List<WineRecord>, ParseError>.Error(lineResult.GetErrorOrThrow());
                }

                records.Add(lineResult.GetValueOrThrow());
                lineNumber++;
            }

            Log.Information("Successfully parsed {RecordCount} wine records", records.Count);
            return Result<List<WineRecord>, ParseError>.Success(records);
        }
        catch (Exception ex)
        {
            Log.Error(ex, "Failed to read CSV data");
            return Result<List<WineRecord>, ParseError>.Error(
                new ParseError($"Failed to read CSV data: {ex.Message}", 0));
        }
    }

    private static Result<WineRecord, ParseError> ParseCsvLine(string line, int lineNumber)
    {
        try
        {
            string[] parts = line.Split(',');
            if (parts.Length != 4)
            {
                Log.Warning("Invalid CSV line format at line {LineNumber}: Expected 4 fields, got {FieldCount}",
                    lineNumber, parts.Length);
                return Result<WineRecord, ParseError>.Error(
                    new ParseError($"Expected 4 fields, got {parts.Length}", lineNumber));
            }

            if (!int.TryParse(parts[0], out int wineId))
            {
                Log.Warning("Invalid wine ID at line {LineNumber}: {Value}", lineNumber, parts[0]);
                return Result<WineRecord, ParseError>.Error(
                    new ParseError("Invalid wine ID", lineNumber));
            }

            if (!int.TryParse(parts[2], out int harvestYear))
            {
                Log.Warning("Invalid harvest year at line {LineNumber}: {Value}", lineNumber, parts[2]);
                return Result<WineRecord, ParseError>.Error(
                    new ParseError("Invalid harvest year", lineNumber));
            }

            var wineTypeResult = ParseWineType(parts[3], lineNumber);
            if (wineTypeResult.IsError)
            {
                return Result<WineRecord, ParseError>.Error(wineTypeResult.GetErrorOrThrow());
            }

            Log.Debug(
                "CSV line parsed successfully: WineId={WineId}, Label={Label}, HarvestYear={HarvestYear}, Type={WineType}",
                wineId, parts[1], harvestYear, wineTypeResult.GetValueOrThrow());

            return Result<WineRecord, ParseError>.Success(
                new WineRecord(wineId, parts[1], harvestYear, wineTypeResult.GetValueOrThrow()));
        }
        catch (Exception ex)
        {
            Log.Warning(ex, "Failed to parse CSV line at line {LineNumber}: {Line}", lineNumber, line);
            return Result<WineRecord, ParseError>.Error(
                new ParseError($"Failed to parse line: {ex.Message}", lineNumber));
        }
    }

    private static Result<WineType, ParseError> ParseWineType(string type, int lineNumber)
    {
        Log.Debug("Parsing wine type at line {LineNumber}: {WineType}", lineNumber, type);
        var result = type.ToLowerInvariant() switch
        {
            "tinto" => Result<WineType, ParseError>.Success(WineType.Red),
            "branco" => Result<WineType, ParseError>.Success(WineType.White),
            "rose" or "rosé" => Result<WineType, ParseError>.Success(WineType.Rose),
            _ => Result<WineType, ParseError>.Error(
                new ParseError($"Invalid wine type: {type}", lineNumber))
        };

        if (result.IsError)
        {
            Log.Warning("Invalid wine type at line {LineNumber}: {WineType}", lineNumber, type);
        }

        return result;
    }
}