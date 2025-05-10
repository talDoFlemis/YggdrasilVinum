using System;
using System.Collections.Generic;
using System.IO;
using YggdrasilVinum.Models;

namespace YggdrasilVinum.Tests.Unit.Parsers;

public static class TestHelper
{
    public static readonly string ValidCsvHeader = "id,label,year,type";
    
    public static readonly List<(int Id, string Label, int Year, WineType Type)> TestWineData = new()
    {
        (1, "Château Margaux", 2018, WineType.Red),
        (2, "Domaine Leflaive", 2020, WineType.White),
        (3, "Château d'Yquem", 2019, WineType.White),
        (4, "Biondi-Santi", 2015, WineType.Red),
        (5, "Domaine Tempier", 2021, WineType.Rose)
    };
    
    public static string GenerateTestCsvContent(IEnumerable<(int Id, string Label, int Year, WineType Type)> entries)
    {
        using var stringWriter = new StringWriter();
        stringWriter.WriteLine(ValidCsvHeader);
        
        foreach (var entry in entries)
        {
            string typeStr = entry.Type switch
            {
                WineType.Red => "tinto",
                WineType.White => "branco",
                WineType.Rose => "rosé",
                _ => throw new ArgumentOutOfRangeException()
            };
            
            stringWriter.WriteLine($"{entry.Id},{entry.Label},{entry.Year},{typeStr}");
        }
        
        return stringWriter.ToString();
    }
    
    public static List<WineRecord> CreateWineRecords(IEnumerable<(int Id, string Label, int Year, WineType Type)> entries)
    {
        var records = new List<WineRecord>();
        foreach (var entry in entries)
        {
            records.Add(new WineRecord(entry.Id, entry.Label, entry.Year, entry.Type));
        }
        return records;
    }
    
    public static string GenerateInvalidTestCsvContent()
    {
        return 
            $"{ValidCsvHeader}\n" +
            "1,Valid Wine,2020,tinto\n" +
            "not_an_id,Invalid ID,2021,tinto\n" +
            "2,Missing Year,,branco\n" +
            "3,Invalid Year,not_a_year,tinto\n" +
            "4,Invalid Type,2022,invalid_type\n" +
            "5,Too Few Fields,2023\n" +
            "6,Too Many Fields,2024,tinto,extra_field\n";
    }
    
    public static string CreateTempCsvFile(string content)
    {
        string tempFilePath = Path.GetTempFileName();
        File.WriteAllText(tempFilePath, content);
        return tempFilePath;
    }
    
    public static void DeleteTempFile(string filePath)
    {
        if (File.Exists(filePath))
        {
            File.Delete(filePath);
        }
    }
}