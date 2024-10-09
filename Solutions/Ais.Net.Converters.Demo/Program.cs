// <copyright file="Program.cs" company="Endjin">
// Copyright (c) Endjin. All rights reserved.
// </copyright>

namespace Ais.Net.Converters.Demo
{
    using Ais.Net;
    using Ais.Net.Converters.Parquet;

    using Microsoft.Extensions.Configuration;

    using System.IO;
    using System.Threading.Tasks;

    public static class Program
    {
        static async Task Main(string[] args)
        {
            var config = new ConfigurationBuilder()
                            .AddJsonFile("settings.json", true, true)
                            .AddJsonFile("local.settings.json", true, true)
                            .Build();

            string input = @"C:\Temp\20210228T15.nm4";
            string output = @"C:\Temp\20210228T15.parquet";

            await using Stream fileStream = File.OpenWrite(output);
            await NmeaStreamParser.ParseFileAsync(input, new ParquetExporter(fileStream)).ConfigureAwait(false);
        }
    }
}