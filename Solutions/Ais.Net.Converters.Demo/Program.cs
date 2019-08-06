// <copyright file="Program.cs" company="Endjin">
// Copyright (c) Endjin. All rights reserved.
// </copyright>

namespace Endjin.Ais.Converters.Demo
{
    using Endjin.Ais;
    using global::Ais.Net.Converters.Parquet;
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

            string input = @"C:\Temp\nmea-ais\raw\20190731T07.nm4";
            string output = @"C:\Temp\nmea-ais\parquet\20190731T07.parquet";
            
            using (Stream fileStream = File.OpenWrite(output))
            {
                await NmeaStreamParser.ParseFileAsync(input, new ParquetExporter(fileStream)).ConfigureAwait(false);
            }
        }
    }
}