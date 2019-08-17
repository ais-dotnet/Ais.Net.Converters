// <copyright file="NmeaToParquetConverter.cs" company="Endjin">
// Copyright (c) Endjin. All rights reserved.
// </copyright>

using System.IO;
using System.Threading.Tasks;
using Ais.Net.Parquet;
using Endjin.Ais;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Ais.Net.Converter
{
    public static class NmeaToParquetConverter
    {
        [FunctionName("NmeaToParquetConverter")]
        public static async Task Run(
            [QueueTrigger("convert-nmea", Connection = "AzureWebJobsStorage")]ConvertNmeaFile message,
            [Blob("%ContainerName%", FileAccess.ReadWrite, Connection = "NmeaStorage")] CloudBlobContainer blobContainer,
            ILogger log)
        {
            log.LogInformation($"Converting '{message.SourcePath}' to {message.TargetPath}");

            CloudAppendBlob sourceBlob = blobContainer.GetAppendBlobReference(message.SourcePath);
            CloudAppendBlob destinationBlob = blobContainer.GetAppendBlobReference(message.TargetPath);
            destinationBlob.Properties.ContentType = "application/octet-stream";

            using (Stream inputStream = await sourceBlob.OpenReadAsync().ConfigureAwait(false))
            using (Stream outputStream = await destinationBlob.OpenWriteAsync(true).ConfigureAwait(false))
            {
                await NmeaStreamParser.ParseStreamAsync(inputStream, new NmeaLineToAisStreamAdapter(new ParquetExporter(log, outputStream))).ConfigureAwait(false);
            }
        }
    }
}
