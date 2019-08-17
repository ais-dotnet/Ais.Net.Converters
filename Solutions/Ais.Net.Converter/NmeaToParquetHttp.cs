// <copyright file="NmeaToParquetHttp.cs" company="Endjin">
// Copyright (c) Endjin. All rights reserved.
// </copyright>

using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace Ais.Net.Converter
{
    public static class NmeaToParquetHttp
    {
        [FunctionName("NmeaToParquetHttp")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequest req,
            [Queue("convert-nmea", Connection = "AzureWebJobsStorage")] ICollector<ConvertNmeaFile> queue,
            ILogger log)
        {
            ConvertNmeaRequest request = JsonConvert.DeserializeObject<ConvertNmeaRequest>(await req.ReadAsStringAsync().ConfigureAwait(false));

            // If start date is not in an hour boundary then set to the next hour.
            var startDateTime = AdjustDateToHourBoundary(request.StartDate);
            var endDateTime = AdjustDateToHourBoundary(request.EndDate);

            if (startDateTime >= endDateTime)
            {
                return new BadRequestResult();
            }

            while (startDateTime < endDateTime)
            {
                var path = $"raw-adjusted/{startDateTime.ToString("yyyyMMdd")}/{startDateTime.ToString("yyyyMMddTHH")}.nm4";
                var targetPath = $"parquet/{startDateTime.ToString("yyyyMMdd")}/{startDateTime.ToString("yyyyMMddTHH")}.parquet";

                log.LogInformation($"Enqueueing request to convert '{path}' to '{targetPath}'");
                queue.Add(new ConvertNmeaFile { SourcePath = path, TargetPath = targetPath });
                startDateTime = startDateTime.AddHours(1);
            }

            return new AcceptedResult();
        }

        private static DateTime AdjustDateToHourBoundary(DateTime dateTimeToAdjust)
        {
            var dateTime = new DateTime(dateTimeToAdjust.Year, dateTimeToAdjust.Month, dateTimeToAdjust.Day, dateTimeToAdjust.Hour, 0, 0);
            return dateTimeToAdjust > dateTime ? dateTime.AddHours(1) : dateTime;
        }
    }
}
