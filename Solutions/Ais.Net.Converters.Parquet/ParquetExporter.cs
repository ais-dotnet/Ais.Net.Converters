// <copyright file="ParquetExporter.cs" company="Endjin">
// Copyright (c) Endjin. All rights reserved.
// </copyright>

namespace Ais.Net.Converters.Parquet
{

    using System;
    using System.Buffers.Text;
    using System.IO;
    using Endjin.Ais;
    using global::Parquet;
    using global::Parquet.Data;

    public class ParquetExporter : INmeaAisMessageStreamProcessor
    {
        private const int MaxRecordsPerGroup = 100_000;

        private static readonly DataField<int> sourceColumnDefinition = new DataField<int>("source");
        private static readonly DataField<long> timestampColumnDefinition = new DataField<long>("timestamp");
        private static readonly DataField<int> mmsiColumnDefinition = new DataField<int>("mmsi");
        private static readonly DataField<int> latitudeColumnDefinition = new DataField<int>("latitude");
        private static readonly DataField<int> longitudeColumnDefinition = new DataField<int>("longitude");
        private static readonly DataField<int> courseOverGroundColumnDefinition = new DataField<int>("courseOverGround");
        private static readonly DataField<int> trueHeadingColumnDefinition = new DataField<int>("trueHeading");

        private static readonly Schema schema = new Schema(
            sourceColumnDefinition,
            timestampColumnDefinition,
            mmsiColumnDefinition,
            latitudeColumnDefinition,
            longitudeColumnDefinition,
            courseOverGroundColumnDefinition,
            trueHeadingColumnDefinition);

        private int[] sourceIds = new int[MaxRecordsPerGroup];
        private long[] timestamps = new long[MaxRecordsPerGroup];
        private int[] mmsis = new int[MaxRecordsPerGroup];
        private int[] lats = new int[MaxRecordsPerGroup];
        private int[] longs = new int[MaxRecordsPerGroup];
        private int[] courseOverGrounds = new int[MaxRecordsPerGroup];
        private int[] trueHeadings = new int[MaxRecordsPerGroup];

        private readonly ParquetWriter parquetWriter;

        private int indexInGroup = 0;

        private int ingested = 0;

        public ParquetExporter(Stream output)
        {
            this.parquetWriter = new ParquetWriter(schema, output);
        }

        public void OnNext(
            in NmeaLineParser firstLine,
            in ReadOnlySpan<byte> asciiPayload,
            uint padding)
        {
            int messageType = NmeaPayloadParser.PeekMessageType(asciiPayload, padding);
            if (messageType >= 1 && messageType <= 3)
            {
                var parsedPosition = new NmeaAisPositionReportClassAParser(asciiPayload, padding);

                try
                {
                    this.WriteRow(
                        firstLine.TagBlock,
                        parsedPosition.Mmsi,
                        parsedPosition.Latitude10000thMins,
                        parsedPosition.Longitude10000thMins,
                        parsedPosition.CourseOverGround10thDegrees,
                        parsedPosition.TrueHeadingDegrees);
                }
                catch { }
            }

            if (messageType == 18)
            {
                try
                {
                    var parsedPosition = new NmeaAisPositionReportClassBParser(asciiPayload, padding);
                    this.WriteRow(
                        firstLine.TagBlock,
                        parsedPosition.Mmsi,
                        parsedPosition.Latitude10000thMins,
                        parsedPosition.Longitude10000thMins,
                        parsedPosition.CourseOverGround10thDegrees,
                        parsedPosition.TrueHeadingDegrees);
                }
                catch { }
            }

            if (messageType == 19)
            {
                try
                {
                    var parsedPosition = new NmeaAisPositionReportExtendedClassBParser(asciiPayload, padding);
                    this.WriteRow(
                        firstLine.TagBlock,
                        parsedPosition.Mmsi,
                        parsedPosition.Latitude10000thMins,
                        parsedPosition.Longitude10000thMins,
                        parsedPosition.CourseOverGround10thDegrees,
                        parsedPosition.TrueHeadingDegrees);
                }
                catch { }
            }
        }

        private void WriteRow(
            in NmeaTagBlockParser tagBlock,
            uint mmsi,
            int latitude10000thMins,
            int longitude10000thMins,
            uint courseOverGround10thDegrees,
            uint trueHeadingDegrees)
        {
            this.sourceIds[this.indexInGroup] = Utf8Parser.TryParse(tagBlock.Source, out int id, out _) ? id : 0;
            this.timestamps[this.indexInGroup] = tagBlock.UnixTimestamp.Value;
            this.mmsis[this.indexInGroup] = (int)mmsi;
            this.lats[this.indexInGroup] = latitude10000thMins;
            this.longs[this.indexInGroup] = longitude10000thMins;
            this.courseOverGrounds[this.indexInGroup] = (int)courseOverGround10thDegrees;
            this.trueHeadings[this.indexInGroup] = (int)trueHeadingDegrees;

            if (++this.indexInGroup == MaxRecordsPerGroup)
            {
                this.WriteRowGroup();
                this.indexInGroup = 0;
            }

            this.ingested += 1;
        }

        public void OnCompleted()
        {
            if (this.indexInGroup != 0)
            {
                Array.Resize(ref this.sourceIds, this.indexInGroup);
                Array.Resize(ref this.timestamps, this.indexInGroup);
                Array.Resize(ref this.mmsis, this.indexInGroup);
                Array.Resize(ref this.lats, this.indexInGroup);
                Array.Resize(ref this.longs, this.indexInGroup);
                Array.Resize(ref this.courseOverGrounds, this.indexInGroup);
                Array.Resize(ref this.trueHeadings, this.indexInGroup);
                this.WriteRowGroup();
            }

            this.parquetWriter.Dispose();
        }

        public void Progress(
            bool done,
            int totalNmeaLines,
            int totalAisMessages,
            int totalTicks,
            int nmeaLinesSinceLastUpdate,
            int aisMessagesSinceLastUpdate,
            int ticksSinceLastUpdate)
        {
            if (done)
            {
                Console.WriteLine(
                    "Processed {0} lines ({1} messages) in {2} seconds, {3} lines/s, {4} messages/s",
                    totalNmeaLines,
                    totalAisMessages,
                    totalTicks / 1000.0,
                    1000 * totalNmeaLines / totalTicks,
                    1000 * totalAisMessages / totalTicks);
                Console.WriteLine("Total imported: " + this.ingested);
            }
            else
            {
                Console.WriteLine(
                    "Processed {0} lines ({1} messages), current speed: {2} lines/s, {3} messages/s",
                    totalNmeaLines,
                    totalAisMessages,
                    1000 * nmeaLinesSinceLastUpdate / ticksSinceLastUpdate,
                    1000 * aisMessagesSinceLastUpdate / ticksSinceLastUpdate);
            }
        }

        private void WriteRowGroup()
        {
            using (ParquetRowGroupWriter groupWriter = this.parquetWriter.CreateRowGroup())
            {
                groupWriter.WriteColumn(new DataColumn(sourceColumnDefinition, this.sourceIds));
                groupWriter.WriteColumn(new DataColumn(timestampColumnDefinition, this.timestamps));
                groupWriter.WriteColumn(new DataColumn(mmsiColumnDefinition, this.mmsis));
                groupWriter.WriteColumn(new DataColumn(latitudeColumnDefinition, this.lats));
                groupWriter.WriteColumn(new DataColumn(longitudeColumnDefinition, this.longs));
                groupWriter.WriteColumn(new DataColumn(courseOverGroundColumnDefinition, this.courseOverGrounds));
                groupWriter.WriteColumn(new DataColumn(trueHeadingColumnDefinition, this.trueHeadings));
            }
        }
    }
}