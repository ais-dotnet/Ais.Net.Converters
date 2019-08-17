// <copyright file="ParquetExporter.cs" company="Endjin">
// Copyright (c) Endjin. All rights reserved.
// </copyright>

namespace Ais.Net.Parquet
{
    using System;
    using System.Buffers.Text;
    using System.IO;
    using Endjin.Ais;
    using global::Parquet;
    using global::Parquet.Data;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// Exprts NMEA messages to Parquet format.
    /// </summary>
    /// <remarks>
    /// This exporter writes messages types 1,2,3,5,18 and 19 to a single flat schema.
    /// </remarks>
    public class ParquetExporter : INmeaAisMessageStreamProcessor
    {
        private const int MaxRecordsPerGroup = 100_000;

        private static readonly DataField<int?> SourceColumnDefinition = new DataField<int?>("source");
        private static readonly DataField<int?> MessageTypeDefinition = new DataField<int?>("messageType");
        private static readonly DataField<long?> TimestampColumnDefinition = new DataField<long?>("timestamp");
        private static readonly DataField<int?> MmsiColumnDefinition = new DataField<int?>("mmsi");
        private static readonly DataField<int?> LatitudeColumnDefinition = new DataField<int?>("latitude");
        private static readonly DataField<int?> LongitudeColumnDefinition = new DataField<int?>("longitude");
        private static readonly DataField<int?> CourseOverGroundColumnDefinition = new DataField<int?>("courseOverGround");
        private static readonly DataField<int?> TrueHeadingColumnDefinition = new DataField<int?>("trueHeading");
        private static readonly DataField<int?> RadioSyncStateColumnDefinition = new DataField<int?>("radioSyncState");
        private static readonly DataField<bool?> RaimFlagColumnDefinition = new DataField<bool?>("raimFlag");
        private static readonly DataField<int?> SpareBitsColumnDefinition = new DataField<int?>("spareBits");
        private static readonly DataField<int?> ManoeuvreIndicatorColumnDefinition = new DataField<int?>("manoeuvreIndicator");
        private static readonly DataField<int?> RadioSlotTimeoutColumnDefinition = new DataField<int?>("radioSlotTimeout");
        private static readonly DataField<bool?> PositionAccuracyColumnDefinition = new DataField<bool?>("positionAccuracy");
        private static readonly DataField<int?> RateOfTurnColumnDefinition = new DataField<int?>("rateOfTurn");
        private static readonly DataField<int?> NavigationStatusColumnDefinition = new DataField<int?>("navigationStatus");
        private static readonly DataField<int?> RepeatIndicatorColumnDefinition = new DataField<int?>("repeatIndicator");
        private static readonly DataField<int?> RadioSubMessageColumnDefinition = new DataField<int?>("radioSubMessage");
        private static readonly DataField<bool?> CanAcceptMessage22ChannelAssignmentsColumnDefinition = new DataField<bool?>("canAcceptMessage22ChannelAssignments");
        private static readonly DataField<bool?> CanSwitchBandsColumnDefinition = new DataField<bool?>("canSwitchBands");
        private static readonly DataField<bool?> IsDscAttachedColumnDefinition = new DataField<bool?>("isDscAttached");
        private static readonly DataField<bool?> HasDisplayColumnDefinition = new DataField<bool?>("hasDisplay");
        private static readonly DataField<int?> CsUnitColumnDefinition = new DataField<int?>("csUnit");
        private static readonly DataField<int?> RegionalReserved139ColumnDefinition = new DataField<int?>("regionalReserved139");
        private static readonly DataField<int?> RadioStatusTypeColumnDefinition = new DataField<int?>("radioStatusType");
        private static readonly DataField<bool?> IsAssignedColumnDefinition = new DataField<bool?>("isAssigned");
        private static readonly DataField<int?> TimeStampSecondstaticDefinition = new DataField<int?>("timeStampSecondstatic");
        private static readonly DataField<int?> SpeedOverGroundTenthsColumnDefinition = new DataField<int?>("speedOverGroundTenths");
        private static readonly DataField<int?> RegionalReserved38ColumnDefinition = new DataField<int?>("regionalReserved38");
        private static readonly DataField<int?> PositionFixTypeColumnDefinition = new DataField<int?>("positionFixType");
        private static readonly DataField<int?> DimensionToStarboardColumnDefinition = new DataField<int?>("dimensionToStarboard");
        private static readonly DataField<int?> DimensionToPortColumnDefinition = new DataField<int?>("dimensionToPort");
        private static readonly DataField<int?> DimensionToSternColumnDefinition = new DataField<int?>("dimensionToStern");
        private static readonly DataField<int?> DimensionToBowColumnDefinition = new DataField<int?>("dimensionToBow");
        private static readonly DataField<int?> ShipTypeColumnDefinition = new DataField<int?>("shipType");
        private static readonly DataField<int?> Spare308ColumnDefinition = new DataField<int?>("spare308");
        private static readonly DataField<int?> Draught10thMetresColumnDefinition = new DataField<int?>("draught10thMetres");
        private static readonly DataField<int?> EtaMinuteColumnDefinition = new DataField<int?>("etaMinute");
        private static readonly DataField<int?> EtaHourColumnDefinition = new DataField<int?>("etaHour");
        private static readonly DataField<int?> EtaDayColumnDefinition = new DataField<int?>("etaDay");
        private static readonly DataField<int?> EtaMonthColumnDefinition = new DataField<int?>("etaMonth");
        private static readonly DataField<int?> ImoNumberColumnDefinition = new DataField<int?>("imoNumber");
        private static readonly DataField<int?> AisVersionColumnDefinition = new DataField<int?>("aisVersion");
        private static readonly DataField<bool?> IsDteNotReadyColumnDefinition = new DataField<bool?>("isDteNotReady");
        private static readonly DataField<int?> Spare423ColumnDefinition = new DataField<int?>("spare423");
        private static readonly DataField<byte[]> ShipNameColumnDefinition = new DataField<byte[]>("shipName");
        private static readonly DataField<byte[]> DestinationColumnDefinition = new DataField<byte[]>("destination");
        private static readonly DataField<byte[]> VesselNameColumnDefinition = new DataField<byte[]>("vesselName");
        private static readonly DataField<byte[]> CallSignColumnDefinition = new DataField<byte[]>("callSign");

        /// <summary>
        /// The parquet schema definition.
        /// </summary>
        private static readonly Schema Schema = new Schema(
            SourceColumnDefinition,
            MessageTypeDefinition,
            TimestampColumnDefinition,
            MmsiColumnDefinition,
            LatitudeColumnDefinition,
            LongitudeColumnDefinition,
            CourseOverGroundColumnDefinition,
            TrueHeadingColumnDefinition,
            RadioSyncStateColumnDefinition,
            RaimFlagColumnDefinition,
            SpareBitsColumnDefinition,
            ManoeuvreIndicatorColumnDefinition,
            RadioSlotTimeoutColumnDefinition,
            PositionAccuracyColumnDefinition,
            RateOfTurnColumnDefinition,
            NavigationStatusColumnDefinition,
            RepeatIndicatorColumnDefinition,
            RadioSubMessageColumnDefinition,
            CanAcceptMessage22ChannelAssignmentsColumnDefinition,
            CanSwitchBandsColumnDefinition,
            IsDscAttachedColumnDefinition,
            HasDisplayColumnDefinition,
            CsUnitColumnDefinition,
            RegionalReserved139ColumnDefinition,
            RadioStatusTypeColumnDefinition,
            IsAssignedColumnDefinition,
            TimeStampSecondstaticDefinition,
            SpeedOverGroundTenthsColumnDefinition,
            RegionalReserved38ColumnDefinition,
            PositionFixTypeColumnDefinition,
            DimensionToStarboardColumnDefinition,
            DimensionToPortColumnDefinition,
            DimensionToSternColumnDefinition,
            DimensionToBowColumnDefinition,
            ShipTypeColumnDefinition,
            Spare308ColumnDefinition,
            Draught10thMetresColumnDefinition,
            EtaMinuteColumnDefinition,
            EtaHourColumnDefinition,
            EtaDayColumnDefinition,
            EtaMonthColumnDefinition,
            ImoNumberColumnDefinition,
            AisVersionColumnDefinition,
            IsDteNotReadyColumnDefinition,
            Spare423ColumnDefinition,
            ShipNameColumnDefinition,
            DestinationColumnDefinition,
            VesselNameColumnDefinition,
            CallSignColumnDefinition);

        private readonly ParquetWriter parquetWriter;
        private readonly ILogger log;

        private int?[] sourceIds = new int?[MaxRecordsPerGroup];
        private int?[] messageTypes = new int?[MaxRecordsPerGroup];
        private long?[] timestamps = new long?[MaxRecordsPerGroup];
        private int?[] mmsis = new int?[MaxRecordsPerGroup];
        private byte[][] uuids = new byte[MaxRecordsPerGroup][];
        private int?[] lats = new int?[MaxRecordsPerGroup];
        private int?[] longs = new int?[MaxRecordsPerGroup];
        private int?[] courseOverGrounds = new int?[MaxRecordsPerGroup];
        private int?[] trueHeadings = new int?[MaxRecordsPerGroup];
        private int?[] radioSyncStates = new int?[MaxRecordsPerGroup];
        private bool?[] raimFlags = new bool?[MaxRecordsPerGroup];
        private int?[] spareBits = new int?[MaxRecordsPerGroup];
        private int?[] manoeuvreIndicators = new int?[MaxRecordsPerGroup];
        private int?[] radioSlotTimeouts = new int?[MaxRecordsPerGroup];
        private bool?[] positionAccuracies = new bool?[MaxRecordsPerGroup];
        private int?[] rateOfTurns = new int?[MaxRecordsPerGroup];
        private int?[] navigationStatuses = new int?[MaxRecordsPerGroup];
        private int?[] repeatIndicators = new int?[MaxRecordsPerGroup];
        private int?[] radioSubMessages = new int?[MaxRecordsPerGroup];
        private bool?[] canAcceptMessage22ChannelAssignments = new bool?[MaxRecordsPerGroup];
        private bool?[] canSwitchBands = new bool?[MaxRecordsPerGroup];
        private bool?[] isDscAttached = new bool?[MaxRecordsPerGroup];
        private bool?[] hasDisplay = new bool?[MaxRecordsPerGroup];
        private int?[] csUnit = new int?[MaxRecordsPerGroup];
        private int?[] regionalReserved139 = new int?[MaxRecordsPerGroup];
        private int?[] radioStatusType = new int?[MaxRecordsPerGroup];
        private bool?[] isAssigned = new bool?[MaxRecordsPerGroup];
        private int?[] timeStampSecond = new int?[MaxRecordsPerGroup];
        private int?[] speedOverGroundTenths = new int?[MaxRecordsPerGroup];
        private int?[] regionalReserved38 = new int?[MaxRecordsPerGroup];
        private int?[] positionFixType = new int?[MaxRecordsPerGroup];
        private int?[] dimensionToStarboard = new int?[MaxRecordsPerGroup];
        private int?[] dimensionToPort = new int?[MaxRecordsPerGroup];
        private int?[] dimensionToStern = new int?[MaxRecordsPerGroup];
        private int?[] dimensionToBow = new int?[MaxRecordsPerGroup];
        private int?[] shipType = new int?[MaxRecordsPerGroup];
        private int?[] spare308 = new int?[MaxRecordsPerGroup];
        private int?[] draught10thMetres = new int?[MaxRecordsPerGroup];
        private int?[] etaMinute = new int?[MaxRecordsPerGroup];
        private int?[] etaHour = new int?[MaxRecordsPerGroup];
        private int?[] etaDay = new int?[MaxRecordsPerGroup];
        private int?[] etaMonth = new int?[MaxRecordsPerGroup];
        private int?[] imoNumber = new int?[MaxRecordsPerGroup];
        private int?[] aisVersion = new int?[MaxRecordsPerGroup];
        private bool?[] isDteNotReady = new bool?[MaxRecordsPerGroup];
        private int?[] spare423 = new int?[MaxRecordsPerGroup];
        private byte[][] shipName = new byte[MaxRecordsPerGroup][];
        private byte[][] destination = new byte[MaxRecordsPerGroup][];
        private byte[][] vesselName = new byte[MaxRecordsPerGroup][];
        private byte[][] callSign = new byte[MaxRecordsPerGroup][];

        private int indexInGroup = 0;
        private int ingested = 0;

        /// <summary>
        /// Initializes a new instance of the <see cref="ParquetExporter"/> class.
        /// </summary>
        /// <param name="log">An <see cref="ILogger"/> for logging trace messags.</param>
        /// <param name="output">The output stream to write the parquet file.</param>
        public ParquetExporter(ILogger log, Stream output)
        {
            this.parquetWriter = new ParquetWriter(Schema, output);
            this.log = log;

            InitializeTextByteArrayColumn(ref this.shipName, 120);
            InitializeTextByteArrayColumn(ref this.destination, 120);
            InitializeTextByteArrayColumn(ref this.vesselName, 120);
            InitializeTextByteArrayColumn(ref this.callSign, 42);
        }

        /// <inheritdoc/>
        public void OnNext(
            in NmeaLineParser firstLine,
            in ReadOnlySpan<byte> asciiPayload,
            uint padding)
        {
            int messageType = NmeaPayloadParser.PeekMessageType(asciiPayload, padding);
            if (messageType >= 1 && messageType <= 3)
            {
                var parsedRecord = new NmeaAisPositionReportClassAParser(asciiPayload, padding);
                this.WriteRow(
                    firstLine.TagBlock,
                    parsedRecord.MessageType,
                    mmsi: parsedRecord.Mmsi,
                    timeStampSecond: parsedRecord.TimeStampSecond,
                    latitude10000thMins: parsedRecord.Latitude10000thMins,
                    longitude10000thMins: parsedRecord.Longitude10000thMins,
                    courseOverGround10thDegrees: parsedRecord.CourseOverGround10thDegrees,
                    trueHeadingDegrees: parsedRecord.TrueHeadingDegrees,
                    radioSyncState: parsedRecord.RadioSyncState,
                    raimFlag: parsedRecord.RaimFlag,
                    spareBits145: parsedRecord.SpareBits145,
                    manoeuvreIndicator: parsedRecord.ManoeuvreIndicator,
                    radioSlotTimeout: parsedRecord.RadioSlotTimeout,
                    positionAccuracy: parsedRecord.PositionAccuracy,
                    rateOfTurn: parsedRecord.RateOfTurn,
                    navigationStatus: parsedRecord.NavigationStatus,
                    repeatIndicator: parsedRecord.RepeatIndicator,
                    radioSubMessage: parsedRecord.RadioSubMessage,
                    speedOverGroundTenths: parsedRecord.SpeedOverGroundTenths);
            }

            

            if (messageType == 18)
            {
                var parsedRecord = new NmeaAisPositionReportClassBParser(asciiPayload, padding);
                this.WriteRow(
                    firstLine.TagBlock,
                    parsedRecord.MessageType,
                    mmsi: parsedRecord.Mmsi,
                    latitude10000thMins: parsedRecord.Latitude10000thMins,
                    longitude10000thMins: parsedRecord.Longitude10000thMins,
                    courseOverGround10thDegrees: parsedRecord.CourseOverGround10thDegrees,
                    trueHeadingDegrees: parsedRecord.TrueHeadingDegrees,
                    raimFlag: parsedRecord.RaimFlag,
                    positionAccuracy: parsedRecord.PositionAccuracy,
                    repeatIndicator: parsedRecord.RepeatIndicator,
                    isAssigned: parsedRecord.IsAssigned,
                    canAcceptMessage22ChannelAssignment: parsedRecord.CanAcceptMessage22ChannelAssignment,
                    canSwitchBands: parsedRecord.CanSwitchBands,
                    isDscAttached: parsedRecord.IsDscAttached,
                    hasDisplay: parsedRecord.HasDisplay,
                    csUnit: parsedRecord.CsUnit,
                    regionalReserved139: parsedRecord.RegionalReserved139,
                    timeStampSecond: parsedRecord.TimeStampSecond,
                    speedOverGroundTenths: parsedRecord.SpeedOverGroundTenths,
                    regionalReserved38: parsedRecord.RegionalReserved38,
                    radioStatusType: parsedRecord.RadioStatusType);
            }

            if (messageType == 19)
            {
                var parsedRecord = new NmeaAisPositionReportExtendedClassBParser(asciiPayload, padding);
                this.WriteRow(
                    firstLine.TagBlock,
                    parsedRecord.MessageType,
                    mmsi: parsedRecord.Mmsi,
                    latitude10000thMins: parsedRecord.Latitude10000thMins,
                    longitude10000thMins: parsedRecord.Longitude10000thMins,
                    courseOverGround10thDegrees: parsedRecord.CourseOverGround10thDegrees,
                    trueHeadingDegrees: parsedRecord.TrueHeadingDegrees,
                    raimFlag: parsedRecord.RaimFlag,
                    positionAccuracy: parsedRecord.PositionAccuracy,
                    repeatIndicator: parsedRecord.RepeatIndicator,
                    isAssigned: parsedRecord.IsAssigned,
                    regionalReserved139: parsedRecord.RegionalReserved139,
                    timeStampSecond: parsedRecord.TimeStampSecond,
                    speedOverGroundTenths: parsedRecord.SpeedOverGroundTenths,
                    regionalReserved38: parsedRecord.RegionalReserved38,
                    positionFixType: parsedRecord.PositionFixType,
                    dimensionToStarboard: parsedRecord.DimensionToStarboard,
                    dimensionToPort: parsedRecord.DimensionToPort,
                    dimensionToStern: parsedRecord.DimensionToStern,
                    dimensionToBow: parsedRecord.DimensionToBow,
                    shipType: parsedRecord.ShipType,
                    shipName: parsedRecord.ShipName,
                    spare308: parsedRecord.Spare308);
            }
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
            if (this.indexInGroup != 0)
            {
                Array.Resize(ref this.sourceIds, this.indexInGroup);
                Array.Resize(ref this.messageTypes, this.indexInGroup);
                Array.Resize(ref this.timestamps, this.indexInGroup);
                Array.Resize(ref this.mmsis, this.indexInGroup);
                Array.Resize(ref this.uuids, this.indexInGroup);
                Array.Resize(ref this.lats, this.indexInGroup);
                Array.Resize(ref this.longs, this.indexInGroup);
                Array.Resize(ref this.courseOverGrounds, this.indexInGroup);
                Array.Resize(ref this.trueHeadings, this.indexInGroup);
                Array.Resize(ref this.radioSyncStates, this.indexInGroup);
                Array.Resize(ref this.raimFlags, this.indexInGroup);
                Array.Resize(ref this.spareBits, this.indexInGroup);
                Array.Resize(ref this.manoeuvreIndicators, this.indexInGroup);
                Array.Resize(ref this.radioSlotTimeouts, this.indexInGroup);
                Array.Resize(ref this.positionAccuracies, this.indexInGroup);
                Array.Resize(ref this.rateOfTurns, this.indexInGroup);
                Array.Resize(ref this.navigationStatuses, this.indexInGroup);
                Array.Resize(ref this.repeatIndicators, this.indexInGroup);
                Array.Resize(ref this.radioSubMessages, this.indexInGroup);
                Array.Resize(ref this.canAcceptMessage22ChannelAssignments, this.indexInGroup);
                Array.Resize(ref this.canSwitchBands, this.indexInGroup);
                Array.Resize(ref this.isDscAttached, this.indexInGroup);
                Array.Resize(ref this.hasDisplay, this.indexInGroup);
                Array.Resize(ref this.csUnit, this.indexInGroup);
                Array.Resize(ref this.regionalReserved139, this.indexInGroup);
                Array.Resize(ref this.radioStatusType, this.indexInGroup);
                Array.Resize(ref this.isAssigned, this.indexInGroup);
                Array.Resize(ref this.timeStampSecond, this.indexInGroup);
                Array.Resize(ref this.speedOverGroundTenths, this.indexInGroup);
                Array.Resize(ref this.regionalReserved38, this.indexInGroup);
                Array.Resize(ref this.positionFixType, this.indexInGroup);
                Array.Resize(ref this.dimensionToStarboard, this.indexInGroup);
                Array.Resize(ref this.dimensionToPort, this.indexInGroup);
                Array.Resize(ref this.dimensionToStern, this.indexInGroup);
                Array.Resize(ref this.dimensionToBow, this.indexInGroup);
                Array.Resize(ref this.shipType, this.indexInGroup);
                Array.Resize(ref this.spare308, this.indexInGroup);
                Array.Resize(ref this.draught10thMetres, this.indexInGroup);
                Array.Resize(ref this.etaMinute, this.indexInGroup);
                Array.Resize(ref this.etaHour, this.indexInGroup);
                Array.Resize(ref this.etaDay, this.indexInGroup);
                Array.Resize(ref this.etaMonth, this.indexInGroup);
                Array.Resize(ref this.imoNumber, this.indexInGroup);
                Array.Resize(ref this.aisVersion, this.indexInGroup);
                Array.Resize(ref this.isDteNotReady, this.indexInGroup);
                Array.Resize(ref this.spare423, this.indexInGroup);
                Array.Resize(ref this.shipName, this.indexInGroup);
                Array.Resize(ref this.destination, this.indexInGroup);
                Array.Resize(ref this.vesselName, this.indexInGroup);
                Array.Resize(ref this.callSign, this.indexInGroup);

                this.WriteRowGroup();
            }

            this.parquetWriter.Dispose();
        }

        /// <inheritdoc/>
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
                this.log.LogInformation("Total imported: " + this.ingested);
            }
            else
            {
                this.log.LogInformation(
                    "Processed {0} lines ({1} messages), current speed: {2} lines/s, {3} messages/s",
                    totalNmeaLines,
                    totalAisMessages,
                    1000 * nmeaLinesSinceLastUpdate / ticksSinceLastUpdate,
                    1000 * aisMessagesSinceLastUpdate / ticksSinceLastUpdate);
            }
        }

        private static void InitializeTextByteArrayColumn(ref byte[][] column, uint bitLength)
        {
            if (bitLength % 6 != 0)
            {
                throw new ArgumentException("AIS stores all text with 6 bits per character, # bits must be divisible by 6", nameof(bitLength));
            }

            for (int i = 0; i < column.Length; i++)
            {
                column[i] = new byte[bitLength / 6];
            }
        }

        private static void WriteTextBytesOrZeroOutBytes(NmeaAisTextFieldParser textFieldParser, ref byte[] buffer)
        {
            if (textFieldParser.CharacterCount == 0)
            {
                Array.Clear(buffer, 0, buffer.Length);
            }
            else
            {
                textFieldParser.WriteAsAscii(buffer);
            }
        }

        private void WriteRow(
            in NmeaTagBlockParser tagBlock,
            uint messageType,
            uint? mmsi = null,
            int? latitude10000thMins = null,
            int? longitude10000thMins = null,
            uint? courseOverGround10thDegrees = null,
            uint? trueHeadingDegrees = null,
            RadioSyncState? radioSyncState = null,
            bool? raimFlag = null,
            uint? spareBits145 = null,
            ManoeuvreIndicator? manoeuvreIndicator = null,
            uint? radioSlotTimeout = null,
            bool? positionAccuracy = null,
            int? rateOfTurn = null,
            NavigationStatus? navigationStatus = null,
            uint? repeatIndicator = null,
            uint? radioSubMessage = null,
            bool? canAcceptMessage22ChannelAssignment = null,
            bool? canSwitchBands = null,
            bool? isDscAttached = null,
            bool? hasDisplay = null,
            ClassBUnit? csUnit = null,
            byte? regionalReserved139 = null,
            ClassBRadioStatusType? radioStatusType = null,
            bool? isAssigned = false,
            uint? timeStampSecond = null,
            uint? speedOverGroundTenths = null,
            byte? regionalReserved38 = null,
            EpfdFixType? positionFixType = null,
            uint? dimensionToStarboard = null,
            uint? dimensionToPort = null,
            uint? dimensionToStern = null,
            uint? dimensionToBow = null,
            ShipType? shipType = null,
            NmeaAisTextFieldParser shipName = default,
            uint? spare308 = null,
            NmeaAisTextFieldParser destination = default,
            uint? draught10thMetres = null,
            uint? etaMinute = null,
            uint? etaHour = null,
            uint? etaDay = null,
            uint? etaMonth = null,
            NmeaAisTextFieldParser vesselName = default,
            NmeaAisTextFieldParser callSign = default,
            uint? imoNumber = null,
            uint? aisVersion = null,
            bool? isDteNotReady = null,
            uint? spare423 = null)
        {
            this.sourceIds[this.indexInGroup] = Utf8Parser.TryParse(tagBlock.Source, out int id, out _) ? id : 0;
            this.messageTypes[this.indexInGroup] = (int?)messageType;
            this.timestamps[this.indexInGroup] = tagBlock.UnixTimestamp.Value;
            this.mmsis[this.indexInGroup] = (int?)mmsi;
            this.lats[this.indexInGroup] = latitude10000thMins;
            this.longs[this.indexInGroup] = longitude10000thMins;
            this.courseOverGrounds[this.indexInGroup] = (int?)courseOverGround10thDegrees;
            this.trueHeadings[this.indexInGroup] = (int?)trueHeadingDegrees;
            this.radioSyncStates[this.indexInGroup] = (int?)radioSyncState;
            this.raimFlags[this.indexInGroup] = raimFlag;
            this.spareBits[this.indexInGroup] = (int?)spareBits145;
            this.manoeuvreIndicators[this.indexInGroup] = (int?)manoeuvreIndicator;
            this.radioSlotTimeouts[this.indexInGroup] = (int?)radioSlotTimeout;
            this.positionAccuracies[this.indexInGroup] = positionAccuracy;
            this.rateOfTurns[this.indexInGroup] = rateOfTurn;
            this.navigationStatuses[this.indexInGroup] = (int?)navigationStatus;
            this.repeatIndicators[this.indexInGroup] = (int?)repeatIndicator;
            this.radioSubMessages[this.indexInGroup] = (int?)radioSubMessage;
            this.canAcceptMessage22ChannelAssignments[this.indexInGroup] = canAcceptMessage22ChannelAssignment;
            this.canSwitchBands[this.indexInGroup] = canSwitchBands;
            this.isDscAttached[this.indexInGroup] = isDscAttached;
            this.hasDisplay[this.indexInGroup] = hasDisplay;
            this.csUnit[this.indexInGroup] = (int?)csUnit;
            this.regionalReserved139[this.indexInGroup] = regionalReserved139;
            this.radioStatusType[this.indexInGroup] = (int?)radioStatusType;
            this.isAssigned[this.indexInGroup] = isAssigned;
            this.timeStampSecond[this.indexInGroup] = (int?)timeStampSecond;
            this.speedOverGroundTenths[this.indexInGroup] = (int?)speedOverGroundTenths;
            this.regionalReserved38[this.indexInGroup] = regionalReserved38;
            this.positionFixType[this.indexInGroup] = (int?)positionFixType;
            this.dimensionToStarboard[this.indexInGroup] = (int?)dimensionToStarboard;
            this.dimensionToPort[this.indexInGroup] = (int?)dimensionToPort;
            this.dimensionToStern[this.indexInGroup] = (int?)dimensionToStern;
            this.dimensionToBow[this.indexInGroup] = (int?)dimensionToBow;
            this.shipType[this.indexInGroup] = (int?)shipType;
            WriteTextBytesOrZeroOutBytes(shipName, ref this.shipName[this.indexInGroup]);
            this.spare308[this.indexInGroup] = (int?)spare308;
            WriteTextBytesOrZeroOutBytes(destination, ref this.destination[this.indexInGroup]);
            this.draught10thMetres[this.indexInGroup] = (int?)draught10thMetres;
            this.etaMinute[this.indexInGroup] = (int?)etaMinute;
            this.etaHour[this.indexInGroup] = (int?)etaHour;
            this.etaDay[this.indexInGroup] = (int?)etaDay;
            this.etaMonth[this.indexInGroup] = (int?)etaMonth;
            WriteTextBytesOrZeroOutBytes(vesselName, ref this.vesselName[this.indexInGroup]);
            WriteTextBytesOrZeroOutBytes(callSign, ref this.callSign[this.indexInGroup]);
            this.imoNumber[this.indexInGroup] = (int?)imoNumber;
            this.aisVersion[this.indexInGroup] = (int?)aisVersion;
            this.isDteNotReady[this.indexInGroup] = isDteNotReady;
            this.spare423[this.indexInGroup] = (int?)spare423;

            if (++this.indexInGroup == MaxRecordsPerGroup)
            {
                this.WriteRowGroup();
                this.indexInGroup = 0;
            }

            this.ingested += 1;
        }

        private void WriteRowGroup()
        {
            using (ParquetRowGroupWriter groupWriter = this.parquetWriter.CreateRowGroup())
            {
                groupWriter.WriteColumn(new DataColumn(SourceColumnDefinition, this.sourceIds));
                groupWriter.WriteColumn(new DataColumn(MessageTypeDefinition, this.messageTypes));
                groupWriter.WriteColumn(new DataColumn(TimestampColumnDefinition, this.timestamps));
                groupWriter.WriteColumn(new DataColumn(MmsiColumnDefinition, this.mmsis));
                groupWriter.WriteColumn(new DataColumn(LatitudeColumnDefinition, this.lats));
                groupWriter.WriteColumn(new DataColumn(LongitudeColumnDefinition, this.longs));
                groupWriter.WriteColumn(new DataColumn(CourseOverGroundColumnDefinition, this.courseOverGrounds));
                groupWriter.WriteColumn(new DataColumn(TrueHeadingColumnDefinition, this.trueHeadings));
                groupWriter.WriteColumn(new DataColumn(RadioSyncStateColumnDefinition, this.radioSyncStates));
                groupWriter.WriteColumn(new DataColumn(RaimFlagColumnDefinition, this.raimFlags));
                groupWriter.WriteColumn(new DataColumn(SpareBitsColumnDefinition, this.spareBits));
                groupWriter.WriteColumn(new DataColumn(ManoeuvreIndicatorColumnDefinition, this.manoeuvreIndicators));
                groupWriter.WriteColumn(new DataColumn(RadioSlotTimeoutColumnDefinition, this.radioSlotTimeouts));
                groupWriter.WriteColumn(new DataColumn(PositionAccuracyColumnDefinition, this.positionAccuracies));
                groupWriter.WriteColumn(new DataColumn(RateOfTurnColumnDefinition, this.rateOfTurns));
                groupWriter.WriteColumn(new DataColumn(NavigationStatusColumnDefinition, this.navigationStatuses));
                groupWriter.WriteColumn(new DataColumn(RepeatIndicatorColumnDefinition, this.repeatIndicators));
                groupWriter.WriteColumn(new DataColumn(RadioSubMessageColumnDefinition, this.radioSubMessages));
                groupWriter.WriteColumn(new DataColumn(CanAcceptMessage22ChannelAssignmentsColumnDefinition, this.canAcceptMessage22ChannelAssignments));
                groupWriter.WriteColumn(new DataColumn(CanSwitchBandsColumnDefinition, this.canSwitchBands));
                groupWriter.WriteColumn(new DataColumn(IsDscAttachedColumnDefinition, this.isDscAttached));
                groupWriter.WriteColumn(new DataColumn(HasDisplayColumnDefinition, this.hasDisplay));
                groupWriter.WriteColumn(new DataColumn(CsUnitColumnDefinition, this.csUnit));
                groupWriter.WriteColumn(new DataColumn(RegionalReserved139ColumnDefinition, this.regionalReserved139));
                groupWriter.WriteColumn(new DataColumn(RadioStatusTypeColumnDefinition, this.radioStatusType));
                groupWriter.WriteColumn(new DataColumn(IsAssignedColumnDefinition, this.isAssigned));
                groupWriter.WriteColumn(new DataColumn(TimeStampSecondstaticDefinition, this.timeStampSecond));
                groupWriter.WriteColumn(new DataColumn(SpeedOverGroundTenthsColumnDefinition, this.speedOverGroundTenths));
                groupWriter.WriteColumn(new DataColumn(RegionalReserved38ColumnDefinition, this.regionalReserved38));
                groupWriter.WriteColumn(new DataColumn(PositionFixTypeColumnDefinition, this.positionFixType));
                groupWriter.WriteColumn(new DataColumn(DimensionToStarboardColumnDefinition, this.dimensionToStarboard));
                groupWriter.WriteColumn(new DataColumn(DimensionToPortColumnDefinition, this.dimensionToPort));
                groupWriter.WriteColumn(new DataColumn(DimensionToSternColumnDefinition, this.dimensionToStern));
                groupWriter.WriteColumn(new DataColumn(DimensionToBowColumnDefinition, this.dimensionToBow));
                groupWriter.WriteColumn(new DataColumn(ShipTypeColumnDefinition, this.shipType));
                groupWriter.WriteColumn(new DataColumn(Spare308ColumnDefinition, this.spare308));
                groupWriter.WriteColumn(new DataColumn(Draught10thMetresColumnDefinition, this.draught10thMetres));
                groupWriter.WriteColumn(new DataColumn(EtaMinuteColumnDefinition, this.etaMinute));
                groupWriter.WriteColumn(new DataColumn(EtaHourColumnDefinition, this.etaHour));
                groupWriter.WriteColumn(new DataColumn(EtaDayColumnDefinition, this.etaDay));
                groupWriter.WriteColumn(new DataColumn(EtaMonthColumnDefinition, this.etaMonth));
                groupWriter.WriteColumn(new DataColumn(ImoNumberColumnDefinition, this.imoNumber));
                groupWriter.WriteColumn(new DataColumn(AisVersionColumnDefinition, this.aisVersion));
                groupWriter.WriteColumn(new DataColumn(IsDteNotReadyColumnDefinition, this.isDteNotReady));
                groupWriter.WriteColumn(new DataColumn(Spare423ColumnDefinition, this.spare423));
                groupWriter.WriteColumn(new DataColumn(ShipNameColumnDefinition, this.shipName));
                groupWriter.WriteColumn(new DataColumn(DestinationColumnDefinition, this.destination));
                groupWriter.WriteColumn(new DataColumn(VesselNameColumnDefinition, this.vesselName));
                groupWriter.WriteColumn(new DataColumn(CallSignColumnDefinition, this.callSign));
            }
        }
    }
}