package com.example.kafkasamplesstreams

import com.example.kafkasamplesstreams.events.*
import com.example.kafkasamplesstreams.serdes.AggregatedFullProbeDataSerde
import com.example.kafkasamplesstreams.serdes.MeasurementDataPointSerde
import com.example.kafkasamplesstreams.serdes.TelemetryDataPointSerde
import mu.KotlinLogging
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Predicate
import org.apache.kafka.streams.state.KeyValueStore
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration


@Configuration
class KafkaStreamsHandler {

    private val logger = KotlinLogging.logger {}

    /**
     * Joins 2 Streams of data for the space probes using KTables.
     * After joining, the data is aggregated to calculate max/total values per probe
     * and then passed on downstream for the consumer service
     */
    @Bean
    fun aggregateTelemetryData(): java.util.function.BiFunction<
            KStream<String, TelemetryDataPoint>,
            KStream<String, MeasurementDataPoint>,
            Array<KStream<String, AggregatedFullProbeMeasurementData>>> {
        return java.util.function.BiFunction<
                // We have 2 inbound streams, telemetry data and measurement data per probe
                KStream<String, TelemetryDataPoint>,
                KStream<String, MeasurementDataPoint>,
                Array<KStream<String, AggregatedFullProbeMeasurementData>>> { telemetryRecords, measurementRecords ->
            // materialize both streams as KTables
            val telemetryDataTable = telemetryRecords.toTable(
                Materialized
                    .`as`<String, TelemetryDataPoint, KeyValueStore<Bytes, ByteArray>>("telemetry-data")
                    .withKeySerde(Serdes.StringSerde())
                    .withValueSerde(TelemetryDataPointSerde())
            )
            val measurementDataTable = measurementRecords.toTable(
                Materialized
                    .`as`<String, MeasurementDataPoint, KeyValueStore<Bytes, ByteArray>>("measurement-data")
                    .withKeySerde(Serdes.StringSerde())
                    .withValueSerde(MeasurementDataPointSerde())
            )
            // Join both tables (default: By key), provide a ValueJoiner that merges both records into one
            telemetryDataTable.join(measurementDataTable) { telemetryRecord, measurementRecord ->
                FullProbeMeasurement(
                    probeId = telemetryRecord.probeId,
                    radiation = measurementRecord.radiation,
                    currentSpeedMph = telemetryRecord.currentSpeedMph,
                    traveledDistanceFeet = telemetryRecord.traveledDistanceFeet,
                    spaceAgency = telemetryRecord.spaceAgency
                )
            }.toStream() // convert the resulting KTable into a KStream
                .branch(
                    // Split up the stream into 2 streams, depending on the space agency of the probe
                    Predicate { _, v -> v.spaceAgency == SpaceAgency.NASA },
                    Predicate { _, v -> v.spaceAgency == SpaceAgency.ESA }
                ).map { telemetryRecordsPerAgency ->
                    // Apply aggregation logic on each stream separately
                    telemetryRecordsPerAgency
                        .groupByKey()
                        .aggregate(
                            // initialize another KTable for performing the aggregation
                            { AggregatedFullProbeMeasurementData(maxSpeedMph = 0.0, traveledDistanceFeet = 0.0, highestRadiation = 0.0) },
                            // Calculation function for telemetry data aggregation
                            { probeId, lastTelemetryReading, aggregatedTelemetryData ->
                                updateTotals(
                                    probeId,
                                    lastTelemetryReading,
                                    aggregatedTelemetryData
                                )
                            },
                            // Configure Serdes for State Store topic
                            Materialized.with(Serdes.StringSerde(), AggregatedFullProbeDataSerde())
                        )
                        .toStream()
                }.toTypedArray()
        }
    }

    /**
     * Performs calculation of per-probe aggregate measurement data.
     * The currently calculated totals are held in a Kafka State Store
     * backing the KTable created with aggregate() and the most recently
     * created aggregate telemetry data record is passed on downstream.
     */
    fun updateTotals(
        probeId: String,
        lastTelemetryReading: FullProbeMeasurement,
        currentAggregatedValue: AggregatedFullProbeMeasurementData
    ): AggregatedFullProbeMeasurementData {
        val totalDistanceTraveled =
            lastTelemetryReading.traveledDistanceFeet + currentAggregatedValue.traveledDistanceFeet
        val maxSpeed = if (lastTelemetryReading.currentSpeedMph > currentAggregatedValue.maxSpeedMph)
            lastTelemetryReading.currentSpeedMph else currentAggregatedValue.maxSpeedMph
        val maxRadiation = if (lastTelemetryReading.radiation > currentAggregatedValue.highestRadiation)
            lastTelemetryReading.radiation else currentAggregatedValue.highestRadiation
        val aggregatedTelemetryData = AggregatedFullProbeMeasurementData(
            traveledDistanceFeet = totalDistanceTraveled,
            maxSpeedMph = maxSpeed,
            highestRadiation = maxRadiation
        )
        logger.info {
            "Calculated new full aggregated telemetry data for probe $probeId. New max speed: ${aggregatedTelemetryData.maxSpeedMph} and " +
                    "traveled distance ${aggregatedTelemetryData.traveledDistanceFeet}, maximum radiation: ${aggregatedTelemetryData.highestRadiation}"

        }
        return aggregatedTelemetryData
    }
}