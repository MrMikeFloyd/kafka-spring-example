package com.example.kafkasamplesstreams

import com.example.kafkasamplesstreams.events.AggregatedTelemetryData
import com.example.kafkasamplesstreams.events.SpaceAgency
import com.example.kafkasamplesstreams.events.TelemetryDataPoint
import com.example.kafkasamplesstreams.serdes.AggregateTelemetryDataSerde
import mu.KotlinLogging
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Predicate
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration


@Configuration
class KafkaStreamsHandler {

    private val logger = KotlinLogging.logger {}

    @Bean
    fun aggregateTelemetryData(): java.util.function.Function<
            KStream<String, TelemetryDataPoint>,
            Array<KStream<String, AggregatedTelemetryData>>> {
        return java.util.function.Function<
                KStream<String, TelemetryDataPoint>,
                Array<KStream<String, AggregatedTelemetryData>>> { telemetryRecords ->
            telemetryRecords.branch(
                // Split up the processing pipeline into 2 streams, depending on the space agency of the probe
                Predicate { _, v -> v.spaceAgency == SpaceAgency.NASA },
                Predicate { _, v -> v.spaceAgency == SpaceAgency.ESA }
            ).map { telemetryRecordsPerAgency ->
                // Apply aggregation logic on each stream separately
                telemetryRecordsPerAgency
                    .groupByKey()
                    .aggregate(
                        // KTable initializer
                        { AggregatedTelemetryData(maxSpeedMph = 0.0, traveledDistanceFeet = 0.0) },
                        // Calculation function for telemetry data aggregation
                        { probeId, lastTelemetryReading, aggregatedTelemetryData ->
                            updateTotals(
                                probeId,
                                lastTelemetryReading,
                                aggregatedTelemetryData
                            )
                        },
                        // Configure Serdes for State Store topic
                        Materialized.with(Serdes.StringSerde(), AggregateTelemetryDataSerde())
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
        lastTelemetryReading: TelemetryDataPoint,
        currentAggregatedValue: AggregatedTelemetryData
    ): AggregatedTelemetryData {
        val totalDistanceTraveled =
            lastTelemetryReading.traveledDistanceFeet + currentAggregatedValue.traveledDistanceFeet
        val maxSpeed = if (lastTelemetryReading.currentSpeedMph > currentAggregatedValue.maxSpeedMph)
            lastTelemetryReading.currentSpeedMph else currentAggregatedValue.maxSpeedMph
        val aggregatedTelemetryData = AggregatedTelemetryData(
            traveledDistanceFeet = totalDistanceTraveled,
            maxSpeedMph = maxSpeed
        )
        logger.info {
            "Calculated new aggregated telemetry data for probe $probeId. New max speed: ${aggregatedTelemetryData.maxSpeedMph} and " +
                    "traveled distance ${aggregatedTelemetryData.traveledDistanceFeet}"

        }
        return aggregatedTelemetryData
    }
}