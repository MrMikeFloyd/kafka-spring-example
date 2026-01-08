package com.example.kafkasamplesstreams

import com.example.kafkasamplesstreams.events.AggregatedTelemetryData
import com.example.kafkasamplesstreams.events.SpaceAgency
import com.example.kafkasamplesstreams.events.TelemetryDataPoint
import com.example.kafkasamplesstreams.serdes.AggregateTelemetryDataSerde
import mu.KotlinLogging
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Materialized
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

            // NASA stream with explicit state store name
            val nasaStream = telemetryRecords
                .filter { _, v -> v.spaceAgency == SpaceAgency.NASA }
                .groupByKey()
                .aggregate(
                    // KTable initializer
                    { AggregatedTelemetryData(maxSpeedMph = 0.0, traveledDistanceFeet = 0.0) },
                    // Calculation function for telemetry data aggregation
                    { probeId, lastTelemetryReading, aggregatedTelemetryData ->
                        updateTotals(probeId, lastTelemetryReading, aggregatedTelemetryData)
                    },
                    // Configure Serdes for State Store topic with explicit store name
                    Materialized.`as`<String, AggregatedTelemetryData, org.apache.kafka.streams.state.KeyValueStore<org.apache.kafka.common.utils.Bytes, ByteArray>>("nasa-aggregates")
                        .withKeySerde(Serdes.StringSerde())
                        .withValueSerde(AggregateTelemetryDataSerde())
                )
                .toStream()

            // ESA stream with explicit state store name
            val esaStream = telemetryRecords
                .filter { _, v -> v.spaceAgency == SpaceAgency.ESA }
                .groupByKey()
                .aggregate(
                    // KTable initializer
                    { AggregatedTelemetryData(maxSpeedMph = 0.0, traveledDistanceFeet = 0.0) },
                    // Calculation function for telemetry data aggregation
                    { probeId, lastTelemetryReading, aggregatedTelemetryData ->
                        updateTotals(probeId, lastTelemetryReading, aggregatedTelemetryData)
                    },
                    // Configure Serdes for State Store topic with explicit store name
                    Materialized.`as`<String, AggregatedTelemetryData, org.apache.kafka.streams.state.KeyValueStore<org.apache.kafka.common.utils.Bytes, ByteArray>>("esa-aggregates")
                        .withKeySerde(Serdes.StringSerde())
                        .withValueSerde(AggregateTelemetryDataSerde())
                )
                .toStream()

            arrayOf(nasaStream, esaStream)
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