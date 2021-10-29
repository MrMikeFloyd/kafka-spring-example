package com.example.kafkasamplesstreams

import com.example.kafkasamplesstreams.events.AggregatedTelemetryData
import com.example.kafkasamplesstreams.events.TelemetryDataPoint
import mu.KotlinLogging
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore

class TelemetryAggregationTransformer :
    Transformer<String, TelemetryDataPoint, KeyValue<String, AggregatedTelemetryData>> {

    private val logger = KotlinLogging.logger {}

    private var stateStore: KeyValueStore<String, AggregatedTelemetryData>? = null

    override fun init(context: ProcessorContext?) {
        stateStore = context!!.getStateStore(STORE_NAME) as KeyValueStore<String, AggregatedTelemetryData>
        logger.info { "Initialized State Store with ${stateStore!!.approximateNumEntries()} entries." }
    }

    /**
     * Performs calculation of per-probe aggregate measurement data.
     * The currently calculated totals are held in the Kafka State Store and the most recently
     * created aggregate telemetry data record is passed on downstream.
     */
    override fun transform(key: String, value: TelemetryDataPoint): KeyValue<String, AggregatedTelemetryData> {
        return when (val stateStoreTelemetryData = stateStore!!.get(key)) {
            null -> {
                // No data in state store for the given probe => initialize it
                val initialAggregatedTelemetryData = AggregatedTelemetryData(
                    probeId = value.probeId,
                    maxSpeedMph = value.currentSpeedMph,
                    traveledDistanceFeet = value.traveledDistanceFeet
                )
                updateStoreAndForwardData(initialAggregatedTelemetryData)
            }
            else -> {
                // State store has data for the given  probe => update it with the current measurement's data
                val totalDistanceTraveled =
                    value.traveledDistanceFeet + stateStoreTelemetryData.traveledDistanceFeet
                val maxSpeed = if (value.currentSpeedMph > stateStoreTelemetryData.maxSpeedMph)
                    value.currentSpeedMph else stateStoreTelemetryData.maxSpeedMph
                val aggregatedTelemetryData = AggregatedTelemetryData(
                    probeId = value.probeId,
                    maxSpeedMph = maxSpeed,
                    traveledDistanceFeet = totalDistanceTraveled
                )
                logger.info {
                    "Calculated new aggregated telemetry data for probe $key. New max speed: ${aggregatedTelemetryData.maxSpeedMph} and " +
                            "travelled distance ${aggregatedTelemetryData.traveledDistanceFeet}"
                }
                updateStoreAndForwardData(aggregatedTelemetryData)
            }
        }
    }

    private fun updateStoreAndForwardData(telemetryData: AggregatedTelemetryData): KeyValue<String, AggregatedTelemetryData> {
        stateStore!!.put(telemetryData.probeId, telemetryData)
        return KeyValue(telemetryData.probeId, telemetryData)
    }

    override fun close() {}
}