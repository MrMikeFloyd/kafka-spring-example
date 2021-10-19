package com.example.kafkasamplesstreams

import com.example.kafkasamplesstreams.events.AggregatedTelemetryData
import com.example.kafkasamplesstreams.events.TelemetryDataPoint
import com.example.kafkasamplesstreams.serdes.AggregateTelemetryDataSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.StoreBuilder
import org.apache.kafka.streams.state.Stores
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

const val STORE_NAME = "telemetryDataStore"

@Configuration
class KafkaStreamsHandler {

    @Bean
    fun aggregateTelemetryData(): java.util.function.Function<
            KStream<String, TelemetryDataPoint>,
            KStream<String, AggregatedTelemetryData>> {
        return java.util.function.Function<
                KStream<String, TelemetryDataPoint>,
                KStream<String, AggregatedTelemetryData>> {
            it.transform({ StateStoreTransformer() }, STORE_NAME)
        }
    }

    @Bean
    fun aggregatedTelemetryDataStateStore(): StoreBuilder<*>? {
        return Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(STORE_NAME),
            Serdes.String(),
            AggregateTelemetryDataSerde()
        )
    }

    inner class StateStoreTransformer : Transformer<String, TelemetryDataPoint, KeyValue<String, AggregatedTelemetryData>> {

        private var stateStore: KeyValueStore<String, AggregatedTelemetryData>? = null

        override fun init(context: ProcessorContext?) {
            stateStore = context!!.getStateStore(STORE_NAME) as KeyValueStore<String, AggregatedTelemetryData>
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
                        value.probeId,
                        value.currentSpeedMph,
                        value.traveledDistanceFeet // TODO: Why do we never end up in this branch? Why is there always smth returned from the state store?
                    )
                    stateStore!!.put(key, initialAggregatedTelemetryData)
                    KeyValue(key, initialAggregatedTelemetryData)
                }
                else -> {
                    // State store has data for the given  probe => update it with the current measurement's data
                    val totalDistanceTraveled = value.traveledDistanceFeet + stateStoreTelemetryData.traveledDistanceFeet
                    val maxSpeed = if (value.currentSpeedMph > stateStoreTelemetryData.maxSpeedMph)
                        value.currentSpeedMph else stateStoreTelemetryData.maxSpeedMph
                    val aggregatedTelemetryData = AggregatedTelemetryData(
                        value.probeId,
                        totalDistanceTraveled,
                        maxSpeed
                    )
                    stateStore!!.put(key, aggregatedTelemetryData)
                    return KeyValue(key, aggregatedTelemetryData)
                }
            }
        }

        override fun close() {
            stateStore!!.close()
        }

    }
}