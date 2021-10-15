package com.example.kafkasamplesstreams

import com.fasterxml.jackson.databind.ObjectMapper
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
    fun aggregateTelemetryData(): java.util.function.Function<KStream<String, String>, KStream<String, String>> {
        return java.util.function.Function<KStream<String, String>, KStream<String, String>> {
            it.transform({ StateStoreTransformer() }, STORE_NAME)
        }
    }

    @Bean
    fun aggregatedTelemetryDataStateStore(): StoreBuilder<*>? {
        return Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(STORE_NAME), Serdes.String(),
            Serdes.String()
        )
    }

    inner class StateStoreTransformer : Transformer<String, String, KeyValue<String, String>> {

        private var stateStore: KeyValueStore<String, String>? = null

        override fun init(context: ProcessorContext?) {
            stateStore = context!!.getStateStore(STORE_NAME) as KeyValueStore<String, String>
        }

        override fun transform(key: String, value: String): KeyValue<String, String> {
            // Calculate travelled distance and max speed per probe
            val telemetryData = TelemetryData(value, "currentSpeedMph", "traveledDistanceFeet")
            var maxSpeed = telemetryData.speed
            var sumDistance = telemetryData.distance

            val stateStoreTelemetryData = stateStore!!.get(key)
            if (stateStoreTelemetryData != null) {
                val aggregatedTelemetryData = TelemetryData(stateStoreTelemetryData, "maxSpeed", "sumDistance")
                maxSpeed = if (telemetryData.speed > aggregatedTelemetryData.speed) telemetryData.speed else aggregatedTelemetryData.speed
                sumDistance = telemetryData.distance + aggregatedTelemetryData.distance
            }
            val aggregatedTelemetryData = convertToJson(maxSpeed, sumDistance)

            stateStore!!.put(key, aggregatedTelemetryData)
            return KeyValue(key, aggregatedTelemetryData)
        }

        private fun convertToJson(speed: Double, distance: Double): String {
            return ObjectMapper().writeValueAsString(
                mapOf(
                    "maxSpeed" to speed.toString(),
                    "sumDistance" to distance.toString()
                )
            )
        }

        override fun close() {
            stateStore!!.close()
        }

    }
}