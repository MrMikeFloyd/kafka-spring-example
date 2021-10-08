package com.example.kafkasamplesstreams

import com.fasterxml.jackson.core.type.TypeReference
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


@Configuration
class KafkaStreamsHandler {

    val storeName = "telemetryDataStore"

    @Bean
    fun handleStream(): java.util.function.Function<KStream<String, String>, KStream<String, String>> {
        return java.util.function.Function<KStream<String, String>, KStream<String, String>> {
            it.transform({ StateStoreTransformer() }, storeName)
        }
    }

    @Bean
    fun myStore(): StoreBuilder<*>? {
        return Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(storeName), Serdes.String(),
            Serdes.String())
    }

    inner class StateStoreTransformer : Transformer<String, String, KeyValue<String, String>> {

        private var exampleStore: KeyValueStore<String, String>? = null

        override fun init(context: ProcessorContext?) {
            exampleStore = context!!.getStateStore(storeName) as KeyValueStore<String, String>
        }

        override fun transform(key: String?, value: String?): KeyValue<String, String> {
            // Calculate travelled distance and max speed per probe
            val typeRef: TypeReference<HashMap<String, String>> = object : TypeReference<HashMap<String, String>>() {}
            val newTelemetryData: MutableMap<String, String> = ObjectMapper().readValue(value, typeRef)
            val lastMeasuredSpeedMph: Double = newTelemetryData["currentSpeedMph"]!!.toDouble()
            val feetTravelledSinceLastMeasurement = newTelemetryData["traveledDistanceFeet"]!!.toDouble()

            val valueFromStore = exampleStore!!.get(key)

            var maxSpeedFromStore = 0.0
            var aggregatedFeetFromStore = 0.0
            if (valueFromStore != null) {
                val aggregatedTelemetryData: MutableMap<String, String> =
                    ObjectMapper().readValue(valueFromStore, typeRef)
                maxSpeedFromStore = aggregatedTelemetryData["maxSpeed"]!!.toDouble()
                aggregatedFeetFromStore = aggregatedTelemetryData["sumDistance"]!!.toDouble()
            }

            val sumDistance = feetTravelledSinceLastMeasurement + aggregatedFeetFromStore
            val maxSpeed = if (lastMeasuredSpeedMph > maxSpeedFromStore) lastMeasuredSpeedMph else maxSpeedFromStore

            val output = ObjectMapper().writeValueAsString(mapOf("sumDistance" to sumDistance.toString(),
                "maxSpeed" to maxSpeed.toString()))
            exampleStore!!.put(key, output)
            return KeyValue(key, output)
        }

        override fun close() {
            exampleStore!!.close()
        }

    }
}