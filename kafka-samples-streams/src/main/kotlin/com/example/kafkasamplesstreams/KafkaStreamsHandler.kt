package com.example.kafkasamplesstreams

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

    @Bean
    fun handleStream(): java.util.function.Function<KStream<String, String>, KStream<String, String>> {
        return java.util.function.Function<KStream<String, String>, KStream<String, String>> {
            it.transform({ StateStoreTransformer() }, "exampleStore")
        }
    }

    @Bean
    fun myStore(): StoreBuilder<*>? {
        return Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore("exampleStore"), Serdes.String(),
            Serdes.String())
    }

    inner class StateStoreTransformer : Transformer<String, String, KeyValue<String, String>> {

        var exampleStore: KeyValueStore<String, String>? = null

        override fun init(context: ProcessorContext?) {
            exampleStore = context!!.getStateStore("exampleStore") as KeyValueStore<String, String>
        }

        override fun transform(key: String?, value: String?): KeyValue<String, String> {
            val valueFromStore = exampleStore!!.get(key)
            val output = if (valueFromStore != null) value.plus(",").plus(valueFromStore) else value
            exampleStore!!.put(key, output)
            return KeyValue(key, output)
        }

        override fun close() {
            exampleStore!!.close()
        }

    }
}