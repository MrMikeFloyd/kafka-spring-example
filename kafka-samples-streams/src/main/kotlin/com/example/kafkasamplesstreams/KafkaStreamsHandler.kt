package com.example.kafkasamplesstreams

import com.example.kafkasamplesstreams.events.AggregatedTelemetryData
import com.example.kafkasamplesstreams.events.SpaceAgency
import com.example.kafkasamplesstreams.events.TelemetryDataPoint
import com.example.kafkasamplesstreams.serdes.AggregateTelemetryDataSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Predicate
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
            Array<KStream<String, AggregatedTelemetryData>>> {
        return java.util.function.Function<
                KStream<String, TelemetryDataPoint>,
                Array<KStream<String, AggregatedTelemetryData>>> {
            it.transform({ TelemetryAggregationTransformer() }, STORE_NAME)
                .branch(
                    Predicate { _, v -> v.spaceAgency == SpaceAgency.NASA },
                    Predicate { _, v -> v.spaceAgency == SpaceAgency.ESA }
                )
        }
    }

    @Bean
    fun aggregatedTelemetryDataStateStore(): StoreBuilder<KeyValueStore<String, AggregatedTelemetryData>> {
        return Stores.keyValueStoreBuilder(
            /**
             * Choosing an inMemory Store ensures that the kafka streams application
             * doesn't store any data on disk (via RocksDb) and our state is clear
             * when we delete the changelog topic and restart our application
             **/
            Stores.inMemoryKeyValueStore(STORE_NAME),
            Serdes.String(),
            AggregateTelemetryDataSerde()
        )
    }
}