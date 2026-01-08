package com.example.kafkasamplesstreams.web.service

import com.example.kafkasamplesstreams.events.AggregatedTelemetryData
import com.example.kafkasamplesstreams.web.dto.ProbeAggregateDto
import mu.KotlinLogging
import org.apache.kafka.streams.kstream.KStream
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import reactor.core.publisher.Flux
import reactor.core.publisher.Sinks
import java.util.function.Consumer

/**
 * Kafka consumer that listens to aggregated telemetry data topics
 * and broadcasts updates to connected SSE clients.
 */
@Configuration
class TelemetryUpdateConsumer {

    private val logger = KotlinLogging.logger {}

    // Multi-cast sink that can emit to multiple subscribers
    private val updateSink = Sinks.many().multicast().onBackpressureBuffer<ProbeAggregateDto>()

    /**
     * Exposes the update stream as a Flux for SSE subscribers.
     */
    fun getUpdateStream(): Flux<ProbeAggregateDto> {
        return updateSink.asFlux()
    }

    /**
     * Consumer for NASA aggregated telemetry data.
     * Bindings configured in application.yml as:
     * consumeNasaAggregates-in-0 -> space-probe-aggregate-telemetry-data-nasa
     */
    @Bean
    fun consumeNasaAggregates(): Consumer<KStream<String, AggregatedTelemetryData>> {
        return Consumer { stream ->
            stream.foreach { probeId, aggregatedData ->
                val dto = ProbeAggregateDto(
                    probeId = probeId,
                    agency = "NASA",
                    maxSpeedMph = aggregatedData.maxSpeedMph,
                    traveledDistanceFeet = aggregatedData.traveledDistanceFeet
                )

                logger.debug { "Broadcasting NASA update for probe: $probeId" }
                updateSink.tryEmitNext(dto)
            }
        }
    }

    /**
     * Consumer for ESA aggregated telemetry data.
     * Bindings configured in application.yml as:
     * consumeEsaAggregates-in-0 -> space-probe-aggregate-telemetry-data-esa
     */
    @Bean
    fun consumeEsaAggregates(): Consumer<KStream<String, AggregatedTelemetryData>> {
        return Consumer { stream ->
            stream.foreach { probeId, aggregatedData ->
                val dto = ProbeAggregateDto(
                    probeId = probeId,
                    agency = "ESA",
                    maxSpeedMph = aggregatedData.maxSpeedMph,
                    traveledDistanceFeet = aggregatedData.traveledDistanceFeet
                )

                logger.debug { "Broadcasting ESA update for probe: $probeId" }
                updateSink.tryEmitNext(dto)
            }
        }
    }
}
