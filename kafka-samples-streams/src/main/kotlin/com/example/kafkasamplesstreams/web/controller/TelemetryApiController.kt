package com.example.kafkasamplesstreams.web.controller

import com.example.kafkasamplesstreams.web.dto.ProbeAggregateDto
import com.example.kafkasamplesstreams.web.service.StateStoreQueryService
import com.example.kafkasamplesstreams.web.service.TelemetryUpdateConsumer
import mu.KotlinLogging
import org.springframework.http.MediaType
import org.springframework.http.codec.ServerSentEvent
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Flux
import java.time.Duration

/**
 * REST API controller for telemetry data.
 * Provides SSE endpoint for real-time updates and REST endpoint for current state.
 */
@RestController
@RequestMapping("/api/telemetry")
class TelemetryApiController(
    private val stateStoreQueryService: StateStoreQueryService,
    private val telemetryUpdateConsumer: TelemetryUpdateConsumer
) {
    private val logger = KotlinLogging.logger {}

    /**
     * Returns current state of all probes from state stores.
     * GET /api/telemetry/current
     */
    @GetMapping("/current")
    fun getCurrentState(): List<ProbeAggregateDto> {
        logger.info { "Fetching current state from state stores" }
        return stateStoreQueryService.getAllProbeAggregates()
    }

    /**
     * Server-Sent Events endpoint that streams real-time updates.
     * Sends initial state, then streams updates as they arrive.
     * Also includes periodic heartbeat to keep connection alive.
     *
     * GET /api/telemetry/stream
     */
    @GetMapping("/stream", produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
    fun streamUpdates(): Flux<ServerSentEvent<ProbeAggregateDto>> {
        logger.info { "New SSE client connected" }

        // Send initial state as first events
        val initialState = Flux.fromIterable(stateStoreQueryService.getAllProbeAggregates())
            .map { dto ->
                ServerSentEvent.builder(dto)
                    .event("initial")
                    .id(dto.probeId)
                    .build()
            }

        // Stream real-time updates
        val updates = telemetryUpdateConsumer.getUpdateStream()
            .map { dto ->
                ServerSentEvent.builder(dto)
                    .event("update")
                    .id(dto.probeId)
                    .build()
            }

        // Periodic heartbeat to keep connection alive
        val heartbeat = Flux.interval(Duration.ofSeconds(30))
            .map {
                ServerSentEvent.builder<ProbeAggregateDto>()
                    .event("heartbeat")
                    .comment("keep-alive")
                    .build()
            }

        // Combine initial state + updates + heartbeat
        return Flux.concat(
            initialState,
            Flux.merge(updates, heartbeat)
        )
    }
}
