package com.example.kafkasamplesstreams.web.service

import com.example.kafkasamplesstreams.events.AggregatedTelemetryData
import com.example.kafkasamplesstreams.web.dto.ProbeAggregateDto
import mu.KotlinLogging
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService
import org.springframework.stereotype.Service

/**
 * Service for querying Kafka Streams state stores to retrieve
 * current aggregated telemetry data for all probes.
 */
@Service
class StateStoreQueryService(
    private val interactiveQueryService: InteractiveQueryService
) {
    private val logger = KotlinLogging.logger {}

    companion object {
        const val NASA_STORE_NAME = "nasa-aggregates"
        const val ESA_STORE_NAME = "esa-aggregates"
    }

    /**
     * Retrieves all current probe aggregates from both NASA and ESA state stores.
     * Returns empty list if stores are not yet available.
     */
    fun getAllProbeAggregates(): List<ProbeAggregateDto> {
        val results = mutableListOf<ProbeAggregateDto>()

        try {
            // Query NASA store
            results.addAll(queryStore(NASA_STORE_NAME, "NASA"))

            // Query ESA store
            results.addAll(queryStore(ESA_STORE_NAME, "ESA"))

        } catch (e: Exception) {
            logger.warn { "Error querying state stores: ${e.message}. Stores may not be ready yet." }
        }

        return results
    }

    /**
     * Query a specific state store and convert entries to DTOs.
     */
    private fun queryStore(storeName: String, agency: String): List<ProbeAggregateDto> {
        return try {
            val store: ReadOnlyKeyValueStore<String, AggregatedTelemetryData> =
                interactiveQueryService.getQueryableStore(
                    storeName,
                    QueryableStoreTypes.keyValueStore()
                )

            val results = mutableListOf<ProbeAggregateDto>()
            store.all().use { iterator ->
                while (iterator.hasNext()) {
                    val entry = iterator.next()
                    results.add(
                        ProbeAggregateDto(
                            probeId = entry.key,
                            agency = agency,
                            maxSpeedMph = entry.value.maxSpeedMph,
                            traveledDistanceFeet = entry.value.traveledDistanceFeet
                        )
                    )
                }
            }

            logger.debug { "Retrieved ${results.size} probes from $agency store" }
            results

        } catch (e: Exception) {
            logger.warn { "Failed to query $agency store ($storeName): ${e.message}" }
            emptyList()
        }
    }
}
