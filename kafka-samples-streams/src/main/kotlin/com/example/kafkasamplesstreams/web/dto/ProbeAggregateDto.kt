package com.example.kafkasamplesstreams.web.dto

import com.fasterxml.jackson.annotation.JsonProperty

/**
 * Data transfer object for web API representing a probe's aggregate data
 * with additional metadata for display purposes.
 */
data class ProbeAggregateDto(
    @JsonProperty("probeId")
    val probeId: String,

    @JsonProperty("agency")
    val agency: String, // "NASA" or "ESA"

    @JsonProperty("maxSpeedMph")
    val maxSpeedMph: Double,

    @JsonProperty("traveledDistanceFeet")
    val traveledDistanceFeet: Double,

    @JsonProperty("lastUpdated")
    val lastUpdated: Long = System.currentTimeMillis()
)
