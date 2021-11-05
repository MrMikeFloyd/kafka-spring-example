package com.example.kafkasamplesstreams.events

import com.fasterxml.jackson.annotation.JsonProperty

/**
 * Represents a single measurement received from a space probe.
 */
data class TelemetryDataPoint(
    @JsonProperty("probeId")
    val probeId: String,
    @JsonProperty("currentSpeedMph")
    val currentSpeedMph: Double,
    @JsonProperty("traveledDistanceFeet")
    val traveledDistanceFeet: Double,
    @JsonProperty("spaceAgency")
    val spaceAgency: SpaceAgency
)
