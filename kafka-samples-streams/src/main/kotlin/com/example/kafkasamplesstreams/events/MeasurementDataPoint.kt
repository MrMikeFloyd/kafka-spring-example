package com.example.kafkasamplesstreams.events

import com.fasterxml.jackson.annotation.JsonProperty

/**
 * Represents a single measurement received from a space probe.
 */
data class MeasurementDataPoint(
    @JsonProperty("probeId")
    val probeId: String,
    @JsonProperty("radiation")
    val radiation: Double,
    @JsonProperty("spaceAgency")
    val spaceAgency: SpaceAgency
)
