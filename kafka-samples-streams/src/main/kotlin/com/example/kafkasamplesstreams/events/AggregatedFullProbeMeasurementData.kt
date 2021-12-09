package com.example.kafkasamplesstreams.events

import com.fasterxml.jackson.annotation.JsonProperty

/**
 * Represents the totals of all measurements received for a given probe.
 */
data class AggregatedFullProbeMeasurementData(
    @JsonProperty("maxSpeedMph")
    val maxSpeedMph: Double,
    @JsonProperty("traveledDistanceFeet")
    val traveledDistanceFeet: Double,
    @JsonProperty("highestRadiation")
    val highestRadiation: Double,
)