package com.example.kafkasamplesstreams.events

import com.fasterxml.jackson.annotation.JsonProperty

data class FullProbeMeasurement(
    @JsonProperty("probeId")
    val probeId: String,
    @JsonProperty("radiation")
    val radiation: Double,
    @JsonProperty("currentSpeedMph")
    val currentSpeedMph: Double,
    @JsonProperty("traveledDistanceFeet")
    val traveledDistanceFeet: Double,
    @JsonProperty("spaceAgency")
    val spaceAgency: SpaceAgency
)