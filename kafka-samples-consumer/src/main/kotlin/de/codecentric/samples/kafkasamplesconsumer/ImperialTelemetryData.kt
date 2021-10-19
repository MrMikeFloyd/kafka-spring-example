package de.codecentric.samples.kafkasamplesconsumer

import com.fasterxml.jackson.annotation.JsonProperty

data class ImperialTelemetryData(
    @JsonProperty("traveledDistanceFeet")
    val totalDistanceTraveledFeet: Double,
    @JsonProperty("maxSpeedMph")
    val maxSpeedMph: Double)