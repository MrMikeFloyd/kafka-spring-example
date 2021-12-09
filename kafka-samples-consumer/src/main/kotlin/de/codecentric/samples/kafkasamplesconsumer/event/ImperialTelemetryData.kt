package de.codecentric.samples.kafkasamplesconsumer.event

import com.fasterxml.jackson.annotation.JsonProperty

data class ImperialTelemetryData(
    @JsonProperty("traveledDistanceFeet")
    val totalDistanceTraveledFeet: Double,
    @JsonProperty("maxSpeedMph")
    val maxSpeedMph: Double,
    @JsonProperty("highestRadiation")
    val maximumRadiation: Double
)