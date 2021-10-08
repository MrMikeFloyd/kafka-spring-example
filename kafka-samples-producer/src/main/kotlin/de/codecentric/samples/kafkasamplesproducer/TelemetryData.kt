package de.codecentric.samples.kafkasamplesproducer

import java.time.ZonedDateTime.now

data class TelemetryData(
    val probeId: String,
    val timestamp: String = now().toString(),
    val currentSpeedMph: Double,
    val traveledDistanceFeet: Double
)