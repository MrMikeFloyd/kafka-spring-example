package de.codecentric.samples.kafkasamplesproducer

import java.time.ZonedDateTime
import java.time.ZonedDateTime.now

data class TelemetryData(
    val probeId: String,
    val timestamp: ZonedDateTime = now(),
    val currentSpeedMph: Double,
    val traveledDistanceFeet: Double
)