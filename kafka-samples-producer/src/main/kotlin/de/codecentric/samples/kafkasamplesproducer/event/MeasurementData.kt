package de.codecentric.samples.kafkasamplesproducer.event

import java.time.ZonedDateTime.now

data class MeasurementData(
    val probeId: String,
    val timestamp: String = now().toString(),
    val radiation: Double,
    val spaceAgency: SpaceAgency,
)