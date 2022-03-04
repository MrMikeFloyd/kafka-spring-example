package com.example.kafkasamplesstreams.events

import com.fasterxml.jackson.annotation.JsonProperty

class TelemetryDataPointWithDisabled(
    val probeId: String,
    val currentSpeedMph: Double,
    val traveledDistanceFeet: Double,
    val spaceAgency: SpaceAgency,
    val disabled: Boolean,
) {
    constructor(telemetryDatapoint: TelemetryDataPoint, disabled: Disabled?) : this(
        telemetryDatapoint.probeId,
        telemetryDatapoint.currentSpeedMph,
        telemetryDatapoint.traveledDistanceFeet,
        telemetryDatapoint.spaceAgency,
        disabled?.disabled ?: false)
}