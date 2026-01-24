package de.codecentric.samples.kafkasamplesconsumer.event

class VerstaTelemetryData(telemetryData: ImperialTelemetryData) {

    val maxSpeedVerstasPerHour: Double
    val totalDistanceVerstas: Double

    init {
        this.maxSpeedVerstasPerHour = telemetryData.maxSpeedMph / 2.0
        this.totalDistanceVerstas = telemetryData.totalDistanceTraveledFeet / 3500.0
    }
}
