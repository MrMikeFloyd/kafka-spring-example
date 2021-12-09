package de.codecentric.samples.kafkasamplesconsumer.event

class MetricTelemetryData(telemetryData: ImperialTelemetryData) {

    val maxSpeedKph: Double
    val totalDistanceMetres: Double
    val maximumRadiation: Double

    init {
        this.maxSpeedKph = telemetryData.maxSpeedMph * 1.61
        this.totalDistanceMetres = telemetryData.totalDistanceTraveledFeet * 0.3048
        this.maximumRadiation = telemetryData.maximumRadiation
    }
}