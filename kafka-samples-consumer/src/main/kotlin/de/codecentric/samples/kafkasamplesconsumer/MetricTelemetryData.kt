package de.codecentric.samples.kafkasamplesconsumer

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper

class MetricTelemetryData(jsonData: String) {

    val maxSpeedKph: Double
    val totalDistanceMetres: Double

    init {
        val typeRef: TypeReference<HashMap<String, String>> = object : TypeReference<HashMap<String, String>>() {}
        val telemetryData: MutableMap<String, String> = ObjectMapper().readValue(jsonData, typeRef)
        val aggregatedSpeedMph: Double = telemetryData["maxSpeed"]!!.toDouble()
        val aggregatedDistanceFeet: Double = telemetryData["sumDistance"]!!.toDouble()

        this.maxSpeedKph = aggregatedSpeedMph * 1.61
        this.totalDistanceMetres = aggregatedDistanceFeet * 0.3048
    }
}