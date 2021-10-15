package com.example.kafkasamplesstreams

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper

class TelemetryData(jsonData: String, speedKeyName: String, distanceKeyName: String) {

    val speed: Double
    val distance: Double

    init {
        val typeRef: TypeReference<HashMap<String, String>> = object : TypeReference<HashMap<String, String>>() {}
        val telemetryDataPoint: MutableMap<String, String> = ObjectMapper().readValue(jsonData, typeRef)

        this.speed = telemetryDataPoint[speedKeyName]!!.toDouble()
        this.distance = telemetryDataPoint[distanceKeyName]!!.toDouble()
    }

}