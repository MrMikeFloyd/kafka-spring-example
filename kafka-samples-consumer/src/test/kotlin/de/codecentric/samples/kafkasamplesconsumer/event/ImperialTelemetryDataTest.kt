package de.codecentric.samples.kafkasamplesconsumer.event

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class ImperialTelemetryDataTest {

    private val objectMapper = jacksonObjectMapper()

    @Test
    fun `should create ImperialTelemetryData with all fields`() {
        val telemetryData = ImperialTelemetryData(
            totalDistanceTraveledFeet = 5000.0,
            maxSpeedMph = 500.0
        )

        assertEquals(5000.0, telemetryData.totalDistanceTraveledFeet)
        assertEquals(500.0, telemetryData.maxSpeedMph)
    }

    @Test
    fun `should support zero values`() {
        val telemetryData = ImperialTelemetryData(
            totalDistanceTraveledFeet = 0.0,
            maxSpeedMph = 0.0
        )

        assertEquals(0.0, telemetryData.totalDistanceTraveledFeet)
        assertEquals(0.0, telemetryData.maxSpeedMph)
    }

    @Test
    fun `should support large values`() {
        val telemetryData = ImperialTelemetryData(
            totalDistanceTraveledFeet = 1000000.0,
            maxSpeedMph = 25000.0
        )

        assertEquals(1000000.0, telemetryData.totalDistanceTraveledFeet)
        assertEquals(25000.0, telemetryData.maxSpeedMph)
    }

    @Test
    fun `should have correct data class equality`() {
        val data1 = ImperialTelemetryData(
            totalDistanceTraveledFeet = 5000.0,
            maxSpeedMph = 500.0
        )
        val data2 = ImperialTelemetryData(
            totalDistanceTraveledFeet = 5000.0,
            maxSpeedMph = 500.0
        )

        assertEquals(data1, data2)
        assertEquals(data1.hashCode(), data2.hashCode())
    }

    @Test
    fun `should have different equality for different values`() {
        val data1 = ImperialTelemetryData(
            totalDistanceTraveledFeet = 5000.0,
            maxSpeedMph = 500.0
        )
        val data2 = ImperialTelemetryData(
            totalDistanceTraveledFeet = 6000.0,
            maxSpeedMph = 500.0
        )

        assertNotEquals(data1, data2)
    }

    @Test
    fun `should support data class copy`() {
        val original = ImperialTelemetryData(
            totalDistanceTraveledFeet = 5000.0,
            maxSpeedMph = 500.0
        )
        val modified = original.copy(maxSpeedMph = 600.0)

        assertEquals(5000.0, modified.totalDistanceTraveledFeet)
        assertEquals(600.0, modified.maxSpeedMph)
    }

    @Test
    fun `should deserialize from JSON with correct field mapping`() {
        val json = """
            {
                "traveledDistanceFeet": 7500.0,
                "maxSpeedMph": 750.0
            }
        """.trimIndent()

        val telemetryData = objectMapper.readValue(json, ImperialTelemetryData::class.java)

        assertEquals(7500.0, telemetryData.totalDistanceTraveledFeet)
        assertEquals(750.0, telemetryData.maxSpeedMph)
    }

    @Test
    fun `should serialize to JSON with correct field names`() {
        val telemetryData = ImperialTelemetryData(
            totalDistanceTraveledFeet = 8000.0,
            maxSpeedMph = 800.0
        )

        val json = objectMapper.writeValueAsString(telemetryData)

        assertTrue(json.contains("\"traveledDistanceFeet\":8000.0"))
        assertTrue(json.contains("\"maxSpeedMph\":800.0"))
    }

    @Test
    fun `should handle decimal precision`() {
        val telemetryData = ImperialTelemetryData(
            totalDistanceTraveledFeet = 1234.5678,
            maxSpeedMph = 567.89
        )

        assertEquals(1234.5678, telemetryData.totalDistanceTraveledFeet, 0.0001)
        assertEquals(567.89, telemetryData.maxSpeedMph, 0.0001)
    }

    @Test
    fun `should serialize and deserialize correctly`() {
        val original = ImperialTelemetryData(
            totalDistanceTraveledFeet = 9999.99,
            maxSpeedMph = 999.99
        )

        val json = objectMapper.writeValueAsString(original)
        val deserialized = objectMapper.readValue(json, ImperialTelemetryData::class.java)

        assertEquals(original, deserialized)
    }
}
