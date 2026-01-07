package com.example.kafkasamplesstreams.events

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class TelemetryDataPointTest {

    private val objectMapper = jacksonObjectMapper()

    @Test
    fun `should create TelemetryDataPoint with all fields`() {
        val dataPoint = TelemetryDataPoint(
            probeId = "probe-1",
            currentSpeedMph = 500.0,
            traveledDistanceFeet = 5000.0,
            spaceAgency = SpaceAgency.NASA
        )

        assertEquals("probe-1", dataPoint.probeId)
        assertEquals(500.0, dataPoint.currentSpeedMph)
        assertEquals(5000.0, dataPoint.traveledDistanceFeet)
        assertEquals(SpaceAgency.NASA, dataPoint.spaceAgency)
    }

    @Test
    fun `should support NASA space agency`() {
        val dataPoint = TelemetryDataPoint(
            probeId = "nasa-probe",
            currentSpeedMph = 500.0,
            traveledDistanceFeet = 5000.0,
            spaceAgency = SpaceAgency.NASA
        )

        assertEquals(SpaceAgency.NASA, dataPoint.spaceAgency)
    }

    @Test
    fun `should support ESA space agency`() {
        val dataPoint = TelemetryDataPoint(
            probeId = "esa-probe",
            currentSpeedMph = 600.0,
            traveledDistanceFeet = 6000.0,
            spaceAgency = SpaceAgency.ESA
        )

        assertEquals(SpaceAgency.ESA, dataPoint.spaceAgency)
    }

    @Test
    fun `should support zero values`() {
        val dataPoint = TelemetryDataPoint(
            probeId = "probe-0",
            currentSpeedMph = 0.0,
            traveledDistanceFeet = 0.0,
            spaceAgency = SpaceAgency.NASA
        )

        assertEquals(0.0, dataPoint.currentSpeedMph)
        assertEquals(0.0, dataPoint.traveledDistanceFeet)
    }

    @Test
    fun `should support large values`() {
        val dataPoint = TelemetryDataPoint(
            probeId = "probe-large",
            currentSpeedMph = 25000.0,
            traveledDistanceFeet = 1000000.0,
            spaceAgency = SpaceAgency.ESA
        )

        assertEquals(25000.0, dataPoint.currentSpeedMph)
        assertEquals(1000000.0, dataPoint.traveledDistanceFeet)
    }

    @Test
    fun `should have correct data class equality`() {
        val data1 = TelemetryDataPoint(
            probeId = "probe-1",
            currentSpeedMph = 500.0,
            traveledDistanceFeet = 5000.0,
            spaceAgency = SpaceAgency.NASA
        )
        val data2 = TelemetryDataPoint(
            probeId = "probe-1",
            currentSpeedMph = 500.0,
            traveledDistanceFeet = 5000.0,
            spaceAgency = SpaceAgency.NASA
        )

        assertEquals(data1, data2)
        assertEquals(data1.hashCode(), data2.hashCode())
    }

    @Test
    fun `should have different equality for different values`() {
        val data1 = TelemetryDataPoint(
            probeId = "probe-1",
            currentSpeedMph = 500.0,
            traveledDistanceFeet = 5000.0,
            spaceAgency = SpaceAgency.NASA
        )
        val data2 = TelemetryDataPoint(
            probeId = "probe-2",
            currentSpeedMph = 500.0,
            traveledDistanceFeet = 5000.0,
            spaceAgency = SpaceAgency.NASA
        )

        assertNotEquals(data1, data2)
    }

    @Test
    fun `should support data class copy`() {
        val original = TelemetryDataPoint(
            probeId = "probe-1",
            currentSpeedMph = 500.0,
            traveledDistanceFeet = 5000.0,
            spaceAgency = SpaceAgency.NASA
        )
        val modified = original.copy(currentSpeedMph = 600.0)

        assertEquals("probe-1", modified.probeId)
        assertEquals(600.0, modified.currentSpeedMph)
        assertEquals(5000.0, modified.traveledDistanceFeet)
        assertEquals(SpaceAgency.NASA, modified.spaceAgency)
    }

    @Test
    fun `should deserialize from JSON with correct field mapping`() {
        val json = """
            {
                "probeId": "probe-test",
                "currentSpeedMph": 750.0,
                "traveledDistanceFeet": 7500.0,
                "spaceAgency": "NASA"
            }
        """.trimIndent()

        val dataPoint = objectMapper.readValue(json, TelemetryDataPoint::class.java)

        assertEquals("probe-test", dataPoint.probeId)
        assertEquals(750.0, dataPoint.currentSpeedMph)
        assertEquals(7500.0, dataPoint.traveledDistanceFeet)
        assertEquals(SpaceAgency.NASA, dataPoint.spaceAgency)
    }

    @Test
    fun `should serialize to JSON with correct field names`() {
        val dataPoint = TelemetryDataPoint(
            probeId = "probe-json",
            currentSpeedMph = 800.0,
            traveledDistanceFeet = 8000.0,
            spaceAgency = SpaceAgency.ESA
        )

        val json = objectMapper.writeValueAsString(dataPoint)

        assertTrue(json.contains("\"probeId\":\"probe-json\""))
        assertTrue(json.contains("\"currentSpeedMph\":800.0"))
        assertTrue(json.contains("\"traveledDistanceFeet\":8000.0"))
        assertTrue(json.contains("\"spaceAgency\":\"ESA\""))
    }

    @Test
    fun `should handle decimal precision`() {
        val dataPoint = TelemetryDataPoint(
            probeId = "probe-decimal",
            currentSpeedMph = 567.89,
            traveledDistanceFeet = 1234.5678,
            spaceAgency = SpaceAgency.NASA
        )

        assertEquals(567.89, dataPoint.currentSpeedMph, 0.0001)
        assertEquals(1234.5678, dataPoint.traveledDistanceFeet, 0.0001)
    }

    @Test
    fun `should serialize and deserialize correctly`() {
        val original = TelemetryDataPoint(
            probeId = "probe-roundtrip",
            currentSpeedMph = 999.99,
            traveledDistanceFeet = 9999.99,
            spaceAgency = SpaceAgency.ESA
        )

        val json = objectMapper.writeValueAsString(original)
        val deserialized = objectMapper.readValue(json, TelemetryDataPoint::class.java)

        assertEquals(original, deserialized)
    }

    @Test
    fun `should support different probe ID formats`() {
        val probeIds = listOf("0", "1", "probe-1", "NASA-001", "ESA-999")

        probeIds.forEach { probeId ->
            val dataPoint = TelemetryDataPoint(
                probeId = probeId,
                currentSpeedMph = 100.0,
                traveledDistanceFeet = 1000.0,
                spaceAgency = SpaceAgency.NASA
            )
            assertEquals(probeId, dataPoint.probeId)
        }
    }
}
