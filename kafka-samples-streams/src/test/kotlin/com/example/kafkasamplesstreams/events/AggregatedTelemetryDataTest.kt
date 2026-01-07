package com.example.kafkasamplesstreams.events

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class AggregatedTelemetryDataTest {

    private val objectMapper = jacksonObjectMapper()

    @Test
    fun `should create AggregatedTelemetryData with all fields`() {
        val aggregatedData = AggregatedTelemetryData(
            maxSpeedMph = 1000.0,
            traveledDistanceFeet = 50000.0
        )

        assertEquals(1000.0, aggregatedData.maxSpeedMph)
        assertEquals(50000.0, aggregatedData.traveledDistanceFeet)
    }

    @Test
    fun `should support zero values`() {
        val aggregatedData = AggregatedTelemetryData(
            maxSpeedMph = 0.0,
            traveledDistanceFeet = 0.0
        )

        assertEquals(0.0, aggregatedData.maxSpeedMph)
        assertEquals(0.0, aggregatedData.traveledDistanceFeet)
    }

    @Test
    fun `should support large values`() {
        val aggregatedData = AggregatedTelemetryData(
            maxSpeedMph = 50000.0,
            traveledDistanceFeet = 10000000.0
        )

        assertEquals(50000.0, aggregatedData.maxSpeedMph)
        assertEquals(10000000.0, aggregatedData.traveledDistanceFeet)
    }

    @Test
    fun `should have correct data class equality`() {
        val data1 = AggregatedTelemetryData(
            maxSpeedMph = 1000.0,
            traveledDistanceFeet = 50000.0
        )
        val data2 = AggregatedTelemetryData(
            maxSpeedMph = 1000.0,
            traveledDistanceFeet = 50000.0
        )

        assertEquals(data1, data2)
        assertEquals(data1.hashCode(), data2.hashCode())
    }

    @Test
    fun `should have different equality for different values`() {
        val data1 = AggregatedTelemetryData(
            maxSpeedMph = 1000.0,
            traveledDistanceFeet = 50000.0
        )
        val data2 = AggregatedTelemetryData(
            maxSpeedMph = 1500.0,
            traveledDistanceFeet = 50000.0
        )

        assertNotEquals(data1, data2)
    }

    @Test
    fun `should support data class copy`() {
        val original = AggregatedTelemetryData(
            maxSpeedMph = 1000.0,
            traveledDistanceFeet = 50000.0
        )
        val modified = original.copy(maxSpeedMph = 1200.0)

        assertEquals(1200.0, modified.maxSpeedMph)
        assertEquals(50000.0, modified.traveledDistanceFeet)
    }

    @Test
    fun `should deserialize from JSON with correct field mapping`() {
        val json = """
            {
                "maxSpeedMph": 1500.0,
                "traveledDistanceFeet": 75000.0
            }
        """.trimIndent()

        val aggregatedData = objectMapper.readValue(json, AggregatedTelemetryData::class.java)

        assertEquals(1500.0, aggregatedData.maxSpeedMph)
        assertEquals(75000.0, aggregatedData.traveledDistanceFeet)
    }

    @Test
    fun `should serialize to JSON with correct field names`() {
        val aggregatedData = AggregatedTelemetryData(
            maxSpeedMph = 1800.0,
            traveledDistanceFeet = 80000.0
        )

        val json = objectMapper.writeValueAsString(aggregatedData)

        assertTrue(json.contains("\"maxSpeedMph\":1800.0"))
        assertTrue(json.contains("\"traveledDistanceFeet\":80000.0"))
    }

    @Test
    fun `should handle decimal precision`() {
        val aggregatedData = AggregatedTelemetryData(
            maxSpeedMph = 1234.5678,
            traveledDistanceFeet = 98765.4321
        )

        assertEquals(1234.5678, aggregatedData.maxSpeedMph, 0.0001)
        assertEquals(98765.4321, aggregatedData.traveledDistanceFeet, 0.0001)
    }

    @Test
    fun `should serialize and deserialize correctly`() {
        val original = AggregatedTelemetryData(
            maxSpeedMph = 2000.0,
            traveledDistanceFeet = 100000.0
        )

        val json = objectMapper.writeValueAsString(original)
        val deserialized = objectMapper.readValue(json, AggregatedTelemetryData::class.java)

        assertEquals(original, deserialized)
    }

    @Test
    fun `should represent accumulated distance correctly`() {
        val aggregatedData = AggregatedTelemetryData(
            maxSpeedMph = 500.0,
            traveledDistanceFeet = 26400000.0 // 5000 miles
        )

        assertEquals(26400000.0, aggregatedData.traveledDistanceFeet)
    }

    @Test
    fun `should represent maximum speed correctly`() {
        val aggregatedData = AggregatedTelemetryData(
            maxSpeedMph = 25000.0, // Approximate escape velocity in mph
            traveledDistanceFeet = 1000000.0
        )

        assertEquals(25000.0, aggregatedData.maxSpeedMph)
    }

    @Test
    fun `should support realistic orbital speeds`() {
        val aggregatedData = AggregatedTelemetryData(
            maxSpeedMph = 17500.0, // Low Earth orbit speed
            traveledDistanceFeet = 52800000.0 // 10000 miles
        )

        assertEquals(17500.0, aggregatedData.maxSpeedMph)
        assertEquals(52800000.0, aggregatedData.traveledDistanceFeet)
    }
}
