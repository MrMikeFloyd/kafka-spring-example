package de.codecentric.samples.kafkasamplesproducer.event

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.ZonedDateTime

class TelemetryDataTest {

    @Test
    fun `should create TelemetryData with all required fields`() {
        val telemetryData = TelemetryData(
            probeId = "probe-1",
            currentSpeedMph = 500.0,
            traveledDistanceFeet = 5000.0,
            spaceAgency = SpaceAgency.NASA
        )

        assertEquals("probe-1", telemetryData.probeId)
        assertEquals(500.0, telemetryData.currentSpeedMph)
        assertEquals(5000.0, telemetryData.traveledDistanceFeet)
        assertEquals(SpaceAgency.NASA, telemetryData.spaceAgency)
        assertNotNull(telemetryData.timestamp)
    }

    @Test
    fun `should create TelemetryData with custom timestamp`() {
        val customTimestamp = "2024-01-01T12:00:00Z"
        val telemetryData = TelemetryData(
            probeId = "probe-2",
            timestamp = customTimestamp,
            currentSpeedMph = 750.0,
            traveledDistanceFeet = 7500.0,
            spaceAgency = SpaceAgency.ESA
        )

        assertEquals(customTimestamp, telemetryData.timestamp)
    }

    @Test
    fun `should generate default timestamp when not provided`() {
        val beforeCreation = ZonedDateTime.now()
        val telemetryData = TelemetryData(
            probeId = "probe-3",
            currentSpeedMph = 600.0,
            traveledDistanceFeet = 6000.0,
            spaceAgency = SpaceAgency.NASA
        )
        val afterCreation = ZonedDateTime.now()

        assertNotNull(telemetryData.timestamp)
        val timestamp = ZonedDateTime.parse(telemetryData.timestamp)
        assertTrue(timestamp.isAfter(beforeCreation.minusSeconds(1)))
        assertTrue(timestamp.isBefore(afterCreation.plusSeconds(1)))
    }

    @Test
    fun `should support NASA space agency`() {
        val telemetryData = TelemetryData(
            probeId = "nasa-probe",
            currentSpeedMph = 500.0,
            traveledDistanceFeet = 5000.0,
            spaceAgency = SpaceAgency.NASA
        )

        assertEquals(SpaceAgency.NASA, telemetryData.spaceAgency)
    }

    @Test
    fun `should support ESA space agency`() {
        val telemetryData = TelemetryData(
            probeId = "esa-probe",
            currentSpeedMph = 500.0,
            traveledDistanceFeet = 5000.0,
            spaceAgency = SpaceAgency.ESA
        )

        assertEquals(SpaceAgency.ESA, telemetryData.spaceAgency)
    }

    @Test
    fun `should have correct data class equality`() {
        val timestamp = "2024-01-01T12:00:00Z"
        val data1 = TelemetryData(
            probeId = "probe-1",
            timestamp = timestamp,
            currentSpeedMph = 500.0,
            traveledDistanceFeet = 5000.0,
            spaceAgency = SpaceAgency.NASA
        )
        val data2 = TelemetryData(
            probeId = "probe-1",
            timestamp = timestamp,
            currentSpeedMph = 500.0,
            traveledDistanceFeet = 5000.0,
            spaceAgency = SpaceAgency.NASA
        )

        assertEquals(data1, data2)
        assertEquals(data1.hashCode(), data2.hashCode())
    }

    @Test
    fun `should have different equality for different values`() {
        val timestamp = "2024-01-01T12:00:00Z"
        val data1 = TelemetryData(
            probeId = "probe-1",
            timestamp = timestamp,
            currentSpeedMph = 500.0,
            traveledDistanceFeet = 5000.0,
            spaceAgency = SpaceAgency.NASA
        )
        val data2 = TelemetryData(
            probeId = "probe-2",
            timestamp = timestamp,
            currentSpeedMph = 500.0,
            traveledDistanceFeet = 5000.0,
            spaceAgency = SpaceAgency.NASA
        )

        assertNotEquals(data1, data2)
    }

    @Test
    fun `should support data class copy with modifications`() {
        val original = TelemetryData(
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
    fun `should allow zero and negative values for speed and distance`() {
        val telemetryData = TelemetryData(
            probeId = "probe-4",
            currentSpeedMph = 0.0,
            traveledDistanceFeet = 0.0,
            spaceAgency = SpaceAgency.ESA
        )

        assertEquals(0.0, telemetryData.currentSpeedMph)
        assertEquals(0.0, telemetryData.traveledDistanceFeet)
    }
}
