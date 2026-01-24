package de.codecentric.samples.kafkasamplesconsumer.event

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class VerstaTelemetryDataTest {

    @Test
    fun `should convert speed from mph to verstas per hour`() {
        val imperialData = ImperialTelemetryData(
            maxSpeedMph = 100.0,
            totalDistanceTraveledFeet = 3500.0
        )

        val verstaData = VerstaTelemetryData(imperialData)

        assertEquals(50.0, verstaData.maxSpeedVerstasPerHour)
    }

    @Test
    fun `should convert distance from feet to verstas`() {
        val imperialData = ImperialTelemetryData(
            maxSpeedMph = 100.0,
            totalDistanceTraveledFeet = 3500.0
        )

        val verstaData = VerstaTelemetryData(imperialData)

        assertEquals(1.0, verstaData.totalDistanceVerstas)
    }

    @Test
    fun `should handle zero values`() {
        val imperialData = ImperialTelemetryData(
            maxSpeedMph = 0.0,
            totalDistanceTraveledFeet = 0.0
        )

        val verstaData = VerstaTelemetryData(imperialData)

        assertEquals(0.0, verstaData.maxSpeedVerstasPerHour)
        assertEquals(0.0, verstaData.totalDistanceVerstas)
    }

    @Test
    fun `should handle large values`() {
        val imperialData = ImperialTelemetryData(
            maxSpeedMph = 20000.0,
            totalDistanceTraveledFeet = 35000000.0
        )

        val verstaData = VerstaTelemetryData(imperialData)

        assertEquals(10000.0, verstaData.maxSpeedVerstasPerHour)
        assertEquals(10000.0, verstaData.totalDistanceVerstas)
    }

    @Test
    fun `should handle decimal values with precision`() {
        val imperialData = ImperialTelemetryData(
            maxSpeedMph = 123.456,
            totalDistanceTraveledFeet = 7000.0
        )

        val verstaData = VerstaTelemetryData(imperialData)

        assertEquals(61.728, verstaData.maxSpeedVerstasPerHour, 0.001)
        assertEquals(2.0, verstaData.totalDistanceVerstas, 0.001)
    }

    @Test
    fun `should correctly apply versta conversion factor for speed`() {
        // 1 versta/hour = 2.0 mph, so 2 mph = 1 versta/hour
        val imperialData = ImperialTelemetryData(
            maxSpeedMph = 2.0,
            totalDistanceTraveledFeet = 3500.0
        )

        val verstaData = VerstaTelemetryData(imperialData)

        assertEquals(1.0, verstaData.maxSpeedVerstasPerHour)
    }

    @Test
    fun `should correctly apply versta conversion factor for distance`() {
        // 1 versta = 3500 feet
        val imperialData = ImperialTelemetryData(
            maxSpeedMph = 100.0,
            totalDistanceTraveledFeet = 7000.0
        )

        val verstaData = VerstaTelemetryData(imperialData)

        assertEquals(2.0, verstaData.totalDistanceVerstas)
    }

    @Test
    fun `should handle fractional versta values`() {
        val imperialData = ImperialTelemetryData(
            maxSpeedMph = 1.0,
            totalDistanceTraveledFeet = 1750.0
        )

        val verstaData = VerstaTelemetryData(imperialData)

        assertEquals(0.5, verstaData.maxSpeedVerstasPerHour)
        assertEquals(0.5, verstaData.totalDistanceVerstas)
    }

    @Test
    fun `should handle realistic orbital speed in verstas`() {
        // Low Earth orbit speed is about 17500 mph
        val imperialData = ImperialTelemetryData(
            maxSpeedMph = 17500.0,
            totalDistanceTraveledFeet = 26400000.0 // 5000 miles
        )

        val verstaData = VerstaTelemetryData(imperialData)

        assertEquals(8750.0, verstaData.maxSpeedVerstasPerHour)
        assertEquals(7542.857, verstaData.totalDistanceVerstas, 0.01)
    }
}
