package de.codecentric.samples.kafkasamplesconsumer.event

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class MetricTelemetryDataTest {

    @Test
    fun `should convert imperial to metric units`() {
        val imperialData = ImperialTelemetryData(
            totalDistanceTraveledFeet = 1000.0,
            maxSpeedMph = 100.0
        )

        val metricData = MetricTelemetryData(imperialData)

        assertEquals(161.0, metricData.maxSpeedKph, 0.1)
        assertEquals(304.8, metricData.totalDistanceMetres, 0.1)
    }

    @Test
    fun `should convert zero values correctly`() {
        val imperialData = ImperialTelemetryData(
            totalDistanceTraveledFeet = 0.0,
            maxSpeedMph = 0.0
        )

        val metricData = MetricTelemetryData(imperialData)

        assertEquals(0.0, metricData.maxSpeedKph)
        assertEquals(0.0, metricData.totalDistanceMetres)
    }

    @Test
    fun `should convert speed from mph to kph with correct factor`() {
        val imperialData = ImperialTelemetryData(
            totalDistanceTraveledFeet = 100.0,
            maxSpeedMph = 50.0
        )

        val metricData = MetricTelemetryData(imperialData)

        // 50 mph * 1.61 = 80.5 kph
        assertEquals(80.5, metricData.maxSpeedKph, 0.001)
    }

    @Test
    fun `should convert distance from feet to metres with correct factor`() {
        val imperialData = ImperialTelemetryData(
            totalDistanceTraveledFeet = 1000.0,
            maxSpeedMph = 100.0
        )

        val metricData = MetricTelemetryData(imperialData)

        // 1000 feet * 0.3048 = 304.8 metres
        assertEquals(304.8, metricData.totalDistanceMetres, 0.001)
    }

    @Test
    fun `should handle large values`() {
        val imperialData = ImperialTelemetryData(
            totalDistanceTraveledFeet = 1000000.0,
            maxSpeedMph = 25000.0
        )

        val metricData = MetricTelemetryData(imperialData)

        assertEquals(40250.0, metricData.maxSpeedKph, 0.1)
        assertEquals(304800.0, metricData.totalDistanceMetres, 0.1)
    }

    @Test
    fun `should handle decimal values with precision`() {
        val imperialData = ImperialTelemetryData(
            totalDistanceTraveledFeet = 123.456,
            maxSpeedMph = 78.9
        )

        val metricData = MetricTelemetryData(imperialData)

        assertEquals(127.029, metricData.maxSpeedKph, 0.001)
        assertEquals(37.6293, metricData.totalDistanceMetres, 0.001)
    }

    @Test
    fun `should convert realistic space probe speed`() {
        // Voyager 1 speed is approximately 38,000 mph
        val imperialData = ImperialTelemetryData(
            totalDistanceTraveledFeet = 100000000.0,
            maxSpeedMph = 38000.0
        )

        val metricData = MetricTelemetryData(imperialData)

        assertEquals(61180.0, metricData.maxSpeedKph, 0.1)
        assertEquals(30480000.0, metricData.totalDistanceMetres, 0.1)
    }

    @Test
    fun `should convert realistic space probe distance`() {
        // Test with a large distance (millions of feet)
        val imperialData = ImperialTelemetryData(
            totalDistanceTraveledFeet = 5280000.0, // 1000 miles in feet
            maxSpeedMph = 5000.0
        )

        val metricData = MetricTelemetryData(imperialData)

        assertEquals(8050.0, metricData.maxSpeedKph, 0.1)
        assertEquals(1609344.0, metricData.totalDistanceMetres, 0.1)
    }

    @Test
    fun `should maintain precision in conversion`() {
        val imperialData = ImperialTelemetryData(
            totalDistanceTraveledFeet = 1.0,
            maxSpeedMph = 1.0
        )

        val metricData = MetricTelemetryData(imperialData)

        // 1 mph = 1.61 kph
        assertEquals(1.61, metricData.maxSpeedKph, 0.001)
        // 1 foot = 0.3048 metres
        assertEquals(0.3048, metricData.totalDistanceMetres, 0.0001)
    }

    @Test
    fun `should create multiple independent conversions`() {
        val imperialData1 = ImperialTelemetryData(
            totalDistanceTraveledFeet = 1000.0,
            maxSpeedMph = 100.0
        )
        val imperialData2 = ImperialTelemetryData(
            totalDistanceTraveledFeet = 2000.0,
            maxSpeedMph = 200.0
        )

        val metricData1 = MetricTelemetryData(imperialData1)
        val metricData2 = MetricTelemetryData(imperialData2)

        assertEquals(161.0, metricData1.maxSpeedKph, 0.1)
        assertEquals(304.8, metricData1.totalDistanceMetres, 0.1)

        assertEquals(322.0, metricData2.maxSpeedKph, 0.1)
        assertEquals(609.6, metricData2.totalDistanceMetres, 0.1)
    }

    @Test
    fun `should convert data matching NASA typical values`() {
        val imperialData = ImperialTelemetryData(
            totalDistanceTraveledFeet = 500000.0,
            maxSpeedMph = 17500.0  // Low Earth orbit speed
        )

        val metricData = MetricTelemetryData(imperialData)

        assertEquals(28175.0, metricData.maxSpeedKph, 0.1)
        assertEquals(152400.0, metricData.totalDistanceMetres, 0.1)
    }
}
