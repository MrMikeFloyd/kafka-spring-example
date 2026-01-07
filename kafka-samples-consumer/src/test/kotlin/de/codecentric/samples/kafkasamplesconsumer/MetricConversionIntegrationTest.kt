package de.codecentric.samples.kafkasamplesconsumer

import de.codecentric.samples.kafkasamplesconsumer.event.ImperialTelemetryData
import de.codecentric.samples.kafkasamplesconsumer.event.MetricTelemetryData
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest

@SpringBootTest
class MetricConversionIntegrationTest {

    @Test
    fun `should convert imperial to metric for ESA processing`() {
        val imperialData = ImperialTelemetryData(
            totalDistanceTraveledFeet = 1000.0,
            maxSpeedMph = 100.0
        )

        val metricData = MetricTelemetryData(imperialData)

        assertEquals(161.0, metricData.maxSpeedKph, 0.1)
        assertEquals(304.8, metricData.totalDistanceMetres, 0.1)
    }

    @Test
    fun `should handle multiple conversions`() {
        val imperialDataList = listOf(
            ImperialTelemetryData(1000.0, 100.0),
            ImperialTelemetryData(2000.0, 200.0),
            ImperialTelemetryData(3000.0, 300.0)
        )

        val metricDataList = imperialDataList.map { MetricTelemetryData(it) }

        assertEquals(3, metricDataList.size)
        assertEquals(161.0, metricDataList[0].maxSpeedKph, 0.1)
        assertEquals(322.0, metricDataList[1].maxSpeedKph, 0.1)
        assertEquals(483.0, metricDataList[2].maxSpeedKph, 0.1)
    }

    @Test
    fun `should handle large orbital speed values`() {
        val imperialData = ImperialTelemetryData(
            totalDistanceTraveledFeet = 26400000.0, // 5000 miles
            maxSpeedMph = 17500.0 // Low Earth orbit speed
        )

        val metricData = MetricTelemetryData(imperialData)

        assertEquals(28175.0, metricData.maxSpeedKph, 1.0)
        assertEquals(8047200.0, metricData.totalDistanceMetres, 1000.0)
    }
}
