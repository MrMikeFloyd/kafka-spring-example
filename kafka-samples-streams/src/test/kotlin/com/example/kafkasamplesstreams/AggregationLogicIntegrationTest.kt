package com.example.kafkasamplesstreams

import com.example.kafkasamplesstreams.events.AggregatedTelemetryData
import com.example.kafkasamplesstreams.events.SpaceAgency
import com.example.kafkasamplesstreams.events.TelemetryDataPoint
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest

@SpringBootTest
class AggregationLogicIntegrationTest {

    @Autowired
    private lateinit var kafkaStreamsHandler: KafkaStreamsHandler

    @Test
    fun `should aggregate telemetry data for single probe`() {
        val probeId = "test-probe-1"
        val dataPoints = listOf(
            TelemetryDataPoint(probeId, 100.0, 1000.0, SpaceAgency.NASA),
            TelemetryDataPoint(probeId, 200.0, 2000.0, SpaceAgency.NASA),
            TelemetryDataPoint(probeId, 150.0, 1500.0, SpaceAgency.NASA)
        )

        var aggregated = AggregatedTelemetryData(0.0, 0.0)
        dataPoints.forEach { dataPoint ->
            aggregated = kafkaStreamsHandler.updateTotals(probeId, dataPoint, aggregated)
        }

        assertEquals(200.0, aggregated.maxSpeedMph)
        assertEquals(4500.0, aggregated.traveledDistanceFeet)
    }

    @Test
    fun `should handle NASA and ESA probes independently`() {
        val nasaProbe = "nasa-probe-1"
        val esaProbe = "esa-probe-1"

        val nasaDataPoints = listOf(
            TelemetryDataPoint(nasaProbe, 500.0, 5000.0, SpaceAgency.NASA),
            TelemetryDataPoint(nasaProbe, 600.0, 6000.0, SpaceAgency.NASA)
        )

        val esaDataPoints = listOf(
            TelemetryDataPoint(esaProbe, 700.0, 7000.0, SpaceAgency.ESA),
            TelemetryDataPoint(esaProbe, 800.0, 8000.0, SpaceAgency.ESA)
        )

        var nasaAggregated = AggregatedTelemetryData(0.0, 0.0)
        var esaAggregated = AggregatedTelemetryData(0.0, 0.0)

        nasaDataPoints.forEach { dataPoint ->
            nasaAggregated = kafkaStreamsHandler.updateTotals(nasaProbe, dataPoint, nasaAggregated)
        }

        esaDataPoints.forEach { dataPoint ->
            esaAggregated = kafkaStreamsHandler.updateTotals(esaProbe, dataPoint, esaAggregated)
        }

        assertEquals(600.0, nasaAggregated.maxSpeedMph)
        assertEquals(11000.0, nasaAggregated.traveledDistanceFeet)

        assertEquals(800.0, esaAggregated.maxSpeedMph)
        assertEquals(15000.0, esaAggregated.traveledDistanceFeet)
    }

    @Test
    fun `should track maximum speed correctly across multiple readings`() {
        val probeId = "speed-test-probe"
        val dataPoints = listOf(
            TelemetryDataPoint(probeId, 500.0, 1000.0, SpaceAgency.NASA),
            TelemetryDataPoint(probeId, 1000.0, 1000.0, SpaceAgency.NASA),
            TelemetryDataPoint(probeId, 750.0, 1000.0, SpaceAgency.NASA),
            TelemetryDataPoint(probeId, 600.0, 1000.0, SpaceAgency.NASA)
        )

        var aggregated = AggregatedTelemetryData(0.0, 0.0)
        dataPoints.forEach { dataPoint ->
            aggregated = kafkaStreamsHandler.updateTotals(probeId, dataPoint, aggregated)
        }

        assertEquals(1000.0, aggregated.maxSpeedMph)
        assertEquals(4000.0, aggregated.traveledDistanceFeet)
    }

    @Test
    fun `should accumulate distance correctly`() {
        val probeId = "distance-test-probe"
        val dataPoints = listOf(
            TelemetryDataPoint(probeId, 100.0, 1000.0, SpaceAgency.ESA),
            TelemetryDataPoint(probeId, 100.0, 2500.0, SpaceAgency.ESA),
            TelemetryDataPoint(probeId, 100.0, 3000.0, SpaceAgency.ESA),
            TelemetryDataPoint(probeId, 100.0, 1500.0, SpaceAgency.ESA)
        )

        var aggregated = AggregatedTelemetryData(0.0, 0.0)
        dataPoints.forEach { dataPoint ->
            aggregated = kafkaStreamsHandler.updateTotals(probeId, dataPoint, aggregated)
        }

        assertEquals(100.0, aggregated.maxSpeedMph)
        assertEquals(8000.0, aggregated.traveledDistanceFeet)
    }

    @Test
    fun `should handle realistic orbital telemetry data`() {
        val probeId = "orbital-probe"
        val dataPoints = listOf(
            TelemetryDataPoint(probeId, 15000.0, 5000000.0, SpaceAgency.NASA),
            TelemetryDataPoint(probeId, 17500.0, 10000000.0, SpaceAgency.NASA),
            TelemetryDataPoint(probeId, 16000.0, 8000000.0, SpaceAgency.NASA)
        )

        var aggregated = AggregatedTelemetryData(0.0, 0.0)
        dataPoints.forEach { dataPoint ->
            aggregated = kafkaStreamsHandler.updateTotals(probeId, dataPoint, aggregated)
        }

        assertEquals(17500.0, aggregated.maxSpeedMph)
        assertEquals(23000000.0, aggregated.traveledDistanceFeet)
    }
}
