package com.example.kafkasamplesstreams

import com.example.kafkasamplesstreams.events.AggregatedTelemetryData
import com.example.kafkasamplesstreams.events.SpaceAgency
import com.example.kafkasamplesstreams.events.TelemetryDataPoint
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class KafkaStreamsHandlerTest {

    private lateinit var handler: KafkaStreamsHandler

    @BeforeEach
    fun setup() {
        handler = KafkaStreamsHandler()
    }

    @Test
    fun `should create aggregateTelemetryData function bean`() {
        val function = handler.aggregateTelemetryData()

        assertNotNull(function)
    }

    @Test
    fun `should initialize aggregated data with zero values`() {
        val initial = AggregatedTelemetryData(maxSpeedMph = 0.0, traveledDistanceFeet = 0.0)

        assertEquals(0.0, initial.maxSpeedMph)
        assertEquals(0.0, initial.traveledDistanceFeet)
    }

    @Test
    fun `should update totals with first telemetry reading`() {
        val probeId = "probe-1"
        val telemetryPoint = TelemetryDataPoint(
            probeId = probeId,
            currentSpeedMph = 500.0,
            traveledDistanceFeet = 5000.0,
            spaceAgency = SpaceAgency.NASA
        )
        val currentAggregate = AggregatedTelemetryData(
            maxSpeedMph = 0.0,
            traveledDistanceFeet = 0.0
        )

        val result = handler.updateTotals(probeId, telemetryPoint, currentAggregate)

        assertEquals(500.0, result.maxSpeedMph)
        assertEquals(5000.0, result.traveledDistanceFeet)
    }

    @Test
    fun `should accumulate distance over multiple readings`() {
        val probeId = "probe-1"
        val telemetryPoint1 = TelemetryDataPoint(
            probeId = probeId,
            currentSpeedMph = 500.0,
            traveledDistanceFeet = 5000.0,
            spaceAgency = SpaceAgency.NASA
        )
        val telemetryPoint2 = TelemetryDataPoint(
            probeId = probeId,
            currentSpeedMph = 600.0,
            traveledDistanceFeet = 6000.0,
            spaceAgency = SpaceAgency.NASA
        )

        var aggregate = AggregatedTelemetryData(maxSpeedMph = 0.0, traveledDistanceFeet = 0.0)
        aggregate = handler.updateTotals(probeId, telemetryPoint1, aggregate)
        aggregate = handler.updateTotals(probeId, telemetryPoint2, aggregate)

        assertEquals(11000.0, aggregate.traveledDistanceFeet)
    }

    @Test
    fun `should track maximum speed across readings`() {
        val probeId = "probe-1"
        val telemetryPoint1 = TelemetryDataPoint(
            probeId = probeId,
            currentSpeedMph = 500.0,
            traveledDistanceFeet = 5000.0,
            spaceAgency = SpaceAgency.NASA
        )
        val telemetryPoint2 = TelemetryDataPoint(
            probeId = probeId,
            currentSpeedMph = 800.0,
            traveledDistanceFeet = 6000.0,
            spaceAgency = SpaceAgency.NASA
        )
        val telemetryPoint3 = TelemetryDataPoint(
            probeId = probeId,
            currentSpeedMph = 600.0,
            traveledDistanceFeet = 7000.0,
            spaceAgency = SpaceAgency.NASA
        )

        var aggregate = AggregatedTelemetryData(maxSpeedMph = 0.0, traveledDistanceFeet = 0.0)
        aggregate = handler.updateTotals(probeId, telemetryPoint1, aggregate)
        aggregate = handler.updateTotals(probeId, telemetryPoint2, aggregate)
        aggregate = handler.updateTotals(probeId, telemetryPoint3, aggregate)

        assertEquals(800.0, aggregate.maxSpeedMph)
    }

    @Test
    fun `should not decrease maximum speed with lower reading`() {
        val probeId = "probe-1"
        val currentAggregate = AggregatedTelemetryData(
            maxSpeedMph = 1000.0,
            traveledDistanceFeet = 10000.0
        )
        val newReading = TelemetryDataPoint(
            probeId = probeId,
            currentSpeedMph = 500.0,
            traveledDistanceFeet = 5000.0,
            spaceAgency = SpaceAgency.NASA
        )

        val result = handler.updateTotals(probeId, newReading, currentAggregate)

        assertEquals(1000.0, result.maxSpeedMph)
    }

    @Test
    fun `should update maximum speed with higher reading`() {
        val probeId = "probe-1"
        val currentAggregate = AggregatedTelemetryData(
            maxSpeedMph = 500.0,
            traveledDistanceFeet = 10000.0
        )
        val newReading = TelemetryDataPoint(
            probeId = probeId,
            currentSpeedMph = 1000.0,
            traveledDistanceFeet = 5000.0,
            spaceAgency = SpaceAgency.NASA
        )

        val result = handler.updateTotals(probeId, newReading, currentAggregate)

        assertEquals(1000.0, result.maxSpeedMph)
    }

    @Test
    fun `should handle zero values in telemetry reading`() {
        val probeId = "probe-1"
        val currentAggregate = AggregatedTelemetryData(
            maxSpeedMph = 500.0,
            traveledDistanceFeet = 5000.0
        )
        val newReading = TelemetryDataPoint(
            probeId = probeId,
            currentSpeedMph = 0.0,
            traveledDistanceFeet = 0.0,
            spaceAgency = SpaceAgency.NASA
        )

        val result = handler.updateTotals(probeId, newReading, currentAggregate)

        assertEquals(500.0, result.maxSpeedMph)
        assertEquals(5000.0, result.traveledDistanceFeet)
    }

    @Test
    fun `should correctly aggregate multiple readings for single probe`() {
        val probeId = "probe-1"
        val readings = listOf(
            TelemetryDataPoint(probeId, 100.0, 1000.0, SpaceAgency.NASA),
            TelemetryDataPoint(probeId, 200.0, 2000.0, SpaceAgency.NASA),
            TelemetryDataPoint(probeId, 150.0, 1500.0, SpaceAgency.NASA),
            TelemetryDataPoint(probeId, 300.0, 3000.0, SpaceAgency.NASA),
            TelemetryDataPoint(probeId, 250.0, 2500.0, SpaceAgency.NASA)
        )

        var aggregate = AggregatedTelemetryData(maxSpeedMph = 0.0, traveledDistanceFeet = 0.0)
        readings.forEach { reading ->
            aggregate = handler.updateTotals(probeId, reading, aggregate)
        }

        assertEquals(300.0, aggregate.maxSpeedMph)
        assertEquals(10000.0, aggregate.traveledDistanceFeet)
    }

    @Test
    fun `should handle NASA probe updates`() {
        val probeId = "nasa-probe-1"
        val telemetryPoint = TelemetryDataPoint(
            probeId = probeId,
            currentSpeedMph = 500.0,
            traveledDistanceFeet = 5000.0,
            spaceAgency = SpaceAgency.NASA
        )
        val currentAggregate = AggregatedTelemetryData(
            maxSpeedMph = 0.0,
            traveledDistanceFeet = 0.0
        )

        val result = handler.updateTotals(probeId, telemetryPoint, currentAggregate)

        assertEquals(500.0, result.maxSpeedMph)
        assertEquals(5000.0, result.traveledDistanceFeet)
    }

    @Test
    fun `should handle ESA probe updates`() {
        val probeId = "esa-probe-1"
        val telemetryPoint = TelemetryDataPoint(
            probeId = probeId,
            currentSpeedMph = 600.0,
            traveledDistanceFeet = 6000.0,
            spaceAgency = SpaceAgency.ESA
        )
        val currentAggregate = AggregatedTelemetryData(
            maxSpeedMph = 0.0,
            traveledDistanceFeet = 0.0
        )

        val result = handler.updateTotals(probeId, telemetryPoint, currentAggregate)

        assertEquals(600.0, result.maxSpeedMph)
        assertEquals(6000.0, result.traveledDistanceFeet)
    }

    @Test
    fun `should handle large accumulated distances`() {
        val probeId = "probe-large"
        val currentAggregate = AggregatedTelemetryData(
            maxSpeedMph = 500.0,
            traveledDistanceFeet = 26400000.0 // 5000 miles
        )
        val newReading = TelemetryDataPoint(
            probeId = probeId,
            currentSpeedMph = 600.0,
            traveledDistanceFeet = 26400000.0, // Another 5000 miles
            spaceAgency = SpaceAgency.NASA
        )

        val result = handler.updateTotals(probeId, newReading, currentAggregate)

        assertEquals(600.0, result.maxSpeedMph)
        assertEquals(52800000.0, result.traveledDistanceFeet) // 10000 miles total
    }

    @Test
    fun `should handle decimal precision in calculations`() {
        val probeId = "probe-decimal"
        val currentAggregate = AggregatedTelemetryData(
            maxSpeedMph = 567.89,
            traveledDistanceFeet = 1234.5678
        )
        val newReading = TelemetryDataPoint(
            probeId = probeId,
            currentSpeedMph = 890.12,
            traveledDistanceFeet = 5678.9012,
            spaceAgency = SpaceAgency.ESA
        )

        val result = handler.updateTotals(probeId, newReading, currentAggregate)

        assertEquals(890.12, result.maxSpeedMph, 0.0001)
        assertEquals(6913.469, result.traveledDistanceFeet, 0.001)
    }

    @Test
    fun `should maintain equal max speed when reading matches current max`() {
        val probeId = "probe-equal"
        val currentAggregate = AggregatedTelemetryData(
            maxSpeedMph = 500.0,
            traveledDistanceFeet = 5000.0
        )
        val newReading = TelemetryDataPoint(
            probeId = probeId,
            currentSpeedMph = 500.0,
            traveledDistanceFeet = 1000.0,
            spaceAgency = SpaceAgency.NASA
        )

        val result = handler.updateTotals(probeId, newReading, currentAggregate)

        assertEquals(500.0, result.maxSpeedMph)
        assertEquals(6000.0, result.traveledDistanceFeet)
    }

    @Test
    fun `should handle realistic orbital speed scenario`() {
        val probeId = "orbital-probe"
        val readings = listOf(
            TelemetryDataPoint(probeId, 15000.0, 1000000.0, SpaceAgency.NASA),
            TelemetryDataPoint(probeId, 17500.0, 2000000.0, SpaceAgency.NASA), // Peak orbital speed
            TelemetryDataPoint(probeId, 16000.0, 1500000.0, SpaceAgency.NASA)
        )

        var aggregate = AggregatedTelemetryData(maxSpeedMph = 0.0, traveledDistanceFeet = 0.0)
        readings.forEach { reading ->
            aggregate = handler.updateTotals(probeId, reading, aggregate)
        }

        assertEquals(17500.0, aggregate.maxSpeedMph)
        assertEquals(4500000.0, aggregate.traveledDistanceFeet)
    }

    @Test
    fun `should aggregate different probes independently in concept`() {
        // This test verifies the aggregation logic works correctly for different probes
        // In actual Kafka Streams, groupByKey ensures this separation
        val probe1Id = "probe-1"
        val probe2Id = "probe-2"

        var aggregate1 = AggregatedTelemetryData(maxSpeedMph = 0.0, traveledDistanceFeet = 0.0)
        var aggregate2 = AggregatedTelemetryData(maxSpeedMph = 0.0, traveledDistanceFeet = 0.0)

        aggregate1 = handler.updateTotals(
            probe1Id,
            TelemetryDataPoint(probe1Id, 500.0, 5000.0, SpaceAgency.NASA),
            aggregate1
        )
        aggregate2 = handler.updateTotals(
            probe2Id,
            TelemetryDataPoint(probe2Id, 600.0, 6000.0, SpaceAgency.NASA),
            aggregate2
        )

        assertEquals(500.0, aggregate1.maxSpeedMph)
        assertEquals(5000.0, aggregate1.traveledDistanceFeet)
        assertEquals(600.0, aggregate2.maxSpeedMph)
        assertEquals(6000.0, aggregate2.traveledDistanceFeet)
    }
}
