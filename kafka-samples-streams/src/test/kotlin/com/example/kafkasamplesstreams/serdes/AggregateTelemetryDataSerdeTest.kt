package com.example.kafkasamplesstreams.serdes

import com.example.kafkasamplesstreams.events.AggregatedTelemetryData
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class AggregateTelemetryDataSerdeTest {

    private lateinit var serde: AggregateTelemetryDataSerde

    @BeforeEach
    fun setup() {
        serde = AggregateTelemetryDataSerde()
    }

    @Test
    fun `should serialize AggregatedTelemetryData to bytes`() {
        val aggregatedData = AggregatedTelemetryData(
            maxSpeedMph = 1000.0,
            traveledDistanceFeet = 50000.0
        )

        val bytes = serde.serializer().serialize("test-topic", aggregatedData)

        assertNotNull(bytes)
        assertTrue(bytes.isNotEmpty())
    }

    @Test
    fun `should deserialize bytes to AggregatedTelemetryData`() {
        val original = AggregatedTelemetryData(
            maxSpeedMph = 1500.0,
            traveledDistanceFeet = 75000.0
        )

        val bytes = serde.serializer().serialize("test-topic", original)
        val deserialized = serde.deserializer().deserialize("test-topic", bytes)

        assertNotNull(deserialized)
        assertEquals(original.maxSpeedMph, deserialized.maxSpeedMph)
        assertEquals(original.traveledDistanceFeet, deserialized.traveledDistanceFeet)
    }

    @Test
    fun `should handle round-trip serialization`() {
        val original = AggregatedTelemetryData(
            maxSpeedMph = 2000.0,
            traveledDistanceFeet = 100000.0
        )

        val bytes = serde.serializer().serialize("test-topic", original)
        val deserialized = serde.deserializer().deserialize("test-topic", bytes)

        assertEquals(original, deserialized)
    }

    @Test
    fun `should handle zero values`() {
        val original = AggregatedTelemetryData(
            maxSpeedMph = 0.0,
            traveledDistanceFeet = 0.0
        )

        val bytes = serde.serializer().serialize("test-topic", original)
        val deserialized = serde.deserializer().deserialize("test-topic", bytes)

        assertEquals(original, deserialized)
    }

    @Test
    fun `should handle large values`() {
        val original = AggregatedTelemetryData(
            maxSpeedMph = 50000.0,
            traveledDistanceFeet = 10000000.0
        )

        val bytes = serde.serializer().serialize("test-topic", original)
        val deserialized = serde.deserializer().deserialize("test-topic", bytes)

        assertEquals(original, deserialized)
    }

    @Test
    fun `should handle decimal precision`() {
        val original = AggregatedTelemetryData(
            maxSpeedMph = 1234.5678,
            traveledDistanceFeet = 98765.4321
        )

        val bytes = serde.serializer().serialize("test-topic", original)
        val deserialized = serde.deserializer().deserialize("test-topic", bytes)

        assertEquals(original.maxSpeedMph, deserialized.maxSpeedMph, 0.0001)
        assertEquals(original.traveledDistanceFeet, deserialized.traveledDistanceFeet, 0.0001)
    }

    @Test
    fun `should handle null deserialization`() {
        val deserialized = serde.deserializer().deserialize("test-topic", null)

        assertNull(deserialized)
    }

    @Test
    fun `should serialize multiple aggregated data independently`() {
        val data1 = AggregatedTelemetryData(
            maxSpeedMph = 1000.0,
            traveledDistanceFeet = 50000.0
        )
        val data2 = AggregatedTelemetryData(
            maxSpeedMph = 2000.0,
            traveledDistanceFeet = 100000.0
        )

        val bytes1 = serde.serializer().serialize("test-topic", data1)
        val bytes2 = serde.serializer().serialize("test-topic", data2)

        val deserialized1 = serde.deserializer().deserialize("test-topic", bytes1)
        val deserialized2 = serde.deserializer().deserialize("test-topic", bytes2)

        assertEquals(data1, deserialized1)
        assertEquals(data2, deserialized2)
        assertNotEquals(deserialized1, deserialized2)
    }

    @Test
    fun `should work with different topic names`() {
        val aggregatedData = AggregatedTelemetryData(
            maxSpeedMph = 3000.0,
            traveledDistanceFeet = 150000.0
        )

        val bytes1 = serde.serializer().serialize("nasa-aggregates", aggregatedData)
        val bytes2 = serde.serializer().serialize("esa-aggregates", aggregatedData)

        val deserialized1 = serde.deserializer().deserialize("nasa-aggregates", bytes1)
        val deserialized2 = serde.deserializer().deserialize("esa-aggregates", bytes2)

        assertEquals(aggregatedData, deserialized1)
        assertEquals(aggregatedData, deserialized2)
    }

    @Test
    fun `should handle realistic aggregated telemetry data`() {
        val aggregatedData = AggregatedTelemetryData(
            maxSpeedMph = 17500.0, // Low Earth orbit speed
            traveledDistanceFeet = 26400000.0 // 5000 miles
        )

        val bytes = serde.serializer().serialize("aggregated-telemetry", aggregatedData)
        val deserialized = serde.deserializer().deserialize("aggregated-telemetry", bytes)

        assertEquals(aggregatedData, deserialized)
    }

    @Test
    fun `should handle accumulated distance over multiple measurements`() {
        val aggregatedData = AggregatedTelemetryData(
            maxSpeedMph = 5000.0,
            traveledDistanceFeet = 528000000.0 // 100,000 miles
        )

        val bytes = serde.serializer().serialize("test-topic", aggregatedData)
        val deserialized = serde.deserializer().deserialize("test-topic", bytes)

        assertEquals(aggregatedData, deserialized)
    }

    @Test
    fun `should preserve data integrity through serialization`() {
        val original = AggregatedTelemetryData(
            maxSpeedMph = 12345.67,
            traveledDistanceFeet = 9876543.21
        )

        // Serialize and deserialize multiple times
        var bytes = serde.serializer().serialize("test-topic", original)
        var deserialized = serde.deserializer().deserialize("test-topic", bytes)

        bytes = serde.serializer().serialize("test-topic", deserialized)
        deserialized = serde.deserializer().deserialize("test-topic", bytes)

        assertEquals(original.maxSpeedMph, deserialized.maxSpeedMph, 0.01)
        assertEquals(original.traveledDistanceFeet, deserialized.traveledDistanceFeet, 0.01)
    }
}
