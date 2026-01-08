package com.example.kafkasamplesstreams.serdes

import com.example.kafkasamplesstreams.events.SpaceAgency
import com.example.kafkasamplesstreams.events.TelemetryDataPoint
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class TelemetryDataPointSerdeTest {

    private lateinit var serde: TelemetryDataPointSerde

    @BeforeEach
    fun setup() {
        serde = TelemetryDataPointSerde()
    }

    @Test
    fun `should serialize TelemetryDataPoint to bytes`() {
        val dataPoint = TelemetryDataPoint(
            probeId = "probe-1",
            currentSpeedMph = 500.0,
            traveledDistanceFeet = 5000.0,
            spaceAgency = SpaceAgency.NASA
        )

        val bytes = serde.serializer().serialize("test-topic", dataPoint)

        assertNotNull(bytes)
        assertTrue(bytes!!.isNotEmpty())
    }

    @Test
    fun `should deserialize bytes to TelemetryDataPoint`() {
        val original = TelemetryDataPoint(
            probeId = "probe-2",
            currentSpeedMph = 600.0,
            traveledDistanceFeet = 6000.0,
            spaceAgency = SpaceAgency.ESA
        )

        val bytes = serde.serializer().serialize("test-topic", original)
        val deserialized = serde.deserializer().deserialize("test-topic", bytes)

        assertNotNull(deserialized)
        assertEquals(original.probeId, deserialized.probeId)
        assertEquals(original.currentSpeedMph, deserialized.currentSpeedMph)
        assertEquals(original.traveledDistanceFeet, deserialized.traveledDistanceFeet)
        assertEquals(original.spaceAgency, deserialized.spaceAgency)
    }

    @Test
    fun `should handle round-trip serialization with NASA probe`() {
        val original = TelemetryDataPoint(
            probeId = "nasa-probe-1",
            currentSpeedMph = 750.0,
            traveledDistanceFeet = 7500.0,
            spaceAgency = SpaceAgency.NASA
        )

        val bytes = serde.serializer().serialize("test-topic", original)
        val deserialized = serde.deserializer().deserialize("test-topic", bytes)

        assertEquals(original, deserialized)
    }

    @Test
    fun `should handle round-trip serialization with ESA probe`() {
        val original = TelemetryDataPoint(
            probeId = "esa-probe-1",
            currentSpeedMph = 850.0,
            traveledDistanceFeet = 8500.0,
            spaceAgency = SpaceAgency.ESA
        )

        val bytes = serde.serializer().serialize("test-topic", original)
        val deserialized = serde.deserializer().deserialize("test-topic", bytes)

        assertEquals(original, deserialized)
    }

    @Test
    fun `should handle zero values`() {
        val original = TelemetryDataPoint(
            probeId = "probe-0",
            currentSpeedMph = 0.0,
            traveledDistanceFeet = 0.0,
            spaceAgency = SpaceAgency.NASA
        )

        val bytes = serde.serializer().serialize("test-topic", original)
        val deserialized = serde.deserializer().deserialize("test-topic", bytes)

        assertEquals(original, deserialized)
    }

    @Test
    fun `should handle large values`() {
        val original = TelemetryDataPoint(
            probeId = "probe-large",
            currentSpeedMph = 25000.0,
            traveledDistanceFeet = 1000000.0,
            spaceAgency = SpaceAgency.ESA
        )

        val bytes = serde.serializer().serialize("test-topic", original)
        val deserialized = serde.deserializer().deserialize("test-topic", bytes)

        assertEquals(original, deserialized)
    }

    @Test
    fun `should handle decimal precision`() {
        val original = TelemetryDataPoint(
            probeId = "probe-decimal",
            currentSpeedMph = 567.89,
            traveledDistanceFeet = 1234.5678,
            spaceAgency = SpaceAgency.NASA
        )

        val bytes = serde.serializer().serialize("test-topic", original)
        val deserialized = serde.deserializer().deserialize("test-topic", bytes)

        assertEquals(original.probeId, deserialized.probeId)
        assertEquals(original.currentSpeedMph, deserialized.currentSpeedMph, 0.0001)
        assertEquals(original.traveledDistanceFeet, deserialized.traveledDistanceFeet, 0.0001)
        assertEquals(original.spaceAgency, deserialized.spaceAgency)
    }

    @Test
    fun `should handle null deserialization`() {
        val deserialized = serde.deserializer().deserialize("test-topic", null)

        assertNull(deserialized)
    }

    @Test
    fun `should serialize multiple data points independently`() {
        val dataPoint1 = TelemetryDataPoint(
            probeId = "probe-1",
            currentSpeedMph = 100.0,
            traveledDistanceFeet = 1000.0,
            spaceAgency = SpaceAgency.NASA
        )
        val dataPoint2 = TelemetryDataPoint(
            probeId = "probe-2",
            currentSpeedMph = 200.0,
            traveledDistanceFeet = 2000.0,
            spaceAgency = SpaceAgency.ESA
        )

        val bytes1 = serde.serializer().serialize("test-topic", dataPoint1)
        val bytes2 = serde.serializer().serialize("test-topic", dataPoint2)

        val deserialized1 = serde.deserializer().deserialize("test-topic", bytes1)
        val deserialized2 = serde.deserializer().deserialize("test-topic", bytes2)

        assertEquals(dataPoint1, deserialized1)
        assertEquals(dataPoint2, deserialized2)
        assertNotEquals(deserialized1, deserialized2)
    }

    @Test
    fun `should work with different topic names`() {
        val dataPoint = TelemetryDataPoint(
            probeId = "probe-topic-test",
            currentSpeedMph = 300.0,
            traveledDistanceFeet = 3000.0,
            spaceAgency = SpaceAgency.NASA
        )

        val bytes1 = serde.serializer().serialize("topic-1", dataPoint)
        val bytes2 = serde.serializer().serialize("topic-2", dataPoint)

        val deserialized1 = serde.deserializer().deserialize("topic-1", bytes1)
        val deserialized2 = serde.deserializer().deserialize("topic-2", bytes2)

        assertEquals(dataPoint, deserialized1)
        assertEquals(dataPoint, deserialized2)
    }

    @Test
    fun `should handle realistic telemetry data`() {
        val dataPoint = TelemetryDataPoint(
            probeId = "voyager-1",
            currentSpeedMph = 38000.0, // Actual Voyager 1 speed
            traveledDistanceFeet = 100000000.0,
            spaceAgency = SpaceAgency.NASA
        )

        val bytes = serde.serializer().serialize("telemetry-topic", dataPoint)
        val deserialized = serde.deserializer().deserialize("telemetry-topic", bytes)

        assertEquals(dataPoint, deserialized)
    }
}
