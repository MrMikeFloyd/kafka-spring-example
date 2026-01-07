package de.codecentric.samples.kafkasamplesproducer

import de.codecentric.samples.kafkasamplesproducer.event.SpaceAgency
import de.codecentric.samples.kafkasamplesproducer.event.TelemetryData
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.cloud.stream.function.StreamBridge
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.Message

class TelemetryDataStreamBridgeTest {

    private lateinit var streamBridge: StreamBridge
    private lateinit var telemetryDataStreamBridge: TelemetryDataStreamBridge

    @BeforeEach
    fun setup() {
        streamBridge = mockk(relaxed = true)
        telemetryDataStreamBridge = TelemetryDataStreamBridge(streamBridge)
    }

    @Test
    fun `should send telemetry data to stream bridge`() {
        val telemetryData = TelemetryData(
            probeId = "probe-1",
            currentSpeedMph = 500.0,
            traveledDistanceFeet = 5000.0,
            spaceAgency = SpaceAgency.NASA
        )

        every { streamBridge.send(any<String>(), any<Message<*>>()) } returns true

        telemetryDataStreamBridge.send(telemetryData)

        verify(exactly = 1) {
            streamBridge.send("telemetry-data-out-0", any<Message<*>>())
        }
    }

    @Test
    fun `should send message with correct payload`() {
        val telemetryData = TelemetryData(
            probeId = "probe-2",
            currentSpeedMph = 750.0,
            traveledDistanceFeet = 7500.0,
            spaceAgency = SpaceAgency.ESA
        )

        val messageSlot = slot<Message<TelemetryData>>()
        every { streamBridge.send(any<String>(), capture(messageSlot)) } returns true

        telemetryDataStreamBridge.send(telemetryData)

        val capturedMessage = messageSlot.captured
        assertEquals(telemetryData, capturedMessage.payload)
    }

    @Test
    fun `should set message key to probe ID for partitioning`() {
        val telemetryData = TelemetryData(
            probeId = "probe-3",
            currentSpeedMph = 600.0,
            traveledDistanceFeet = 6000.0,
            spaceAgency = SpaceAgency.NASA
        )

        val messageSlot = slot<Message<TelemetryData>>()
        every { streamBridge.send(any<String>(), capture(messageSlot)) } returns true

        telemetryDataStreamBridge.send(telemetryData)

        val capturedMessage = messageSlot.captured
        assertEquals("probe-3", capturedMessage.headers[KafkaHeaders.MESSAGE_KEY])
    }

    @Test
    fun `should send to correct destination`() {
        val telemetryData = TelemetryData(
            probeId = "probe-4",
            currentSpeedMph = 800.0,
            traveledDistanceFeet = 8000.0,
            spaceAgency = SpaceAgency.ESA
        )

        val destinationSlot = slot<String>()
        every { streamBridge.send(capture(destinationSlot), any<Message<*>>()) } returns true

        telemetryDataStreamBridge.send(telemetryData)

        assertEquals("telemetry-data-out-0", destinationSlot.captured)
    }

    @Test
    fun `should send multiple messages with different probe IDs`() {
        val telemetryData1 = TelemetryData(
            probeId = "probe-A",
            currentSpeedMph = 500.0,
            traveledDistanceFeet = 5000.0,
            spaceAgency = SpaceAgency.NASA
        )
        val telemetryData2 = TelemetryData(
            probeId = "probe-B",
            currentSpeedMph = 600.0,
            traveledDistanceFeet = 6000.0,
            spaceAgency = SpaceAgency.ESA
        )

        val messageSlots = mutableListOf<Message<TelemetryData>>()
        every { streamBridge.send(any<String>(), capture(messageSlots)) } returns true

        telemetryDataStreamBridge.send(telemetryData1)
        telemetryDataStreamBridge.send(telemetryData2)

        verify(exactly = 2) {
            streamBridge.send("telemetry-data-out-0", any<Message<*>>())
        }
        assertEquals(2, messageSlots.size)
        assertEquals("probe-A", messageSlots[0].headers[KafkaHeaders.MESSAGE_KEY])
        assertEquals("probe-B", messageSlots[1].headers[KafkaHeaders.MESSAGE_KEY])
    }

    @Test
    fun `should handle sending with same probe ID to ensure same partition`() {
        val telemetryData1 = TelemetryData(
            probeId = "probe-1",
            currentSpeedMph = 500.0,
            traveledDistanceFeet = 5000.0,
            spaceAgency = SpaceAgency.NASA
        )
        val telemetryData2 = TelemetryData(
            probeId = "probe-1",
            currentSpeedMph = 550.0,
            traveledDistanceFeet = 5500.0,
            spaceAgency = SpaceAgency.NASA
        )

        val messageSlots = mutableListOf<Message<TelemetryData>>()
        every { streamBridge.send(any<String>(), capture(messageSlots)) } returns true

        telemetryDataStreamBridge.send(telemetryData1)
        telemetryDataStreamBridge.send(telemetryData2)

        assertEquals(2, messageSlots.size)
        assertEquals("probe-1", messageSlots[0].headers[KafkaHeaders.MESSAGE_KEY])
        assertEquals("probe-1", messageSlots[1].headers[KafkaHeaders.MESSAGE_KEY])
    }

    @Test
    fun `should send complete telemetry data with all fields`() {
        val timestamp = "2024-01-01T12:00:00Z"
        val telemetryData = TelemetryData(
            probeId = "probe-5",
            timestamp = timestamp,
            currentSpeedMph = 900.0,
            traveledDistanceFeet = 9000.0,
            spaceAgency = SpaceAgency.NASA
        )

        val messageSlot = slot<Message<TelemetryData>>()
        every { streamBridge.send(any<String>(), capture(messageSlot)) } returns true

        telemetryDataStreamBridge.send(telemetryData)

        val capturedPayload = messageSlot.captured.payload
        assertEquals("probe-5", capturedPayload.probeId)
        assertEquals(timestamp, capturedPayload.timestamp)
        assertEquals(900.0, capturedPayload.currentSpeedMph)
        assertEquals(9000.0, capturedPayload.traveledDistanceFeet)
        assertEquals(SpaceAgency.NASA, capturedPayload.spaceAgency)
    }
}
