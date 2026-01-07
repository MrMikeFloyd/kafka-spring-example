package de.codecentric.samples.kafkasamplesconsumer

import de.codecentric.samples.kafkasamplesconsumer.event.ImperialTelemetryData
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.Message
import org.springframework.messaging.support.MessageBuilder
import java.util.function.Consumer

class KafkaConsumerConfigurationTest {

    private lateinit var configuration: KafkaConsumerConfiguration
    private lateinit var nasaConsumer: Consumer<Message<ImperialTelemetryData>>
    private lateinit var esaConsumer: Consumer<Message<ImperialTelemetryData>>

    @BeforeEach
    fun setup() {
        configuration = KafkaConsumerConfiguration()
        nasaConsumer = configuration.processNasaTelemetryData()
        esaConsumer = configuration.processEsaTelemetryData()
    }

    @Test
    fun `should create NASA telemetry consumer bean`() {
        assertNotNull(nasaConsumer)
    }

    @Test
    fun `should create ESA telemetry consumer bean`() {
        assertNotNull(esaConsumer)
    }

    @Test
    fun `should process NASA telemetry data message`() {
        val telemetryData = ImperialTelemetryData(
            totalDistanceTraveledFeet = 5000.0,
            maxSpeedMph = 500.0
        )
        val message = MessageBuilder
            .withPayload(telemetryData)
            .setHeader(KafkaHeaders.RECEIVED_MESSAGE_KEY, "nasa-probe-1")
            .build()

        assertDoesNotThrow {
            nasaConsumer.accept(message)
        }
    }

    @Test
    fun `should process ESA telemetry data message`() {
        val telemetryData = ImperialTelemetryData(
            totalDistanceTraveledFeet = 6000.0,
            maxSpeedMph = 600.0
        )
        val message = MessageBuilder
            .withPayload(telemetryData)
            .setHeader(KafkaHeaders.RECEIVED_MESSAGE_KEY, "esa-probe-1")
            .build()

        assertDoesNotThrow {
            esaConsumer.accept(message)
        }
    }

    @Test
    fun `should process NASA message with zero values`() {
        val telemetryData = ImperialTelemetryData(
            totalDistanceTraveledFeet = 0.0,
            maxSpeedMph = 0.0
        )
        val message = MessageBuilder
            .withPayload(telemetryData)
            .setHeader(KafkaHeaders.RECEIVED_MESSAGE_KEY, "nasa-probe-0")
            .build()

        assertDoesNotThrow {
            nasaConsumer.accept(message)
        }
    }

    @Test
    fun `should process ESA message with zero values`() {
        val telemetryData = ImperialTelemetryData(
            totalDistanceTraveledFeet = 0.0,
            maxSpeedMph = 0.0
        )
        val message = MessageBuilder
            .withPayload(telemetryData)
            .setHeader(KafkaHeaders.RECEIVED_MESSAGE_KEY, "esa-probe-0")
            .build()

        assertDoesNotThrow {
            esaConsumer.accept(message)
        }
    }

    @Test
    fun `should process NASA message with large values`() {
        val telemetryData = ImperialTelemetryData(
            totalDistanceTraveledFeet = 1000000.0,
            maxSpeedMph = 25000.0
        )
        val message = MessageBuilder
            .withPayload(telemetryData)
            .setHeader(KafkaHeaders.RECEIVED_MESSAGE_KEY, "nasa-probe-large")
            .build()

        assertDoesNotThrow {
            nasaConsumer.accept(message)
        }
    }

    @Test
    fun `should process ESA message with large values`() {
        val telemetryData = ImperialTelemetryData(
            totalDistanceTraveledFeet = 2000000.0,
            maxSpeedMph = 30000.0
        )
        val message = MessageBuilder
            .withPayload(telemetryData)
            .setHeader(KafkaHeaders.RECEIVED_MESSAGE_KEY, "esa-probe-large")
            .build()

        assertDoesNotThrow {
            esaConsumer.accept(message)
        }
    }

    @Test
    fun `should process multiple NASA messages sequentially`() {
        val messages = listOf(
            createTestMessage("probe-1", 1000.0, 100.0),
            createTestMessage("probe-2", 2000.0, 200.0),
            createTestMessage("probe-3", 3000.0, 300.0)
        )

        messages.forEach { message ->
            assertDoesNotThrow {
                nasaConsumer.accept(message)
            }
        }
    }

    @Test
    fun `should process multiple ESA messages sequentially`() {
        val messages = listOf(
            createTestMessage("probe-4", 4000.0, 400.0),
            createTestMessage("probe-5", 5000.0, 500.0),
            createTestMessage("probe-6", 6000.0, 600.0)
        )

        messages.forEach { message ->
            assertDoesNotThrow {
                esaConsumer.accept(message)
            }
        }
    }

    @Test
    fun `should process NASA message with decimal precision`() {
        val telemetryData = ImperialTelemetryData(
            totalDistanceTraveledFeet = 1234.5678,
            maxSpeedMph = 567.89
        )
        val message = MessageBuilder
            .withPayload(telemetryData)
            .setHeader(KafkaHeaders.RECEIVED_MESSAGE_KEY, "nasa-probe-decimal")
            .build()

        assertDoesNotThrow {
            nasaConsumer.accept(message)
        }
    }

    @Test
    fun `should process ESA message with decimal precision`() {
        val telemetryData = ImperialTelemetryData(
            totalDistanceTraveledFeet = 9876.5432,
            maxSpeedMph = 321.98
        )
        val message = MessageBuilder
            .withPayload(telemetryData)
            .setHeader(KafkaHeaders.RECEIVED_MESSAGE_KEY, "esa-probe-decimal")
            .build()

        assertDoesNotThrow {
            esaConsumer.accept(message)
        }
    }

    @Test
    fun `should handle NASA message without message key header`() {
        val telemetryData = ImperialTelemetryData(
            totalDistanceTraveledFeet = 5000.0,
            maxSpeedMph = 500.0
        )
        val message = MessageBuilder
            .withPayload(telemetryData)
            .build()

        assertDoesNotThrow {
            nasaConsumer.accept(message)
        }
    }

    @Test
    fun `should handle ESA message without message key header`() {
        val telemetryData = ImperialTelemetryData(
            totalDistanceTraveledFeet = 6000.0,
            maxSpeedMph = 600.0
        )
        val message = MessageBuilder
            .withPayload(telemetryData)
            .build()

        assertDoesNotThrow {
            esaConsumer.accept(message)
        }
    }

    @Test
    fun `should process NASA message with realistic orbital speed`() {
        val telemetryData = ImperialTelemetryData(
            totalDistanceTraveledFeet = 26400000.0, // 5000 miles
            maxSpeedMph = 17500.0 // Low Earth orbit speed
        )
        val message = MessageBuilder
            .withPayload(telemetryData)
            .setHeader(KafkaHeaders.RECEIVED_MESSAGE_KEY, "nasa-orbital")
            .build()

        assertDoesNotThrow {
            nasaConsumer.accept(message)
        }
    }

    @Test
    fun `should process ESA message with realistic orbital speed`() {
        val telemetryData = ImperialTelemetryData(
            totalDistanceTraveledFeet = 52800000.0, // 10000 miles
            maxSpeedMph = 18000.0
        )
        val message = MessageBuilder
            .withPayload(telemetryData)
            .setHeader(KafkaHeaders.RECEIVED_MESSAGE_KEY, "esa-orbital")
            .build()

        assertDoesNotThrow {
            esaConsumer.accept(message)
        }
    }

    @Test
    fun `should handle different probe IDs for NASA`() {
        val probeIds = listOf("0", "1", "2", "3", "4")

        probeIds.forEach { probeId ->
            val message = createTestMessage(probeId, 1000.0, 100.0)
            assertDoesNotThrow {
                nasaConsumer.accept(message)
            }
        }
    }

    @Test
    fun `should handle different probe IDs for ESA`() {
        val probeIds = listOf("5", "6", "7", "8", "9")

        probeIds.forEach { probeId ->
            val message = createTestMessage(probeId, 2000.0, 200.0)
            assertDoesNotThrow {
                esaConsumer.accept(message)
            }
        }
    }

    @Test
    fun `should process NASA and ESA messages with same probe ID`() {
        val probeId = "probe-shared"
        val nasaMessage = createTestMessage(probeId, 1000.0, 100.0)
        val esaMessage = createTestMessage(probeId, 2000.0, 200.0)

        assertDoesNotThrow {
            nasaConsumer.accept(nasaMessage)
            esaConsumer.accept(esaMessage)
        }
    }

    private fun createTestMessage(
        probeId: String,
        distance: Double,
        speed: Double
    ): Message<ImperialTelemetryData> {
        val telemetryData = ImperialTelemetryData(
            totalDistanceTraveledFeet = distance,
            maxSpeedMph = speed
        )
        return MessageBuilder
            .withPayload(telemetryData)
            .setHeader(KafkaHeaders.RECEIVED_MESSAGE_KEY, probeId)
            .build()
    }
}
