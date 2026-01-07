package de.codecentric.samples.kafkasamplesconsumer

import de.codecentric.samples.kafkasamplesconsumer.event.ImperialTelemetryData
import de.codecentric.samples.kafkasamplesconsumer.serdes.TelemetryDataDeserializer
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest

@SpringBootTest
class TelemetryDataDeserializerIntegrationTest {

    private lateinit var deserializer: TelemetryDataDeserializer

    @BeforeEach
    fun setup() {
        deserializer = TelemetryDataDeserializer()
        deserializer.configure(emptyMap<String, Any>(), false)
    }

    @Test
    fun `should deserialize imperial telemetry data in Spring context`() {
        val json = """
            {
                "traveledDistanceFeet": 5000.0,
                "maxSpeedMph": 500.0
            }
        """.trimIndent()

        val result = deserializer.deserialize("test-topic", json.toByteArray())

        assertNotNull(result)
        assertEquals(5000.0, result.totalDistanceTraveledFeet)
        assertEquals(500.0, result.maxSpeedMph)
    }

    @Test
    fun `should handle multiple deserializations`() {
        val data = listOf(
            """{"traveledDistanceFeet":1000.0,"maxSpeedMph":100.0}""",
            """{"traveledDistanceFeet":2000.0,"maxSpeedMph":200.0}""",
            """{"traveledDistanceFeet":3000.0,"maxSpeedMph":300.0}"""
        )

        val results = data.map { json ->
            deserializer.deserialize("test-topic", json.toByteArray())
        }

        assertEquals(3, results.size)
        assertEquals(1000.0, results[0].totalDistanceTraveledFeet)
        assertEquals(2000.0, results[1].totalDistanceTraveledFeet)
        assertEquals(3000.0, results[2].totalDistanceTraveledFeet)
    }
}
