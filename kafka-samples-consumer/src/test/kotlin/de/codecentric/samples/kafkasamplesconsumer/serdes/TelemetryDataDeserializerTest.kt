package de.codecentric.samples.kafkasamplesconsumer.serdes

import de.codecentric.samples.kafkasamplesconsumer.event.ImperialTelemetryData
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class TelemetryDataDeserializerTest {

    private lateinit var deserializer: TelemetryDataDeserializer

    @BeforeEach
    fun setup() {
        deserializer = TelemetryDataDeserializer()
        deserializer.configure(emptyMap<String, Any>(), false)
    }

    @Test
    fun `should deserialize valid JSON to ImperialTelemetryData`() {
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
    fun `should deserialize JSON with integer values`() {
        val json = """
            {
                "traveledDistanceFeet": 1000,
                "maxSpeedMph": 100
            }
        """.trimIndent()

        val result = deserializer.deserialize("test-topic", json.toByteArray())

        assertNotNull(result)
        assertEquals(1000.0, result.totalDistanceTraveledFeet)
        assertEquals(100.0, result.maxSpeedMph)
    }

    @Test
    fun `should deserialize JSON with zero values`() {
        val json = """
            {
                "traveledDistanceFeet": 0.0,
                "maxSpeedMph": 0.0
            }
        """.trimIndent()

        val result = deserializer.deserialize("test-topic", json.toByteArray())

        assertNotNull(result)
        assertEquals(0.0, result.totalDistanceTraveledFeet)
        assertEquals(0.0, result.maxSpeedMph)
    }

    @Test
    fun `should deserialize JSON with large values`() {
        val json = """
            {
                "traveledDistanceFeet": 1000000.0,
                "maxSpeedMph": 25000.0
            }
        """.trimIndent()

        val result = deserializer.deserialize("test-topic", json.toByteArray())

        assertNotNull(result)
        assertEquals(1000000.0, result.totalDistanceTraveledFeet)
        assertEquals(25000.0, result.maxSpeedMph)
    }

    @Test
    fun `should deserialize JSON with decimal precision`() {
        val json = """
            {
                "traveledDistanceFeet": 1234.5678,
                "maxSpeedMph": 567.89
            }
        """.trimIndent()

        val result = deserializer.deserialize("test-topic", json.toByteArray())

        assertNotNull(result)
        assertEquals(1234.5678, result.totalDistanceTraveledFeet, 0.0001)
        assertEquals(567.89, result.maxSpeedMph, 0.0001)
    }

    @Test
    fun `should deserialize compact JSON`() {
        val json = """{"traveledDistanceFeet":7500.0,"maxSpeedMph":750.0}"""

        val result = deserializer.deserialize("test-topic", json.toByteArray())

        assertNotNull(result)
        assertEquals(7500.0, result.totalDistanceTraveledFeet)
        assertEquals(750.0, result.maxSpeedMph)
    }

    @Test
    fun `should deserialize JSON with extra whitespace`() {
        val json = """
            {
                "traveledDistanceFeet"  :  8000.0  ,
                "maxSpeedMph"  :  800.0
            }
        """.trimIndent()

        val result = deserializer.deserialize("test-topic", json.toByteArray())

        assertNotNull(result)
        assertEquals(8000.0, result.totalDistanceTraveledFeet)
        assertEquals(800.0, result.maxSpeedMph)
    }

    @Test
    fun `should handle null byte array`() {
        val result = deserializer.deserialize("test-topic", null)

        assertNull(result)
    }

    @Test
    fun `should deserialize multiple messages independently`() {
        val json1 = """{"traveledDistanceFeet":1000.0,"maxSpeedMph":100.0}"""
        val json2 = """{"traveledDistanceFeet":2000.0,"maxSpeedMph":200.0}"""

        val result1 = deserializer.deserialize("test-topic", json1.toByteArray())
        val result2 = deserializer.deserialize("test-topic", json2.toByteArray())

        assertNotNull(result1)
        assertNotNull(result2)
        assertEquals(1000.0, result1.totalDistanceTraveledFeet)
        assertEquals(100.0, result1.maxSpeedMph)
        assertEquals(2000.0, result2.totalDistanceTraveledFeet)
        assertEquals(200.0, result2.maxSpeedMph)
    }

    @Test
    fun `should work with different topic names`() {
        val json = """{"traveledDistanceFeet":3000.0,"maxSpeedMph":300.0}"""

        val result1 = deserializer.deserialize("nasa-telemetry", json.toByteArray())
        val result2 = deserializer.deserialize("esa-telemetry", json.toByteArray())

        assertNotNull(result1)
        assertNotNull(result2)
        assertEquals(result1.totalDistanceTraveledFeet, result2.totalDistanceTraveledFeet)
        assertEquals(result1.maxSpeedMph, result2.maxSpeedMph)
    }

    @Test
    fun `should handle scientific notation`() {
        val json = """{"traveledDistanceFeet":1.5e6,"maxSpeedMph":2.5e4}"""

        val result = deserializer.deserialize("test-topic", json.toByteArray())

        assertNotNull(result)
        assertEquals(1500000.0, result.totalDistanceTraveledFeet, 0.1)
        assertEquals(25000.0, result.maxSpeedMph, 0.1)
    }

    @Test
    fun `should deserialize realistic telemetry data`() {
        val json = """
            {
                "traveledDistanceFeet": 26400000.0,
                "maxSpeedMph": 17500.0
            }
        """.trimIndent()

        val result = deserializer.deserialize("telemetry-topic", json.toByteArray())

        assertNotNull(result)
        assertEquals(26400000.0, result.totalDistanceTraveledFeet)
        assertEquals(17500.0, result.maxSpeedMph)
    }
}
