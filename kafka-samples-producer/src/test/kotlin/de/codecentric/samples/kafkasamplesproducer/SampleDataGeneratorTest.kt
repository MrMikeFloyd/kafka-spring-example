package de.codecentric.samples.kafkasamplesproducer

import de.codecentric.samples.kafkasamplesproducer.event.SpaceAgency
import de.codecentric.samples.kafkasamplesproducer.event.TelemetryData
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.RepeatedTest
import org.junit.jupiter.api.Test

class SampleDataGeneratorTest {

    private lateinit var telemetryDataStreamBridge: TelemetryDataStreamBridge
    private lateinit var sampleDataGenerator: SampleDataGenerator

    @BeforeEach
    fun setup() {
        telemetryDataStreamBridge = mockk(relaxed = true)
        sampleDataGenerator = SampleDataGenerator(telemetryDataStreamBridge)
    }

    @Test
    fun `should emit sample telemetry data`() {
        sampleDataGenerator.emitSampleTelemetryData()

        verify(exactly = 1) {
            telemetryDataStreamBridge.send(any<TelemetryData>())
        }
    }

    @Test
    fun `should generate telemetry data with probe ID between 0 and 9`() {
        val telemetryDataSlot = slot<TelemetryData>()

        sampleDataGenerator.emitSampleTelemetryData()

        verify { telemetryDataStreamBridge.send(capture(telemetryDataSlot)) }

        val capturedData = telemetryDataSlot.captured
        val probeIdAsInt = capturedData.probeId.toIntOrNull()
        assertNotNull(probeIdAsInt)
        assertTrue(probeIdAsInt!! in 0..9, "Probe ID should be between 0 and 9, but was $probeIdAsInt")
    }

    @Test
    fun `should generate speed within valid range`() {
        val telemetryDataSlot = slot<TelemetryData>()

        sampleDataGenerator.emitSampleTelemetryData()

        verify { telemetryDataStreamBridge.send(capture(telemetryDataSlot)) }

        val capturedData = telemetryDataSlot.captured
        assertTrue(capturedData.currentSpeedMph >= 0.0, "Speed should be >= 0")
        assertTrue(capturedData.currentSpeedMph < 1000.0, "Speed should be < 1000")
    }

    @Test
    fun `should generate distance within valid range`() {
        val telemetryDataSlot = slot<TelemetryData>()

        sampleDataGenerator.emitSampleTelemetryData()

        verify { telemetryDataStreamBridge.send(capture(telemetryDataSlot)) }

        val capturedData = telemetryDataSlot.captured
        assertTrue(capturedData.traveledDistanceFeet >= 1.0, "Distance should be >= 1.0")
        assertTrue(capturedData.traveledDistanceFeet < 10000.0, "Distance should be < 10000")
    }

    @Test
    fun `should assign NASA to probe IDs less than 5`() {
        val telemetryDataSlots = mutableListOf<TelemetryData>()

        // Run multiple times to capture different random values
        repeat(50) {
            sampleDataGenerator.emitSampleTelemetryData()
        }

        verify(atLeast = 1) {
            telemetryDataStreamBridge.send(capture(telemetryDataSlots))
        }

        // Check that probe IDs < 5 are assigned to NASA
        val nasaProbes = telemetryDataSlots.filter { it.probeId.toInt() < 5 }
        assertTrue(nasaProbes.isNotEmpty(), "Should have at least one NASA probe")
        assertTrue(
            nasaProbes.all { it.spaceAgency == SpaceAgency.NASA },
            "All probes with ID < 5 should belong to NASA"
        )
    }

    @Test
    fun `should assign ESA to probe IDs 5 or greater`() {
        val telemetryDataSlots = mutableListOf<TelemetryData>()

        // Run multiple times to capture different random values
        repeat(50) {
            sampleDataGenerator.emitSampleTelemetryData()
        }

        verify(atLeast = 1) {
            telemetryDataStreamBridge.send(capture(telemetryDataSlots))
        }

        // Check that probe IDs >= 5 are assigned to ESA
        val esaProbes = telemetryDataSlots.filter { it.probeId.toInt() >= 5 }
        assertTrue(esaProbes.isNotEmpty(), "Should have at least one ESA probe")
        assertTrue(
            esaProbes.all { it.spaceAgency == SpaceAgency.ESA },
            "All probes with ID >= 5 should belong to ESA"
        )
    }

    @Test
    fun `should generate telemetry data with timestamp`() {
        val telemetryDataSlot = slot<TelemetryData>()

        sampleDataGenerator.emitSampleTelemetryData()

        verify { telemetryDataStreamBridge.send(capture(telemetryDataSlot)) }

        val capturedData = telemetryDataSlot.captured
        assertNotNull(capturedData.timestamp)
        assertTrue(capturedData.timestamp.isNotBlank())
    }

    @Test
    fun `should generate different data on multiple calls`() {
        val telemetryDataSlots = mutableListOf<TelemetryData>()

        repeat(20) {
            sampleDataGenerator.emitSampleTelemetryData()
        }

        verify(exactly = 20) {
            telemetryDataStreamBridge.send(capture(telemetryDataSlots))
        }

        // At least some values should be different (given randomness)
        val uniqueSpeeds = telemetryDataSlots.map { it.currentSpeedMph }.distinct()
        val uniqueDistances = telemetryDataSlots.map { it.traveledDistanceFeet }.distinct()

        assertTrue(uniqueSpeeds.size > 1, "Should generate different speed values")
        assertTrue(uniqueDistances.size > 1, "Should generate different distance values")
    }

    @RepeatedTest(10)
    fun `should consistently generate valid telemetry data`() {
        val telemetryDataSlot = slot<TelemetryData>()

        sampleDataGenerator.emitSampleTelemetryData()

        verify { telemetryDataStreamBridge.send(capture(telemetryDataSlot)) }

        val capturedData = telemetryDataSlot.captured

        // Validate all fields
        assertNotNull(capturedData.probeId)
        assertTrue(capturedData.probeId.toInt() in 0..9)
        assertNotNull(capturedData.timestamp)
        assertTrue(capturedData.currentSpeedMph >= 0.0 && capturedData.currentSpeedMph < 1000.0)
        assertTrue(capturedData.traveledDistanceFeet >= 1.0 && capturedData.traveledDistanceFeet < 10000.0)
        assertTrue(capturedData.spaceAgency in listOf(SpaceAgency.NASA, SpaceAgency.ESA))
    }

    @Test
    fun `should generate data matching space agency assignment logic`() {
        val telemetryDataSlots = mutableListOf<TelemetryData>()

        repeat(100) {
            sampleDataGenerator.emitSampleTelemetryData()
        }

        verify(exactly = 100) {
            telemetryDataStreamBridge.send(capture(telemetryDataSlots))
        }

        // Verify the assignment logic is correct for all generated data
        telemetryDataSlots.forEach { data ->
            val probeId = data.probeId.toInt()
            if (probeId < 5) {
                assertEquals(
                    SpaceAgency.NASA,
                    data.spaceAgency,
                    "Probe $probeId should be assigned to NASA"
                )
            } else {
                assertEquals(
                    SpaceAgency.ESA,
                    data.spaceAgency,
                    "Probe $probeId should be assigned to ESA"
                )
            }
        }
    }
}
