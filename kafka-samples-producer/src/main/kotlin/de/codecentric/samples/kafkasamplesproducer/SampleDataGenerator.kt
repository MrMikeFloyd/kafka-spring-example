package de.codecentric.samples.kafkasamplesproducer

import de.codecentric.samples.kafkasamplesproducer.event.SpaceAgency
import de.codecentric.samples.kafkasamplesproducer.event.TelemetryData
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import kotlin.random.Random

@Component
class SampleDataGenerator(@Autowired val telemetryDataStreamBridge: TelemetryDataStreamBridge) {

    // Emit 1 telemetry data point every 1s, wait 5s for the application to settle
    @Scheduled(initialDelay = 5000L, fixedRate = 1000L)
    fun emitSampleTelemetryData() {
        val nextInt = Random.nextInt(10)
        val telemetryData = TelemetryData(
            probeId = nextInt.toString(),
            currentSpeedMph = Random.nextDouble(0.0, 1000.0),
            traveledDistanceFeet = Random.nextDouble(1.0, 10000.0),
            spaceAgency = when {
                nextInt < 5 -> {
                    SpaceAgency.NASA
                }
                else -> {
                    SpaceAgency.ESA
                }
            }
        )
        telemetryDataStreamBridge.send(telemetryData)
    }
}