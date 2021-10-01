package de.codecentric.samples.kafkasamplesproducer

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.util.*
import kotlin.random.Random

@Component
class SampleDataGenerator(@Autowired val telemetryDataStreamBridge: TelemetryDataStreamBridge) {

    // Emit 1 telemetry data point ever 1s, wait for 5s for the application to settle
    @Scheduled(initialDelay = 5000L, fixedRate = 1000L)
    fun emitSampleTelementryData() {
        val telemetryData = TelemetryData(
            probeId = Random.nextInt(10).toString(),
            currentSpeedMph = 41234.1,
            traveledDistanceFeet = 2232342342.3
        )
        telemetryDataStreamBridge.send(telemetryData)
    }
}