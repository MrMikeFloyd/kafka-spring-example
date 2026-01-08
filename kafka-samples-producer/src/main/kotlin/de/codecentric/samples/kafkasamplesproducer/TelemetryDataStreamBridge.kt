package de.codecentric.samples.kafkasamplesproducer

import de.codecentric.samples.kafkasamplesproducer.event.TelemetryData
import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.cloud.stream.function.StreamBridge
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.support.MessageBuilder
import org.springframework.stereotype.Component

@Component
class TelemetryDataStreamBridge(@Autowired val streamBridge: StreamBridge) {

    private val logger = KotlinLogging.logger {}

    fun send(telemetryData: TelemetryData) {
        val kafkaMessage = MessageBuilder
            .withPayload(telemetryData)
            // Make sure all messages for a given probe go to the same partition to ensure proper ordering
            .setHeader(KafkaHeaders.KEY, telemetryData.probeId)
            .build()
        logger.info { "Publishing space probe telemetry data: Payload: '${kafkaMessage.payload}'" }
        streamBridge.send("telemetry-data-out-0", kafkaMessage)
    }
}