package de.codecentric.samples.kafkasamplesproducer

import de.codecentric.samples.kafkasamplesproducer.event.MeasurementData
import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.cloud.stream.function.StreamBridge
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.support.MessageBuilder
import org.springframework.stereotype.Component

@Component
class MeasurementDataStreamBridge(@Autowired val streamBridge: StreamBridge) {

    private val logger = KotlinLogging.logger {}

    fun send(measurementData: MeasurementData) {
        val kafkaMessage = MessageBuilder
            .withPayload(measurementData)
            // Make sure all messages for a given probe go to the same partition to ensure proper ordering
            .setHeader(KafkaHeaders.MESSAGE_KEY, measurementData.probeId)
            .build()
        logger.info { "Publishing space probe measurement data: Payload: '${kafkaMessage.payload}'" }
        streamBridge.send("measurement-data-out-0", kafkaMessage)
    }
}