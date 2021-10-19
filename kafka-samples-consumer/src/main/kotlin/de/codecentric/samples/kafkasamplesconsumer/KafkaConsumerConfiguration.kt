package de.codecentric.samples.kafkasamplesconsumer

import mu.KotlinLogging
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.messaging.Message
import java.util.function.Consumer

@Configuration
class KafkaConsumerConfiguration {

    private val logger = KotlinLogging.logger {}

    @Bean
    fun processTelemetryData(): Consumer<Message<ImperialTelemetryData>> =
        Consumer { telemetryMessage ->
            try {
                val metricTelemetryData = MetricTelemetryData(telemetryMessage.payload)
                logger.info {
                    "\nReceived telemetry data for probe '${telemetryMessage.headers["kafka_receivedMessageKey"]}':" +
                            "\n\tMax Speed: ${metricTelemetryData.maxSpeedKph} kph" +
                            "\n\tTotal distance travelled: ${metricTelemetryData.totalDistanceMetres} meters"
                }
            } catch (e: Exception) {
                logger.error {
                    "Error processing telemetry data for probe '${telemetryMessage.headers["kafka_receivedMessageKey"]}': " +
                            "'${telemetryMessage.payload}': '${e.message}'"
                }
            }
        }

}
