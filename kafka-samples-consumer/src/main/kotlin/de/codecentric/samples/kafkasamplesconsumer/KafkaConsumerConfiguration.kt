package de.codecentric.samples.kafkasamplesconsumer

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import mu.KotlinLogging
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.messaging.Message
import java.util.function.Consumer

@Configuration
class KafkaConsumerConfiguration {

    private val logger = KotlinLogging.logger {}

    @Bean
    fun processTelemetryData(): Consumer<Message<String>> =
        Consumer { telemetryMessage ->
            try {
                val telemetryDataInMetric = convertToMetricSystem(telemetryMessage.payload)
                logger.info {
                    "\nReceived telemetry data for probe '${telemetryMessage.headers["kafka_receivedMessageKey"]}':" +
                            "\n\tMax Speed: ${telemetryDataInMetric.maxSpeedKph} kph" +
                            "\n\tTotal distance travelled: ${telemetryDataInMetric.totalDistanceMetres} meters"
                }
            } catch (e: Exception) {
                logger.error {
                    "Error processing telemetry data for probe '${telemetryMessage.headers["kafka_receivedMessageKey"]}': " +
                            "'${telemetryMessage.payload}'"
                }
            }
        }

    private fun convertToMetricSystem(telemetryRecord: String): AggregatedMetricData {
        val typeRef: TypeReference<HashMap<String, String>> = object : TypeReference<HashMap<String, String>>() {}
        val telemetryData: MutableMap<String, String> = ObjectMapper().readValue(telemetryRecord, typeRef)
        val aggregatedSpeedMph: Double = telemetryData["maxSpeed"]!!.toDouble()
        val aggregatedDistanceFeet: Double = telemetryData["sumDistance"]!!.toDouble()

        return AggregatedMetricData(aggregatedSpeedMph * 1.61, aggregatedDistanceFeet * 0.3048)
    }

    inner class AggregatedMetricData(
        val maxSpeedKph: Double,
        val totalDistanceMetres: Double
    )

}
