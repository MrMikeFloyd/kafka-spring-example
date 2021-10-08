package de.codecentric.samples.kafkasamplesconsumer

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import mu.KotlinLogging
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.function.Consumer

@Configuration
class KafkaConsumerConfiguration {

    private val logger = KotlinLogging.logger {}

    @Bean
    fun processTelemetryData(): Consumer<String> =
        Consumer { telemetryRecord ->
            try {
                val telemetryDataInMetric = convertToMetricSystem(telemetryRecord)
                logger.info { "Received Probe Telemetry data with maxSpeed ${telemetryDataInMetric.maxSpeedKph} KpH" +
                        "and distance travelled ${telemetryDataInMetric.totalDistanceMetres} Meter" }
            } catch (e: Exception) {
                logger.error { "Error processing telemetry data: '$telemetryRecord'" }
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
