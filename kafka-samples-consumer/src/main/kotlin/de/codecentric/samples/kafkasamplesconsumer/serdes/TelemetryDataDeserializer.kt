@file:Suppress("unused")

package de.codecentric.samples.kafkasamplesconsumer.serdes

import de.codecentric.samples.kafkasamplesconsumer.event.ImperialTelemetryData
import org.springframework.kafka.support.serializer.JsonDeserializer

class TelemetryDataDeserializer : JsonDeserializer<ImperialTelemetryData>()