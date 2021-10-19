package de.codecentric.samples.kafkasamplesconsumer

import org.springframework.kafka.support.serializer.JsonDeserializer

class TelemetryDataDeserializer : JsonDeserializer<ImperialTelemetryData>()