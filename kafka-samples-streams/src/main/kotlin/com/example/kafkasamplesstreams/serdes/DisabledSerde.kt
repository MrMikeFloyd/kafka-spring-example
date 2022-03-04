@file:Suppress("unused")

package com.example.kafkasamplesstreams.serdes

import com.example.kafkasamplesstreams.events.Disabled
import com.example.kafkasamplesstreams.events.TelemetryDataPoint
import org.springframework.kafka.support.serializer.JsonSerde

class DisabledSerde : JsonSerde<Disabled>()