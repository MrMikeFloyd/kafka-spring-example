package com.example.kafkasamplesstreams.serdes

import com.example.kafkasamplesstreams.events.TelemetryDataPointWithDisabled
import org.springframework.kafka.support.serializer.JsonSerde

class TelemetryDataWithDisabledSerde : JsonSerde<TelemetryDataPointWithDisabled>()