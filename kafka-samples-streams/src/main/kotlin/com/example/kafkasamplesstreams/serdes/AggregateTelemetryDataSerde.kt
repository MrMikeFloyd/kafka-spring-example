package com.example.kafkasamplesstreams.serdes

import com.example.kafkasamplesstreams.events.AggregatedTelemetryData
import org.springframework.kafka.support.serializer.JsonSerde

class AggregateTelemetryDataSerde : JsonSerde<AggregatedTelemetryData>()