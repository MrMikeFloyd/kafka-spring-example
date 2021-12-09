package com.example.kafkasamplesstreams.serdes

import com.example.kafkasamplesstreams.events.AggregatedFullProbeMeasurementData
import org.springframework.kafka.support.serializer.JsonSerde

class AggregatedFullProbeDataSerde : JsonSerde<AggregatedFullProbeMeasurementData>()