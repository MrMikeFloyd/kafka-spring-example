package com.example.kafkasamplesstreams.serdes

import com.example.kafkasamplesstreams.events.MeasurementDataPoint
import org.springframework.kafka.support.serializer.JsonSerde

class MeasurementDataPointSerde : JsonSerde<MeasurementDataPoint>()