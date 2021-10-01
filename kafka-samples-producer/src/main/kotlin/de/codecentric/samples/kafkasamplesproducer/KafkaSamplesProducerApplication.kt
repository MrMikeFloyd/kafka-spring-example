package de.codecentric.samples.kafkasamplesproducer

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class KafkaSamplesProducerApplication

fun main(args: Array<String>) {
	runApplication<KafkaSamplesProducerApplication>(*args)
}
