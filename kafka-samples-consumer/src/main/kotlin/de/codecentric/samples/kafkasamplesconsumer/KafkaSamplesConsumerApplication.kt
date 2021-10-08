package de.codecentric.samples.kafkasamplesconsumer

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class KafkaSamplesConsumerApplication

fun main(args: Array<String>) {
	runApplication<KafkaSamplesConsumerApplication>(*args)
}
