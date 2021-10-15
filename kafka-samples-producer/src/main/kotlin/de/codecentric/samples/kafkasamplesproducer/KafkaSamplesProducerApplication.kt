package de.codecentric.samples.kafkasamplesproducer

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling

@SpringBootApplication
@EnableScheduling
class KafkaSamplesProducerApplication

fun main(args: Array<String>) {
    runApplication<KafkaSamplesProducerApplication>(*args)
}
