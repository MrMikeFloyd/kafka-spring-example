package com.example.kafkasamplesstreams

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class KafkaSamplesStreamsApplication

fun main(args: Array<String>) {
    runApplication<KafkaSamplesStreamsApplication>(*args)
}
