# Spring Boot & Kafka Streams Sample

This sample project illustrates different ways to integrate Kafka with Spring, processing a Streams workload. The following aspects are being considered:

- Producing messages to Kafka using `StreamBridge`
- Handling Streams of Kafka messages using Spring Cloud Streams with the Functions and Kafka Streams API
- Consuming Streams of Kafka Messages using Consumer Functions
- Maintaining application state using Kafka State Store
- Illustrating error handling for serialization and non-deserialization errors

To simulate a data processing pipeline, 3 separate services produce, process, and consume messages. The services' interactions can be observed using AKHQ and the application logs (see below for instructions on how to run it).

## Fictitious use case :satellite:

We are helping a space agency to set up their telemetry data receivers. Therefore, we need to keep track of the telemetry data we receive from the various space probes that are roaming the solar system (and beyond!). The space agency's requirements are:

- We need frequent updates on the aggregated telemetry data per probe, i.e. the total distance traveled by a given probe and the maximum speed it has reached on its journey so far.
- We need to be able to process aggregated telemetry data regardless of whether the inbound data was received from a NASA or an ESA probe:
  - NASA probes send their data in the imperial system (i.e. speed in miles/hour, distances in feet)
  - ESA probes send their data in the metric system (i.e. speed in kilometres/hour, distances in metres)

The received data thus needs to be converted into a common format (in our case, we opted for the metric system) to make sure that our mission experts can interpret the data correctly, regardless of how a given probe sent it. As we learned from [the Mars Climate Orbiter fail](https://en.wikipedia.org/wiki/Mars_Climate_Orbiter), this is pretty important to keep our probes from ending up in a flaming fireball.

## Technical setup

Our setup consists of 5 components:

- Sample producer that simulates imperial and metric telemetry data that comes in from a number of different space probes
- Aggregator component that brings the data into the desired aggregated format
- Sample consumer that ingests the (aggregated) imperial telemetry data and converts it into metric units if necessary
- Kafka cluster for data transfer
- AKHQ web UI for observing the Kafka cluster

## How to run

In order to run this sample, you need Docker Desktop.
You can run it by executing the following commands:

- Start up the Kafka Cluster and AKHQ: `docker compose --project-directory ./docker up`
    - Once started, you can explore the Kafka Topics and messages with AKHQ: `http://localhost:9080`
- Start up the respective services:
  - Sample producer: `gradle -p ./kafka-samples-producer bootRun`
  - Sample consumer: `gradle -p ./kafka-samples-consumer bootRun`
  - Streams processor: `gradle -p ./kafka-samples-streams bootRun`

The sample producer will emit a sample telemetry record for one of 10 different arbitrary probes every second (waiting for 5s after Spring context startup), so you should see the first aggregated telemetry records come in after a few seconds.

## Further information

If you want to have a look at the Streams Topology used in this sample, you find a visualization in the `doc` subdirectory. Generate your own [via Spring Boot Actuator](http://localhost:8080/actuator/kafkastreamstopology/kafka-telemetry-data-aggregator) and the excellent [Kafka Streams Topology Visualizer](https://zz85.github.io/kafka-streams-viz/).

### My docker containers are throwing errors on startup - what's wrong?

Unless you're running this example for the first time, your docker containers might be struggling with a previous inconsistent state. Giving yourself a fresh start by running `docker-compose down --remove-orphans -v` inside the `docker` directory should do the trick.

Have fun!
