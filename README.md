# Space Agency Kafka Spring Sample Project

This sample project illustrates different ways to integrate Kafka with Spring. The following aspects are being considered:

- Producing messages to Kafka using `StreamBridge`
- Handling Streams of Kafka messages using Spring Cloud Streams with the Functions and Kafka Streams API
- Consuming Streams of Kafka Messages using Consumer Functions
- Maintaining application state using Kafka State Store
- Illustrating error handling for serialization and non-deserialization errors

To simulate a data processing pipeline, 3 separate services produce, process, and consume messages. The services' interactions can be observed using AKHQ and the application logs (see below for instructions on how to run it).

## Fictitious use case

We are helping a european space agency to set up their telemetry data receivers. Therefore, we need to keep track of the telemetry data we receive from the various space probes that are roaming the solar system (and beyond!). The space agency's requirements are:

- Receive the aggregated telemetry data per probe, i.e. the total distance traveled by a given probe and the maximum speed it has reached on its journey so far.
- Receive the telemetry data in metric units.

All the space probes send their telemetry data using imperial units. As we learned from [the Mars Climate Orbiter fail](https://en.wikipedia.org/wiki/Mars_Climate_Orbiter), we need to take care of this before the probe goes down in a flaming fireball.

## Technical setup

Our setup consists of 5 components:

- Sample producer that simulates (imperial) telemetry data that comes in from the space probes
- Aggregator component that brings the data into the desired aggregated format
- Sample consumer that ingests the (aggregated) imperial telemetry data and converts it into metric units
- Kafka cluster for data transfer
- AKHQ web UI for observing the Kafka cluster

## How to run

To run this sample, you need Docker Desktop.
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

Have fun!
