# Space Agency Kafka Spring Sample Project

This sample project illustrates  different ways to integrate Spring with Kafka.

## Fictitious use case

We are helping a european space agency to set up their telemetry data receivers. Therefore we need to keep track of the telemetry data we receive from the various space probes that are roaming the solar system (and beyond!). The problem is: All of the space probes send their telemetry data using imperial units. Our client systems use the metric system, though. As we learned from [the Mars Climate Orbiter fail](https://en.wikipedia.org/wiki/Mars_Climate_Orbiter), we need to take care of this before the probe goes down in a flaming fireball. We need to:

- Convert the incoming stream of (imperial) telemetry data points to the metric system
- Set up one consumer of the converted data as an example

## Technical setup

Our setup consists of 4 components:

- Sample producer that simulates (imperial) telemetry data that comes in from the space probes
- Converter component that transforms the data into a form our clients understand
- Sample consumer that ingests the (metric) telemetric data
- Kafka cluster for data transfer

