# kstreams-des-demo
Demo project for Derivative Event Sourcing talks

Relies on the [kafka-streams-fluent-test](https://github.com/jbfletch/kafka-streams-fluent-test) library.

Requirements: 
Maven, Java 8+

**Overview:**
This is a self contained Kafka Streams project that illustrates a few different ways to use derivative events.  
It includes the following unit tests

**DesStreamJsonTest -** Simple example of applying an event profile for notification 

**DesStreamMultiTableJsonTest -** Complex event aggregate example that shows how to implement topology to assure event completeness

**SessionWindowProcessorApiTest -** Simple example of emitting the latest value for a given session key using wall clock time and the processor api

**To Run:**
Pull the project down, specify jdk 8+, and run `mvn clean test` this will run the unit tests. 
