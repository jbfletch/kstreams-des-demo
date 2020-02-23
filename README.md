# kstreams-des-demo
Demo project for Derivative Events and other Kafka Streams fun

Relies on the [kafka-streams-fluent-test](https://github.com/jbfletch/kafka-streams-fluent-test) library.

Requirements: 
Maven, Java 8+

**Overview:**
This is a self contained Kafka Streams project that illustrates a few different ways to use derivative events.  
It includes the following unit tests

**DesStreamJsonTest -** Simple example of applying an event profile for notification 

**DesStreamMultiTableJsonTest -** Complex event aggregate example that shows how to implement topology to assure event completeness

**SessionWindowProcessorApiTest -** Simple example of emitting the latest value for a given session key using wall clock time and the processor api

**WallClockAlertProcessorApiTest -** Demo illustrating a way to use wall clock time to detect when an external integration receives and event and never returns a message back within a given time span. 

**WallClockWindowProcessorApiTest -** Shows how to use a TimestampKeyValue store to emit the last received value for a given key after x time interval. The interval is tracked per key and begins when that key is first inserted into the store

**SampleLatestNMessagesTest-**  This demonstrates keeping and emitting the last n values (in our example n == 2) for a given key

**To Run:**
Pull the project down, specify jdk 8+, and run `mvn clean test` this will run the unit tests. 
