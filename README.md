# Use Case

#### Create a Beam or Flink application
#### Consume from 1 Kafka topic and publish onto 2 kafka topics
#### Inbound Kafka with a payload that contains Name, Address and Date of Birth.
#### If age is EVEN number, republish to the EVEN_TOPIC
#### If age is ODD number, republish to the ODD_TOPIC
#### Persist all published message onto a datastore of your choice (i.e. file, database, in-mem db)

### Command to run flink job

bin/flink run /Users/janaarthanm/Documents/shift2024/flink/flink-datastream/target/flink-datastream-0.0.1-SNAPSHOT-jar-with-dependencies.jar  
/Users/janaarthanm/Documents/shift2024/flink/flink-datastream/src/main/resources/flink-datastream.properties


### Kafka Source Topic console producer

bin/kafka-console-producer --bootstrap-server localhost:9092 --topic input-topic

>{"Name":"aaa","Address":"Bangalore","Date of Birth":"1990-10-10"}

>{"Name":"aaa","Address":"Bangalore","Date of Birth":"1989-10-10"}



### Even Age topic console consumer

[appuser@5c5f08ffcaae usr]$ bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic even-topic

{"Address":"Bangalore","Date of Birth":"1990-10-10","Age":34,"Name":"aaa"}


### Odd Age topic console consumer
[appuser@5c5f08ffcaae usr]$ bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic odd-topic
{"Address":"Bangalore","Date of Birth":"1989-10-10","Age":35,"Name":"aaa"}

### Persisting even-topic to even.txt file

janaarthanm@Janaarthans-Air flink-1.19.1 % cat even.txt 

{"Address":"Bangalore","Date of Birth":"1990-10-10","Age":34,"Name":"aaa"}


### Persisting odd-topic to odd.txt file

janaarthanm@Janaarthans-Air flink-1.19.1 % cat odd.txt 

{"Address":"Bangalore","Date of Birth":"1989-10-10","Age":35,"Name":"aaa"}
