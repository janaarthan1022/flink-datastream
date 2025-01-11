# Use Case

#### Create a Beam or Flink application
#### Consume from 1 Kafka topic and publish onto 2 kafka topics
#### Inbound Kafka with a payload that contains Name, Address and Date of Birth.
#### If age is EVEN number, republish to the EVEN_TOPIC
#### If age is ODD number, republish to the ODD_TOPIC
#### Persist all published message onto a datastore of your choice (i.e. file, database, in-mem db)
