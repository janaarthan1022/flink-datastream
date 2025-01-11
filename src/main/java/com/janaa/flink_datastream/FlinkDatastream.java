package com.janaa.flink_datastream;

import java.io.FileInputStream;
import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.janaa.flink_datastream.constants.FlinkDatastreamConstants;
import com.janaa.flink_datastream.functions.FilterEvenFunction;
import com.janaa.flink_datastream.functions.FilterOddFunction;
import com.janaa.flink_datastream.functions.MapAgeFunction;
import com.janaa.flink_datastream.utils.AgeCalculator;

public class FlinkDatastream {

	private static final Logger logger = LoggerFactory.getLogger(FlinkDatastream.class);

	public static void main(String[] args) throws Exception {
		
		if(args.length != 1) {
			System.out.println("Usage:\nflink run jarFile.jar propertiesFileLocation");
			System.exit(0);
		}

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final AgeCalculator ageCalculator = new AgeCalculator();
		// Kafka consumer properties
		Properties kafkaProps = new Properties();
		kafkaProps.load(new FileInputStream(args[0]));
		kafkaProps.setProperty("bootstrap.servers", "localhost:9092");
		kafkaProps.setProperty("group.id", "flink-consumer-group");
		FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
				kafkaProps.getProperty(FlinkDatastreamConstants.KAFKA_TOPIC_SOURCE), new SimpleStringSchema(),
				kafkaProps);
		//kafkaConsumer.setStartFromEarliest();
		DataStream<String> kafkaStream = env.addSource(kafkaConsumer);

		FlinkKafkaProducer<String> evenProducer = new FlinkKafkaProducer<String>(
				kafkaProps.getProperty(FlinkDatastreamConstants.KAFKA_TOPIC_EVEN), new SimpleStringSchema(),
				kafkaProps);
		FlinkKafkaProducer<String> oddProducer = new FlinkKafkaProducer<String>(
				kafkaProps.getProperty(FlinkDatastreamConstants.KAFKA_TOPIC_ODD), new SimpleStringSchema(), kafkaProps);

		kafkaStream.map(new MapAgeFunction(ageCalculator)).filter(new FilterEvenFunction()).addSink(evenProducer);

		kafkaStream.map(new MapAgeFunction(ageCalculator)).filter(new FilterOddFunction()).addSink(oddProducer);
		
		
		
		FlinkKafkaConsumer<String> evenConsumer = new FlinkKafkaConsumer<>(
				kafkaProps.getProperty(FlinkDatastreamConstants.KAFKA_TOPIC_EVEN), new SimpleStringSchema(),
				kafkaProps);
		//kafkaConsumer.setStartFromEarliest();
		DataStream<String> evenKafkaStream = env.addSource(evenConsumer);
		evenKafkaStream.writeAsText(kafkaProps.getProperty(FlinkDatastreamConstants.OUTPUT_FILE_EVEN));
		
		FlinkKafkaConsumer<String> oddConsumer = new FlinkKafkaConsumer<>(
				kafkaProps.getProperty(FlinkDatastreamConstants.KAFKA_TOPIC_ODD), new SimpleStringSchema(),
				kafkaProps);
		//kafkaConsumer.setStartFromEarliest();
		DataStream<String> oddKafkaStream = env.addSource(oddConsumer);
		evenKafkaStream.writeAsText(kafkaProps.getProperty(FlinkDatastreamConstants.OUTPUT_FILE_ODD));
		
		
		
		env.execute(FlinkDatastreamConstants.FLINK_JOB_NAME);

	}

}
