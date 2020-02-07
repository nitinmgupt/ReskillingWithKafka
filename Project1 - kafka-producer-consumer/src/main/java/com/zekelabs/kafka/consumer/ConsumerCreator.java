package com.zekelabs.kafka.consumer;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;

import com.zekelabs.kafka.constants.IKafkaConstants;
import com.zekelabs.kafka.deserializer.CustomDeserializer;
import com.zekelabs.kafka.pojo.SalesData;

//import kafka_avro_confluent.deserializers.AvroDeserializer;

public class ConsumerCreator {

	public static Consumer<Long, SalesData> createConsumer(String country) {
		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BROKERS);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, IKafkaConstants.GROUP_ID_CONFIG);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
		//props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CustomDeserializer.class.getName());
		//props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AvroDeserializer.class.getName());
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, IKafkaConstants.MAX_POLL_RECORDS);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, IKafkaConstants.OFFSET_RESET_EARLIER);

		final Consumer<Long, SalesData> consumer = new KafkaConsumer<Long, SalesData>(props);
		consumer.subscribe(Collections.singletonList("UK".contentEquals(country)?IKafkaConstants.TOPIC_SALES_UK:"FR".contentEquals(country)?IKafkaConstants.TOPIC_SALES_FR:IKafkaConstants.TOPIC_NAME));
		return consumer;
	}


}
