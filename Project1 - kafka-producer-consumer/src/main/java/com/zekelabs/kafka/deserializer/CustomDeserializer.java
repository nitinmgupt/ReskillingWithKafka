package com.zekelabs.kafka.deserializer;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zekelabs.kafka.pojo.SalesData;

public class CustomDeserializer implements Deserializer<SalesData> {
	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
	}

	@Override
	public SalesData deserialize(String topic, byte[] data) {
		ObjectMapper mapper = new ObjectMapper();
		SalesData object = null;
		try {
			object = mapper.readValue(data, SalesData.class);
		} catch (Exception exception) {
			System.out.println("Error in deserializing bytes " + exception);
		}
		return object;
	}

	@Override
	public void close() {
	}
}