package com.zekelabs.kafka;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zekelabs.kafka.constants.IKafkaConstants;
import com.zekelabs.kafka.consumer.ConsumerCreator;
import com.zekelabs.kafka.pojo.SalesData;
import com.zekelabs.kafka.producer.ProducerCreator;
import com.zekelabs.util.ReadExcelFile;

public class App {
	private static Map<String,Double> salesUK = new HashMap<String,Double>();
	private static Map<String,Double> salesFR = new HashMap<String,Double>();
	private static DecimalFormat df2 = new DecimalFormat("#.##");
	
	public static void main(String[] args) {
        runProducer();
		runConsumer("UK");
		printSales("UK");
		runConsumer("FR");
		printSales("FR");
	}

	static void runConsumer(String country) {
		ObjectMapper objectMapper = new ObjectMapper();
		Consumer<Long, SalesData> consumer = ConsumerCreator.createConsumer(country);

		int noMessageToFetch = 0;

		while (true) {
			final ConsumerRecords<Long, SalesData> consumerRecords = consumer.poll(1000);
			if (consumerRecords.count() == 0) {
				noMessageToFetch++;
				//System.out.println(noMessageToFetch);
				if (noMessageToFetch > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
					break;
				else
					continue;
			}

			/*
			 * consumerRecords.forEach(record -> {
			 * System.out.println("Record received from Topic " + record.topic() +
			 * " with key " + record.key() + " to partition " + record.partition() +
			 * " with offset " + record.offset()); try { System.out.println("Record value "
			 * + objectMapper.writeValueAsString(record.value())); } catch
			 * (JsonProcessingException e) { // TODO Auto-generated catch block
			 * e.printStackTrace(); } });
			 */
			
			consumerRecords.forEach(record -> {
				SalesData data = record.value();
				switch(data.getCountry()) {
				case "United Kingdom":
					salesUK.put(data.getStockCode(), data.getQuantity()*data.getUnitPrice());
					break;
				case "France":
					salesFR.put(data.getStockCode(), data.getQuantity()*data.getUnitPrice());
					break;
				}
				
			});
			consumer.commitAsync();
		}
		consumer.close();
	}

	
	static void runProducer() {
		Producer<Long, SalesData> producer = ProducerCreator.createProducer();

		List<SalesData> salesList = ReadExcelFile.readFile("/home/edyoda/Downloads/Kafka-java-projects/Project1/Sales.xlsx");
		SalesData data = salesList.get(0);
		int index = 0;
		for(SalesData sales: salesList) {
			index++;
			//System.out.println("Country:" + sales.getCountry() + ",StockCode:" + sales.getStockCode() + ",Quantity:" + sales.getQuantity() + ",UnitePrice:" + sales.getUnitPrice());
			final ProducerRecord<Long, SalesData> record = new ProducerRecord<Long, SalesData>(("United Kingdom".equalsIgnoreCase(sales.getCountry())? IKafkaConstants.TOPIC_SALES_UK:("France".equalsIgnoreCase(sales.getCountry())?IKafkaConstants.TOPIC_SALES_FR:IKafkaConstants.TOPIC_NAME)) ,sales);
			try {
				RecordMetadata metadata = producer.send(record).get();
				//producer.send(record, new DemoCallback());
				/*
				 * System.out.println("Record ("+ index + ") sent to Topic " +metadata.topic() +
				 * " with key " + index + " to partition " + metadata.partition() +
				 * " with offset " + metadata.offset());
				 */
			} catch (ExecutionException e) {
				System.out.println("Error in sending record");
				System.out.println(e);
			} catch (InterruptedException e) {
				System.out.println("Error in sending record");
				System.out.println(e);
			}
		}
		System.out.println("Total messages sent to Kafka Broker:" + index);
	}
	
	static void printSales(String country) {
		System.out.println("");
		System.out.println("Sale Report for Country: "+country);
		System.out.println("=================================");
		System.out.println("StockCode\t\tSale");
		System.out.println("=================================");
		if("UK".equalsIgnoreCase(country)) {
			salesUK.entrySet().forEach(entry-> {
				System.out.println(entry.getKey() +"\t\t\t" + df2.format(entry.getValue()));
			});
		} else if("FR".equalsIgnoreCase(country)) {
			salesFR.entrySet().forEach(entry-> {
				System.out.println(entry.getKey() +"\t\t\t" + df2.format(entry.getValue()));
			});
		}
	}
}
