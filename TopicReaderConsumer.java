package hor.us.consumer;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;


public class TopicReaderConsumer {
	public static void main(String agrs[]){
		
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("key.deserializer", StringDeserializer.class.getName());
		properties.setProperty("value.deserializer", StringDeserializer.class.getName());
		properties.setProperty("group.id", "test");
		properties.setProperty("enabled.auto.commit", "true");
		properties.setProperty("auto.commit.interval.ms", "1000");
		properties.setProperty("enabled.auto.commit", "true");
		
		KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<String,String>(properties);
		kafkaConsumer.subscribe(Arrays.asList("svc-horizon"));
		
		while(true){
			ConsumerRecords<String,String> consumerRecords = kafkaConsumer.poll(100);
			for(ConsumerRecord<String, String> consumerRecord:consumerRecords){
				System.out.println("key :" + consumerRecord.key());
				System.out.println("value :" +consumerRecord.value());
				System.out.println("timestamp :" +consumerRecord.timestamp());
				System.out.println("partition :" +consumerRecord.partition());
				System.out.println("topic :" +consumerRecord.topic());
				System.out.println("offset :" +consumerRecord.offset());
			}
			kafkaConsumer.commitSync();
		}
		
	}

}

