package hor.us.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import hor.us.constant.ConstantFile;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaFileReaderProducer extends Thread {

	private static  String topicName= ConstantFile.topicName;
   // = "svc-ads";
	//public static final String fileName = "/home/hadoop/test.txt";
	public static final String fileName = ConstantFile.fileName;

private final KafkaProducer<String, String> producer;

	
public KafkaFileReaderProducer(String topic) {
	//properties for producer
    Properties props = new Properties();
    //kafka bootstrap server
    props.put("bootstrap.servers", "localhost:9092");
    //producer acks
    props.put("acks", "1");
  //producer topic
    props.put("retries", "3");
    props.put("linger.ms","1");
    props.put("key.serializer", StringSerializer.class.getName());
    props.put("value.serializer", StringSerializer.class.getName());

    producer = new KafkaProducer<String, String>(props);
}

public void sendMessage(String key, String value) {
    
        try {
            producer.send(
                    new ProducerRecord<String, String>(topicName, key, value))
                    .get();
            System.out.println("Sent message: (" + key + ", " + value + ")");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

public static void main(String [] args){
	KafkaFileReaderProducer producer = new KafkaFileReaderProducer(topicName);
    int lineCount = 0;
    FileInputStream fis;
    BufferedReader br = null;
    try {
        fis = new FileInputStream(fileName);
        //Construct BufferedReader from InputStreamReader
        br = new BufferedReader(new InputStreamReader(fis));

        String line = null;
        while ((line = br.readLine()) != null) {
            lineCount++;
            producer.sendMessage(lineCount+"", line);
        }

    } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
    }finally{
        try {
            br.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}



}

