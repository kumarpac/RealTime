package hor.us.producer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class Test3 {
	public static void main(String args[]) {
		String msg;
		// properties for producer
		Properties props = new Properties();
		// kafka bootstrap server
		props.put("bootstrap.servers", "localhost:9092");
		// producer acks
		props.put("acks", "1");
		// producer topic
		props.put("retries", "3");
		props.put("linger.ms", "1");
		props.put("key.serializer", StringSerializer.class.getName());
		props.put("value.serializer", StringSerializer.class.getName());

		// create producer
		Producer<String, String> producer = new KafkaProducer<String, String>(
				props);

		try {
			Class.forName("com.mysql.jdbc.Driver");
			Connection con = DriverManager
					.getConnection(
							"jdbc:mysql://localhost:3306/ads?verifyServerCertificate=false&useSSL=true",
							"root", "Usa@12345");
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery("select * from Persons");
			while (rs.next()) {
				int PersonID = rs.getInt("PersonID");
				String LastName = rs.getString("LastName");
				String FirstName = rs.getString("FirstName");
				String Address = rs.getString("Address");
				String City = rs.getString("City");
				System.out.println("PersonID" + PersonID + "LastName"
						+ LastName + "FirstName" + FirstName + "Address"
						+ Address + "City" + City);
				msg = PersonID + "," + LastName + "" + FirstName + ","
						+ Address + "," + City;
				ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(
						"horizon_mysql_reader_topic", msg);

				producer.send(producerRecord);

			}
		} catch (Exception e) {
			System.out.println(e);
		}

		producer.close();
		// con.close();
	}
}

