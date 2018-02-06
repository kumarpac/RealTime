package main.scala
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.storage.StorageLevel

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer

import Utilities._

import org.apache.spark.streaming.kafka._
import kafka.serializer.StringDecoder

object ReadFile {
  def main(args: Array[String]) {
    //val conf = new SparkConf().setAppName("CSVLoad").setMaster("local[*]").setJars(Array("/home/pawan/Documents/Hadoop/Sofwarespark/sparkexternallib/spark-csv_2.10-1.2.0.jar"))
    //val sc = new SparkContext(conf)
    // val data =sc.textFile("/home/hadoop/test.txt")
    // data.collect().foreach(println)
    // /user/horizon/data
    //data.saveAsTextFile("hdfs://oc1175581158.ibm.com:9000/user/horizon1")

    // Create the context with a 1 second batch size
    val ssc = new StreamingContext("local[*]", "LogAnalysis", Seconds(15))

    // Construct a regular expression (regex) to extract fields from raw Apache log lines
    val pattern = apacheLogPattern()

    // hostname:port for Kafka brokers, not Zookeeper
    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")
    // List of topics you want to listen for from Kafka
    val topics = List("svc-horizon").toSet
    // Create our Kafka stream, which will contain (topic,message) pairs. We tack a 
    // map(_._2) at the end in order to only get the messages, which contain individual
    // lines of data.
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)

    lines.print();
    lines.foreachRDD(record => {
      record.saveAsTextFile("hdfs://oc1175581158.ibm.com:9000/user/horizon1")
    })

    ssc.checkpoint("/home/hadoop/checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }
}
