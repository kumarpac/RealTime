
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer

import org.apache.spark.SparkContext
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.storage.StorageLevel

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext

import kafka.serializer.StringDecoder

object hdfs {
  def main(args: Array[String]) {
    val ssc = new StreamingContext("local[*]", "LogAnalysis", Seconds(15))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))

    val topics = Array("horizon_mysql_reader_topic")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))
    stream.print()

    stream.foreachRDD(record => {
      record.saveAsTextFile("hdfs://oc1175581158.ibm.com:9000/user/ads_store")
    })

    val key = stream.map(record => (record.key))
    val value = stream.map(record => (record.value))
    (key).print()
    (value).print()

    value.foreachRDD(record => {
      record.saveAsTextFile("hdfs://oc1175581158.ibm.com:9000/user/ads_lake")
    })

    ssc.checkpoint("/home/hadoop/checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }
}
