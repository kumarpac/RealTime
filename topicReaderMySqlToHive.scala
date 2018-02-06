import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._

import org.apache.spark.sql.types._
import org.apache.kafka.common.serialization.StringDeserializer

import org.apache.spark.sql.types.{ IntegerType, StringType, StructField, StructType }
import org.apache.spark.sql.functions._

object test {
  def main(args: Array[String]) {
    println("hi  test =======================>")

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("spark session example")
      .getOrCreate()

    val mySchema = StructType(Array(
      StructField("PersonID", IntegerType),
      StructField("LastName", StringType),
      StructField("FirstName", StringType),
      StructField("Address", StringType),
      StructField("City", StringType)))

    val kafkaSource = sparkSession.
      readStream.
      format("kafka"). // <-- use KafkaSource
      option("subscribe", "horizon_mysql_reader_topic").
      option("kafka.bootstrap.servers", "localhost:9092").
      option("startingoffsets", "earliest").
      load()

    kafkaSource.selectExpr("CAST(value AS STRING)")
    //option("kafka.key.deserializer","classOf[StringDeserializer]").

    kafkaSource.printSchema()
    //kafkaSource.write.parquet("hdfs://oc1175581158.ibm.com:9000/user/horizon2")

    val sink = kafkaSource.writeStream.
      format("parquet").
      option("path", "hdfs://oc1175581158.ibm.com:9000/user/horizon3").
      option("checkpointLocation", "/home/hadoop").
      start()

    val query = kafkaSource.writeStream.
      format("console").
      option("truncate", value = false).
      outputMode("append").
      queryName("logs").
      start()

    query.awaitTermination()

  }
}
