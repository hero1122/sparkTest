package com.jcloud.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.ProcessingTime

/**
  * Created by liwei26 on 17-2-16.
  */
object SparkStructuredStreaming {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Spark Structured Streaming Example")
      .master("local[4]")
      .getOrCreate()

    import spark.implicits._

    val kafka = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "BDS-TEST-001:9092")
      .option("subscribe", "structuredStreaming-test-topic")
      .option("startingOffsets", "latest")
      .load()

    /*val df = kafka.select(explode(split($"value".cast("string"), "\\s+")).as("word"))
      .groupBy($"word")
      .count*/

    val values = kafka.selectExpr("CAST(value AS STRING)").as[String]

    values.writeStream
      .trigger(ProcessingTime("5 seconds"))
      .outputMode("append")
      .format("console")
      .start()
      .awaitTermination()
  }
}
