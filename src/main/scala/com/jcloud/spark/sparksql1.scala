package com.jcloud.spark
import scala.reflect.runtime.universe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
/**
  * Created by liwei26 on 17-1-4.
  */
object sparksql1 extends App{
  val spark = SparkSession.builder().appName("Spark-2.0-SQL-APP-1").master("local").config("spark.some.config.option", "some-value")
    .enableHiveSupport()
    .getOrCreate();

  import spark.implicits._
  import spark.sql

  case class Energy(Name: String, Postcode: String, Date: String, Unit: String, MeterReading: Int, Output: Int, CapacityKW: Double)
  val columns = "Name,Postcode,Date,Unit,Meter Reading,Output,Installed Capacity KW"
  val df = spark.sparkContext.textFile("input/energy_generation_wc_140114.csv")
    .filter { !_.contains(columns) }
    .map { _.split(",") }
    .map { p => Energy(p(0), p(1), p(2), p(3), p(4).trim().toInt, p(5).trim().toInt, p(6).trim().toDouble) }
    .toDF()
  df.show(10)
  df.printSchema()
  val jsonData = df.toJSON
  jsonData.take(5).foreach { println }

  df.select("Name", "Date", "Output").show()
  df.filter($"Output" > 30 and $"Output" < 70).show()
  df.filter($"Output" < 30 or $"Output" > 70).show()
  df.groupBy("Name").count().show()
  df.groupBy("Date").count().sort("Date").show()
  df.groupBy("Date").sum("Output").sort("Date").show()
  df.createOrReplaceTempView("energy")
  sql("SELECT * FROM energy LIMIT 10").show()
  sql("SELECT COUNT(*) AS CNT FROM energy").show()
  sql("SELECT Date, SUM(Output) AS Output_Sum FROM energy GROUP BY Date ORDER BY Date").show()

  spark.udf.register("doubleOutput", { output: String => output.toInt * 2 })
  sql("SELECT Output, doubleOutput(Output) AS Double_Output FROM energy LIMIT 10").show()
  df.write.mode(SaveMode.Overwrite).saveAsTable("hive_energy")
  val hdf = spark.table("hive_energy")
  hdf.show(10)
}
