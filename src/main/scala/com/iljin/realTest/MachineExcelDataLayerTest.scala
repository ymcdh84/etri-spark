package com.iljin.realTest

import org.apache.spark.sql.SparkSession

object MachineExcelDataLayerTest {

  val site_id:String = "SITE_00001"
  val kafka_topic:String = "dispMachine"

  //Param1: SITE_ID, Param2: CONNECT_ID, Param3: TOPIC_ID
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .master("local[*]")
      .config("spark.driver.host", "localhost")
//      .master("spark://10.0.7.25:7077")
//      .appName("MachineExcelDataLayer")
//      .config("spark.cores.max", "3")
//      .config("spark.num.executors", "3")
//      .config("spark.executors.cores", "1")
//      .config("spark.executor.memory", "1GB")
      .getOrCreate

    val kafkaStreamDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "197.200.11.176:9093,197.200.11.177:9094,197.200.90.116:9095")
      //.option("subscribe", args(0))
      .option("subscribe", kafka_topic)
      .option("startingOffsets", "earliest")
      .load()

    val dataFrame = kafkaStreamDF
      .selectExpr("CAST(value AS STRING)")
      .toDF

    val machineExcelWriter: MachineExcelWriterTest = new MachineExcelWriterTest(site_id, kafka_topic)

    var convertDataSet = dataFrame
      .writeStream
      .outputMode("append")
      .foreach(machineExcelWriter)
      .start()
      .awaitTermination()
  }
}
