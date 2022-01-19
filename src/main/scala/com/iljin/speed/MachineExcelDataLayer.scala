package com.iljin.speed

import org.apache.spark.sql.SparkSession

object MachineExcelDataLayer {

  var site_id:String = "SITE_00001"
  var kafka_topic:String = "dispMachine"
  var startOffset:String = "earliest"

  //Param1: SITE_ID, Param2: TOPIC_ID, Param3: OFFSET
  def main(args: Array[String]): Unit = {

    site_id = args(0)
    kafka_topic = args(1)
    startOffset = args(2)

    val spark = SparkSession
      .builder
//      .master("local[*]")
//      .config("spark.driver.host", "localhost")
      .master("spark://10.0.7.175:7077")
      .appName("MachineExcelDataLayer")
      .config("spark.cores.max", "3")
      .config("spark.num.executors", "3")
      .config("spark.executors.cores", "1")
      .config("spark.executor.memory", "1GB")
      .getOrCreate

    val kafkaStreamDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "197.200.11.176:9093,197.200.11.177:9094,197.200.90.116:9095")
      //.option("subscribe", args(0))
      .option("subscribe", kafka_topic)
      .option("startingOffsets", startOffset)
      .load()

    val dataFrame = kafkaStreamDF
      .selectExpr("CAST(value AS STRING)")
      .toDF

    val machineExcelWriter: MachineExcelWriter = new MachineExcelWriter(site_id, kafka_topic)

    var convertDataSet = dataFrame
      .writeStream
      .outputMode("append")
      .foreach(machineExcelWriter)
      .start()
      .awaitTermination()
  }
}
