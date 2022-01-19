package com.iljin.speed

import org.apache.spark.sql.SparkSession

object MachineStatusLayer {

  var site_id:String = "SITE_00001"
  var kafka_topic:String = "dispMachineStatus"
  var startOffset:String = "latest"

  //Param1: SITE_ID, Param2: TOPIC_ID, Param3: OFFSET
  def main(args: Array[String]): Unit = {

    site_id = args(0)
    kafka_topic = args(1)
    startOffset = args(2)

    val spark = SparkSession
      .builder
//      .master("local[*]")
      .master("spark://10.0.7.175:7077")
      .appName("MachineStatusLayer")
      .config("spark.cores.max", "3")
      .config("spark.num.executors", "3")
      .config("spark.executors.cores", "1")
      .config("spark.executor.memory", "1GB")
      //.config("spark.driver.host", "localhost")
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

    val machineStatusWriter: MachineStatusWriter = new MachineStatusWriter(site_id)

    var convertDataSet = dataFrame
      .writeStream
      .outputMode("append")
      .foreach(machineStatusWriter)
      .start()
      .awaitTermination()
  }
}
