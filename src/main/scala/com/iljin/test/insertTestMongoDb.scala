package com.iljin.test

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.SparkSession
import org.bson.Document

object insertTestMongoDb {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      //.master("local[*]")
      .master("spark://10.0.7.175:7077")
      .appName("insertTestMongoDb")
      .config("spark.cores.max", "3")
      .config("spark.num.executors", "3")
      .config("spark.executors.cores", "1")
      .config("spark.executor.memory", "1GB")
      .config("spark.mongodb.input.uri", "mongodb://rndadmin2:rnd1234!!@197.200.11.176:27019/sharding.spark2")
      .config("spark.mongodb.output.uri", "mongodb://rndadmin2:rnd1234!!@197.200.11.176:27019/sharding.spark2")
      //.config("spark.driver.host", "localhost")
      .getOrCreate

    val sc = spark.sparkContext

    val documents = sc.parallelize((1 to 10).map(i => Document.parse(s"{test: $i}")))

    MongoSpark.save(documents) // Uses the SparkConf for configuration

  }
}
