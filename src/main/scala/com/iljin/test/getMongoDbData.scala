package com.iljin.test

import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.sql.toSparkSessionFunctions
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}

import java.util.Properties

object getMongoDbData {

  def main(args: Array[String]): Unit = {

    //[STEP - 1] 스파크 세션 가져오기
    val spark = SparkSession
      .builder
      //.master("local[*]")
      .master("spark://10.0.7.175:7077")
      .appName("getMongoDbData")
      .config("spark.driver.host", "localhost")
      .getOrCreate

    //[STEP - 2] MongoDB 테이블 데이터 출력
    val mongoURI = "mongodb://rndadmin2:rnd1234!!@197.200.11.176:27019"
    val Conf = makeMongoURI(mongoURI,"sharding","Machine007")

    // Uses the ReadConfig
    val df3 = spark.sqlContext.loadFromMongoDB(ReadConfig(Map("uri" -> Conf)))

    df3.show(df3.count().toInt,false)
  }

  def makeMongoURI(uri:String,database:String,collection:String) = (s"${uri}/${database}.${collection}")
}
