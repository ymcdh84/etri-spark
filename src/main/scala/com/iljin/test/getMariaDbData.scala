package com.iljin.test

import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}

import java.util.Properties

object getMariaDbData {

  def main(args: Array[String]): Unit = {

    //[STEP - 1] 스파크 세션 가져오기
    val spark = SparkSession
      .builder
      .master("local[*]")
      //.master("spark://10.0.7.80:7077")
      .appName("getMariaDbData")
      .config("spark.driver.host", "localhost")
      .getOrCreate

    //[STEP - 2] MariaDB 테이블 데이터 출력
    //jdbc:mysql로 연결해야 제대로된 데이터를 받아옴
    //val url = "jdbc:mariadb://197.200.11.176:3306/rnd?user=rndadmin&password=rnd1234!!"
    val url = "jdbc:mysql://197.200.11.176:3306/rnd?user=rndadmin&password=rnd1234!!"

    val df = spark
      .read
      .format("jdbc")
      .option("url", url)
      .option("dbtable", "tb_tag_info")
      .load()

    //df.printSchema()
    df.show(50, false)

    df.createOrReplaceTempView("tb_tag_info_temp3")

    spark.sql("select * from tb_tag_info_temp3 where cycle > 8000").show()


    spark.sql("insert into tb_tag_info_temp3" +
          " values (\"VTAG_NM00_14\", \"NM00 TEMP_14\", \"Double\", \"스파크에서 INSERT\", 28000, \"14\", \"TEMP_14\", 9, 1400, \"Sparktest\")")


    spark.sql("select * from tb_tag_info_temp3 where cycle > 8000").show()

    val df2 = spark
      .read
      .format("jdbc")
      .option("url", url)
      .option("dbtable", "testmachine007")
      .load()

    df2.createOrReplaceTempView("testmachine007")

    spark.sql("insert into testmachine007" +
      " values (\"2021\", \"09\", \"25\", null, null, null, null, null, null, null, null, null, null, null, null)")

    //[STEP - 3] MariaDB 테이블에 데이터 INSERT
//    val sc = spark.sparkContext
//
//    val sqlContext = new SQLContext(sc)
//
//    import sqlContext.implicits._
//
//    val tempDF = List(("VTAG_NM00_11", "NM00 TEMP_11", "Double", "스파크에서 INSERT", "18000", "8", "TEMP_8", "8", "8000", "Spark")).toDF("TAG_ID"
//                                                                , "TAG_NM"
//                                                                , "DATA_TYPE"
//                                                                , "TAG_DESC"
//                                                                , "CYCLE"
//                                                                , "IMPORTANCE"
//                                                                , "ALIAS"
//                                                                , "MIN_VALUE"
//                                                                , "MAX_VALUE"
//                                                                , "FORMAT"
//                                                              )
//
//    val properties = new Properties()
//    properties.put("user", "rndadmin")
//    properties.put("password", "rnd1234!!")
//
//    tempDF.write.mode(SaveMode.Append).jdbc("jdbc:mysql://197.200.11.176:3306/rnd", "tb_tag_info", properties)
    //tempDF.write.mode(SaveMode.Overwrite).option("truncate",true).jdbc("jdbc:mysql://197.200.11.176:4008/rnd", "tb_tag_info", properties)
  }
}
