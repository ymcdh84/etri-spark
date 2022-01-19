package com.iljin.speed

import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.sql.Row
import org.mongodb.scala.bson.collection.mutable.Document

import java.sql.{Connection, PreparedStatement, Timestamp}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class MachineExcelSaveConfig(p_site_id: String,
                             p_uri: String,
                             p_dbName: String,
                             p_collectionName: String,
                             p_lotNum: String,
                             p_asset_list: ArrayBuffer[Row],
                             p_tag_list: ArrayBuffer[Row]){

  //자산 Obj 존재 확인 함수
  def checkCollection(p_cltNm: String): Boolean = collectionName.equals(p_cltNm)
  def checkLotNum(p_cltNm: String): Boolean = lotNum.equals(p_cltNm)

  //[START] MongoDb 저장 변수 및 함수
  val siteId:String = p_site_id
  val mongodbURI:String = p_uri
  val dbName:String = p_dbName
  val collectionName:String = p_collectionName
  val lotNum:String = p_lotNum

  val writeConfig: WriteConfig = WriteConfig(Map("uri" -> (mongodbURI + "/" + dbName + "." + collectionName)))
  var mongoConnector: MongoConnector = MongoConnector(writeConfig.asOptions)
  var rows: mutable.ArrayBuffer[Document] = new mutable.ArrayBuffer[Document]()

  //실시간 데이터 생성 Python Param
  var r_time_rows: mutable.ArrayBuffer[AnyRef] = new mutable.ArrayBuffer[AnyRef]()
  //실시간 데이터 생성 Python Param

  //Document 추가
  def appendDocument(doc: Document, json: AnyRef): Boolean = {
    rows.append(doc)
    r_time_rows.append(json)
    true
  }
  //[END] MongoDb 저장 변수 및 함수

  //[START] MariaDb 저장 변수 및 함수
  val jdbcHelper: JdbcHelper = new JdbcHelper()

  var conn: Connection = _

  var statement: PreparedStatement = _
  var sql: String = _

  var statementAlarm: PreparedStatement = _
  var sqlAlarm: String = _

  var modelingYn: Boolean = false

  var s_asset_list: ArrayBuffer[Row] = p_asset_list
  var s_tag_list: ArrayBuffer[Row] = p_tag_list

  var filter_tag_list: ArrayBuffer[Row] = _

  def initMariaDb() = {

    //모델링 여부 확인
    val assetRes = s_asset_list.filter(c =>
      c.get(0).equals(siteId) && c.get(1).equals(collectionName) && c.get(2).equals("Y")
    )

    if(assetRes.size > 0) modelingYn = true

    if (modelingYn && conn == null) {
      conn = jdbcHelper.openConnection
      conn.setAutoCommit(false)

      sql = "insert into " + collectionName + "("

      //태그목록 필터링
      filter_tag_list = s_tag_list.filter(c =>
        c.get(1).equals(collectionName)
      )

      filter_tag_list.foreach( c => {
        sql += c.get(4) + ","
      })

      sql = sql.slice(0, sql.size - 1)

      sql += ") values ("

      filter_tag_list.foreach( c => {
        sql += "?,"
      })

      sql = sql.slice(0, sql.size-1)

      sql += ")"

      statement = conn.prepareStatement(sql)

      //알람데이터 Sql
      sqlAlarm = "insert into TB_BIGDATA_ALARM_HISTORY (ALARM_ID, SITE_ID ,ASSET_ID ,TAG_ID , ALARM_DATE_TIME ,ALARM_VAL , ALARM_LEVEL_CD, ALARM_DESC, CONFIRM_YN"
      sqlAlarm += ") values (?, ?, ?, ?, now(), ?, ?, ?, ?)"
      statementAlarm = conn.prepareStatement(sqlAlarm)
    }
    true
  }

  def addStateMent (doc: Document) = {

    if (modelingYn) {

      filter_tag_list.zipWithIndex.foreach{ case(x,i) =>

        var alarmChkVal:Double = 0
        var alarmLevelCd:String = ""

        val mappingKey = x(5).toString
        val inputDataType = doc.get(mappingKey).get.getBsonType.toString

        if(inputDataType.equals("DOUBLE")){

          alarmChkVal = doc.get(x.get(5).toString).get.asDouble().getValue

          statement.setString(i + 1, alarmChkVal.toString)

          if(!Option(x.get(6)).toString.equals("None")) {
            //알람 정보 Min 값 체크
            if(alarmChkVal < x.get(6).toString.toDouble){
              alarmLevelCd = "0"
            }
          }
          if(!Option(x.get(7)).toString.equals("None")) {
            //알람 정보 Max 값 체크
            if(alarmChkVal > x.get(7).toString.toDouble){
              alarmLevelCd = "0"
            }
          }
          if(!Option(x.get(8)).toString.equals("None")) {
            //알람 경고 Min 값 체크
            if(alarmChkVal < x.get(8).toString.toDouble){
              alarmLevelCd = "1"
            }
          }
          if(!Option(x.get(9)).toString.equals("None")) {
            //알람 경고 Max 값 체크
            if(alarmChkVal > x.get(9).toString.toDouble){
              alarmLevelCd = "1"
            }
          }
          if(!Option(x.get(10)).toString.equals("None")) {
            //알람 심각 Max 값 체크
            if(alarmChkVal < x.get(10).toString.toDouble){
              alarmLevelCd = "2"
            }
          }
          if(!Option(x.get(11)).toString.equals("None")){
            //알람 심각 Min 값 체크
            if(alarmChkVal > x.get(11).toString.toDouble){
              alarmLevelCd = "2"
            }
          }

          if(!"".equals(alarmLevelCd)){

            val year = "20" + doc.get("0").get.asDouble().getValue.toInt.toString

            var month = doc.get("1").get.asDouble().getValue.toInt.toString
            if(month.length == 1) month = "0" + month

            var day = doc.get("2").get.asDouble().getValue.toInt.toString
            if(day.length == 1) day = "0" + day

            var hour = doc.get("3").get.asDouble().getValue.toInt.toString
            if(hour.length == 1) hour = "0" + hour

            var min = doc.get("4").get.asDouble().getValue.toInt.toString
            if(min.length == 1) min = "0" + min

            var tmpMappingKey = x.get(5).toString
            if(tmpMappingKey.length == 1) tmpMappingKey = "0" + tmpMappingKey

            statementAlarm.setString(1, lotNum + "_" + year + month + day + hour + min + tmpMappingKey)//alarm_id
            statementAlarm.setString(2, x.get(0).toString) //site_id
            statementAlarm.setString(3, x.get(1).toString)//asset_id
            statementAlarm.setString(4, x.get(2).toString) //tag_id
            statementAlarm.setString(5, alarmChkVal.toString) //alarm_val
            statementAlarm.setString(6, alarmLevelCd) //alarm_level_cd
            //statementAlarm.setString(6, alarmChkVal.toString) //alarm_desc
            statementAlarm.setString(7, "알람 발생 값 = " + alarmChkVal.toString) //alarm_desc
            statementAlarm.setString(8, "N") //confirm_yn

            statementAlarm.addBatch()
            statementAlarm.clearParameters()
          }

        }else if(inputDataType.equals("STRING")){

          val tagDataType = x(12).toString

          val strVal = doc.get(mappingKey).get.asString().getValue

          if(tagDataType.contains("DOUBLE") && strVal.equals(""))
            statement.setString(i + 1, "0.0")
          else
            statement.setString(i + 1, strVal)

        }else if(inputDataType.equals("DATE_TIME")){
          statement.setTimestamp(i + 1, new Timestamp(doc.get(mappingKey).get.asDateTime().getValue))
        }

        //태그 알람 정보 체크
      }

      statement.addBatch()
      statement.clearParameters()
    }
    true
  }

  //모델링 자산 테이블 데이터 INSERT
  def excuteBatch()= {
    if (modelingYn) {
      statement.executeBatch()
      statement.clearBatch()

      statementAlarm.executeBatch()
      statementAlarm.clearBatch()

      conn.commit()
    }
    true
  }

  def disConnectMariaDb()= {
    if (null != conn) conn.close()
    if (null != statement) statement.close()
    true
  }
  //[END] MariaDb 저장 변수 및 함수
}
