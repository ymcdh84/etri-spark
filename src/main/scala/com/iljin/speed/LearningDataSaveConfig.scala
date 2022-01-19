package com.iljin.speed

import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.sql.Row
import org.mongodb.scala.bson.collection.mutable.Document

import java.sql.{Connection, PreparedStatement, Timestamp}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class LearningDataSaveConfig(p_site_id: String,
                             p_uri: String,
                             p_dbName: String,
                             p_collectionName: String,
                             p_asset_list: ArrayBuffer[Row],
                             p_tag_list: ArrayBuffer[Row]){

  //자산 Obj 존재 확인 함수
  def checkCollection(p_cltNm: String): Boolean = collectionName.equals(p_cltNm)

  //[START] MongoDb 저장 변수 및 함수
  val siteId:String = p_site_id
  val mongodbURI:String = p_uri
  val dbName:String = p_dbName
  val collectionName:String = p_collectionName

  val writeConfig: WriteConfig = WriteConfig(Map("uri" -> (mongodbURI + "/" + dbName + "." + collectionName)))
  var mongoConnector: MongoConnector = MongoConnector(writeConfig.asOptions)
  var rows: mutable.ArrayBuffer[Document] = new mutable.ArrayBuffer[Document]()

  //Document 추가
  def appendDocument(doc: Document): Boolean = {
    rows.append(doc)
    true
  }
  //[END] MongoDb 저장 변수 및 함수

  //[START] MariaDb 저장 변수 및 함수
  val jdbcHelper: JdbcHelper = new JdbcHelper()

  var conn: Connection = _

  var statement: PreparedStatement = _
  var sql: String = _

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
    }
    true
  }

  def addStateMent (doc: Document) = {

    if (modelingYn) {

      filter_tag_list.zipWithIndex.foreach{ case(x,i) =>

        val mappingKey = x(5).toString

        val inputDataType = doc.get(mappingKey).get.getBsonType.toString

        if(inputDataType.equals("DOUBLE")){
          statement.setString(i + 1, doc.get(mappingKey).get.asDouble().getValue.toString)
        }else if(inputDataType.equals("STRING")){

          val tagDataType = x(6).toString

          val strVal = doc.get(mappingKey).get.asString().getValue

          if(tagDataType.contains("DOUBLE") && strVal.equals(""))
            statement.setString(i + 1, "0.0")
          else
            statement.setString(i + 1, strVal)

        }else if(inputDataType.equals("DATE_TIME")){
          statement.setTimestamp(i + 1, new Timestamp(doc.get(mappingKey).get.asDateTime().getValue))
        }
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
