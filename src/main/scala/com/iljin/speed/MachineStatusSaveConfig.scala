package com.iljin.speed

import org.apache.spark.sql.Row
import org.mongodb.scala.bson.collection.mutable.Document
import java.sql.{Connection, PreparedStatement, Timestamp}
import java.time.LocalDateTime
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class MachineStatusSaveConfig(p_site_id: String,
                              p_machine_id: String,
                              p_asset_list: ArrayBuffer[Row]){

  //자산 Obj 존재 확인 함수
  def checkMachineId(p_cltNm: String): Boolean = machineId.equals(p_cltNm)

  //[START] MongoDb 저장 변수 및 함수
    val siteId:String = p_site_id
    val machineId:String = p_machine_id

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

    //자산 상태 데이터 저장
    def saveAssetStat()= {

      //모델링 여부 확인
      val assetRes = s_asset_list.filter(c =>
        c.get(0).equals(siteId) && c.get(1).equals(machineId) && c.get(2).equals("Y")
      )

      if(assetRes.size > 0) modelingYn = true

      if (modelingYn) {

        conn = jdbcHelper.openConnection
        conn.setAutoCommit(false)

        //UPDATE
        var updSql = "update tb_bigdata_asset set asset_stat_cd = ?, seq_proc_stat_cd = ?, grow_proc_stat_cd = ? , stat_chg_dtm = ? where site_id = ? and asset_id = ?"
        statement = conn.prepareStatement(updSql)

        statement.setString(1, rows.last.get("status").get.asString().getValue)
        statement.setString(2, rows.last.get("seqStatus").get.asString().getValue)
        statement.setString(3, rows.last.get("growStatus").get.asString().getValue)
        statement.setTimestamp(4, Timestamp.valueOf(LocalDateTime.now()))
        //statement.setTimestamp(4 Timestamp.valueOf(rows.last.get("datetime").get.asString().getValue))
        statement.setString(5, siteId)
        statement.setString(6, machineId)

        statement.execute()
        conn.commit()
        statement.clearParameters()
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
