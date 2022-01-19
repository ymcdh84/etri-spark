package com.iljin.speed

import org.apache.spark.sql.{ForeachWriter, Row}
import org.mongodb.scala.bson.collection.mutable.Document

import java.sql.{Connection, PreparedStatement}

import scala.collection.mutable

class MachineStatusWriter(p_site_id: String
                         ) extends ForeachWriter[Row]{

  val site_id:String = p_site_id

  var w_asset_list : mutable.ArrayBuffer[Row] = new mutable.ArrayBuffer[Row]()

  //asset save Obj 배열
  var saveAssetList: mutable.ArrayBuffer[MachineStatusSaveConfig] = new mutable.ArrayBuffer[MachineStatusSaveConfig]()

  //asset save Obj에 존재하는 자산인지 체크하는 변수
  var matching:Boolean = false

  var conn: Connection = _
  var statement: PreparedStatement = _

  val jdbcHelper: JdbcHelper = new JdbcHelper()

  //MongoDB 커넥션 연결 및 데이터 수신 변수 선언
  override def open(partitionId: Long, version: Long): Boolean = {
    conn = jdbcHelper.openConnection

    //자산 데이터 조회 [START]
    var sqlAsset = "select * from tb_bigdata_asset where  site_id = ?"

    statement = conn.prepareStatement(sqlAsset)
    statement.setString(1,site_id)

    var rs = statement.executeQuery()

    while ( {
      rs.next
    }) {
      var row = Row(rs.getString("site_id"), rs.getString("asset_id"), rs.getString("model_confirm_yn"))
      w_asset_list.append(row)
    }
    //자산 데이터 조회 [END]

    true
  }

  //row별 스트림 데이터 json 형태 변경
  override def process(record: Row): Unit = {

    // asset save Obj 추가 [START]
    val valueStr = record.getAs[String]("value")
    val doc: Document  = Document(valueStr)

    var machineId: String = doc.get("machineid").get.asString().getValue
    machineId = machineId.slice(4, machineId.size)

    saveAssetList.foreach(c => {
      if(c.checkMachineId(machineId)){
        c.appendDocument(doc)
        matching = true
      }
    })

    if(!matching){
      // asset save Obj 생성
      val saveAssetObj: MachineStatusSaveConfig =  new MachineStatusSaveConfig(site_id, machineId, w_asset_list);

      saveAssetObj.appendDocument(doc)

      saveAssetList.append(saveAssetObj)

    }

    //매칭 확인 변수 초기화
    matching = false;

    // asset save Obj 추가 [END]

  }

  //asset save Obj 저장 및 연결 해제
  override def close(errorOrNull: Throwable): Unit = {
    if(saveAssetList.size > 0){

      // Save 로직 처리 [START]
      saveAssetList.foreach(c => {

        //자산 상태 테이블 데이터 업데이트
        c.saveAssetStat()

        c.disConnectMariaDb()
      })
      // Save 로직 처리 [END]

      //저장 객체 리스트 초기화
      saveAssetList = new mutable.ArrayBuffer[MachineStatusSaveConfig]();
    }

    if (null != conn) conn.close()
    if (null != statement) statement.close()
  }
}
