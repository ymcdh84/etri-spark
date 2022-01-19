package com.iljin.speed

import com.mongodb.client.MongoCollection
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.apache.avro.data.Json
import org.mongodb.scala.bson.collection.mutable.Document

import java.sql.{Connection, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.{Date, Locale}
import scala.collection.JavaConverters._
import scala.collection.mutable

class MachineExcelWriter(p_site_id: String,
                         p_kafka_topic: String
                        ) extends ForeachWriter[Row]{

  val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.KOREA)
  val mongodb_uri:String = "mongodb://rndadmin2:rnd1234!!@197.200.11.176:27019"
  val mdb_name:String = "sharding"

  val site_id:String = p_site_id
  val kafka_topic:String = p_kafka_topic

  var w_asset_list : mutable.ArrayBuffer[Row] = new mutable.ArrayBuffer[Row]()
  var w_tag_list : mutable.ArrayBuffer[Row] = new mutable.ArrayBuffer[Row]()

  //var w_asset_list : List[Row] = p_asset_list
  //var w_tag_list : List[Row] = p_tag_list

  //asset save Obj 배열
  var saveAssetList: mutable.ArrayBuffer[MachineExcelSaveConfig] = new mutable.ArrayBuffer[MachineExcelSaveConfig]()

  //asset save Obj에 존재하는 자산인지 체크하는 변수
  var matching:Boolean = false

  var conn: Connection = _
  var statement: PreparedStatement = _
  var statement2: PreparedStatement = _

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

    //자산 태그 데이터 조회 [START]
    var sqlAssetStat = "select site_id, asset_id, tag_id, tag_nm, alias, mapping_key" +
      ", alarm_info_min_val, alarm_info_max_val, alarm_worm_min_val, alarm_worm_max_val, alarm_emer_min_val, alarm_emer_max_val" +
      ", FN_CMN_GET_CODE_NM(\"DATA_TYPE_CD\", data_type_cd) as data_type_cd from tb_bigdata_asset_tag where  site_id = ?"

    statement2 = conn.prepareStatement(sqlAssetStat)
    statement2.setString(1,site_id)

    var rs2 = statement2.executeQuery()

    while ( {
      rs2.next
    }) {
      var row = Row(rs2.getString("site_id")
        , rs2.getString("asset_id")
        , rs2.getString("tag_id")
        , rs2.getString("tag_nm")
        , rs2.getString("alias")
        , rs2.getString("mapping_key")
        , rs2.getString("alarm_info_min_val")
        , rs2.getString("alarm_info_max_val")
        , rs2.getString("alarm_worm_min_val")
        , rs2.getString("alarm_worm_max_val")
        , rs2.getString("alarm_emer_min_val")
        , rs2.getString("alarm_emer_max_val")
        , rs2.getString("data_type_cd")
      )
      w_tag_list.append(row)
    }
    //자산 태그 데이터 조회 [END]

    if (null != conn) conn.close()
    if (null != statement) statement.close()
    if (null != statement2) statement2.close()
    true
  }

  //row별 스트림 데이터 json 형태 변경
  override def process(record: Row): Unit = {

    // asset save Obj 추가 [START]
    var valueStr = record.getAs[String]("value")
    valueStr = valueStr.replace("  \", \"6\":", "\", \"6\":")

    val doc: Document  = Document(valueStr)
    val jsonVal = Json.parseJson(valueStr)

    //Lot번호별 -> 성장로 번호
    val lotNum: String = doc.get("5").get.asString().getValue

    //현재시간, 공정상태 필드 추가
    doc.put("crtDtm", new Date())
    doc.put("operStat", "1")

    //컬렉션명 생성
    val colNm = "Machine" + lotNum.slice(5,8);

    saveAssetList.foreach(c => {
      if(c.checkLotNum(lotNum)){
        c.appendDocument(doc, jsonVal)
        c.addStateMent(doc)
        matching = true
      }
    })

    if(!matching){
      // asset save Obj 생성
      val saveAssetObj: MachineExcelSaveConfig =  new MachineExcelSaveConfig(site_id, mongodb_uri, mdb_name, colNm, lotNum, w_asset_list, w_tag_list);

      saveAssetObj.appendDocument(doc, jsonVal)

      saveAssetObj.initMariaDb()
      saveAssetObj.addStateMent(doc)

      saveAssetList.append(saveAssetObj)
    }

    //매칭 확인 변수 초기화
    matching = false;

    // asset save Obj 추가 [END]

  }

  //asset save Obj 저장 및 연결 해제
  override def close(errorOrNull: Throwable): Unit = {
    if(saveAssetList.size > 0){

      val spark2 = SparkSession
        .builder
//        .master("local[*]")
        .master("spark://10.0.7.175:7077")
        .appName("PythonCall")
        .config("spark.cores.max", "2")
        .config("spark.num.executors", "2")
        .config("spark.executors.cores", "1")
        .config("spark.executor.memory", "1GB")
//        .config("spark.driver.host", "localhost")
//        .config("spark.driver.allowMultipleContexts", "true")
        .getOrCreate

      val context = spark2.sparkContext

      //ETRI제공 실시간 차트 데이터 생성 파이썬 호출[START]


      // Save 로직 처리 [START]
      saveAssetList.foreach(c => {

        //자산 모델링 테이블 데이터 Insert_MariaDb(RDB)
        c.excuteBatch()

        c.disConnectMariaDb()

        //자산 데이터 Insert_MongoDb(NoSql)
        c.mongoConnector.withCollectionDo(c.writeConfig, {collection: MongoCollection[Document] =>
          collection.insertMany(c.rows.map(wc => wc).asJava)
        })
        c.mongoConnector.close()

        try {

          val ipData = context.makeRDD(c.r_time_rows, 1)

          //val opData = ipData.pipe("python C:\\workspace\\etri_python\\Realtime_chart.py")
          val opData = ipData.pipe("python3 /usr/python/Realtime_chart.py")

          opData.collect().foreach(println)

          context.clearJobGroup()

        } catch {

          case ex: Exception => println(ex)

        }
        //ETRI제공 실시간 차트 데이터 생성 파이썬 호출[END]
      })

      spark2.close()
      // Save 로직 처리 [END]

      //저장 객체 리스트 초기화
      saveAssetList = new mutable.ArrayBuffer[MachineExcelSaveConfig]();
    }
  }
}
