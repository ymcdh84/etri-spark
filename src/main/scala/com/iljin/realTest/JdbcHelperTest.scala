package com.iljin.realTest

import java.sql.{Connection, DriverManager}
import java.util.Properties

class JdbcHelperTest extends java.io.Serializable{

  var connn: Connection = _
  val url = "jdbc:mysql://197.200.11.176:3306/rnd?useUnicode=true&characterEncoding=utf8"

  val username = "rndadmin"
  val password = "rnd1234!!"
  def openConnection: Connection = {
    if (null == connn || connn.isClosed) {
      val p = new Properties
      connn = DriverManager.getConnection(url, username, password)
    }
    connn
  }
}
